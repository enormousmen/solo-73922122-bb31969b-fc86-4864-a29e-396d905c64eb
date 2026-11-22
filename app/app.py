#!/usr/bin/env python3
"""单卷恢复服务。

场景:虚拟机数据盘尚未恢复完,值班员需要立刻恢复业务读写。

- 本地块用 SQLite 持久化(<DATA_DIR>/volume.db);本地缺块时从只读备份镜像读透;
- 后台线程把缺块从备份回填到本地(INSERT OR IGNORE:业务写入永远优先,
  迟到的备份数据不会覆盖已提交的业务整块写入);
- 网页可按块挂起/放行后台备份读取;挂起期间同一块的整块写入照常提交;
- 进程重启后保留已落盘的块,并从持久化的选块游标继续回填;
- 只要还有缺块(包括挂起未放行的缺块)就拒绝断开备份;补齐后允许断开。

选块设计(本版本重点):
- 已占用块(present ∪ suspended)在 SQLite 中用合并后的区间表 ``segments`` 表示,
  连续区间只存一行,不按块建任务、不把全卷块状态载入内存;
- 后台回填维护一个持久化游标 ``backfill_cursor``,选块时用主键索引做区间跳转,
  一次跳过整段本地块 / 挂起块,再取该段之后的第一个空洞;
- 放行一个已被跳过的缺块时把游标回拨到该块,后台会回头补齐;
- 写入 / 挂起 / 放行 / 回填落盘都在单个事务内同时更新块表、区间表和游标,
  任何一步失败都回滚,选块看到的仍是操作前的状态;
- 旧版 SQLite 卷没有 segments 表,首次启动顺序扫描 blocks/suspended 构建区间。
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time

from flask import Flask, jsonify, render_template, request

from backup_util import make_image as make_backup_image

log = logging.getLogger("recovery")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks(
  idx        INTEGER PRIMARY KEY,
  data       BLOB NOT NULL,
  source     TEXT NOT NULL,          -- backfill | business
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS suspended(
  idx          INTEGER PRIMARY KEY,
  suspended_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- 已占用块(present ∪ suspended)的合并区间 [start, end),end 不含。
-- 空卷时为零行;连续整段只占一行,选块按区间跳转而不是逐块检查。
CREATE TABLE IF NOT EXISTS segments(
  start INTEGER PRIMARY KEY,
  end   INTEGER NOT NULL
);
"""

SEGMENT_VERSION = "1"
DEFAULT_SEGMENT_SIZE = 256
MAX_SEGMENT_SIZE = 1024


class Volume:
    """本地块存储(SQLite)+ 只读备份镜像 + 后台回填 worker。"""

    def __init__(self, data_dir, backup_path, block_size, block_count, backfill_delay):
        self.data_dir = data_dir
        self.backup_path = backup_path
        self.block_size = block_size
        self.block_count = block_count
        self.backfill_delay = backfill_delay
        os.makedirs(data_dir, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(
            os.path.join(data_dir, "volume.db"), check_same_thread=False, timeout=5
        )
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        with self.lock:
            self.db.executescript(SCHEMA)
            self.db.commit()
            self._migrate_locked()
        self._backup_fd = None
        self._worker = None
        self._stop = threading.Event()
        self._closed = False
        self.generation = 0  # reset/stop 时 +1,旧 worker 发现世代变更即退出
        self.stats = {
            "backup_reads": 0,
            "backfilled": 0,
            "business_writes": 0,
            "late_ignored": 0,
            "segment_hops": 0,  # 选块时整段跳过的次数(不是逐块检查次数)
        }

    # ---------- 元数据 ----------
    def _meta_get_locked(self, key, default="0"):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def _meta_set_locked(self, key, value):
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def _cursor_locked(self):
        try:
            cursor = int(self._meta_get_locked("backfill_cursor", "0"))
        except (TypeError, ValueError):
            cursor = 0
        return min(max(cursor, 0), self.block_count)

    def _present_count_locked(self):
        """已落盘块数:随写入/回填在同一事务里维护,避免每轮 O(块数) 的 COUNT。"""
        raw = self._meta_get_locked("present_count", None)
        try:
            return int(raw)
        except (TypeError, ValueError):
            (present,) = self.db.execute("SELECT COUNT(*) FROM blocks").fetchone()
            return present

    def _bump_present_locked(self, delta):
        count = self._present_count_locked() + delta
        self._meta_set_locked("present_count", str(max(0, min(count, self.block_count))))

    # ---------- 旧卷升级:顺序扫描构建区间 ----------
    def _iter_occupied_locked(self):
        """按块号升序归并 blocks 与 suspended(流式,不载入内存)。"""
        block_cur = self.db.execute("SELECT idx FROM blocks ORDER BY idx")
        susp_cur = self.db.execute("SELECT idx FROM suspended ORDER BY idx")
        b = block_cur.fetchone()
        s = susp_cur.fetchone()
        last = None
        while b is not None or s is not None:
            if b is not None and (s is None or b[0] <= s[0]):
                idx = b[0]
                b = block_cur.fetchone()
            else:
                idx = s[0]
                s = susp_cur.fetchone()
            if idx != last:
                yield idx
                last = idx

    def _rebuild_segments_locked(self):
        self.db.execute("DELETE FROM segments")
        runs = []
        run_start = run_end = None
        for idx in self._iter_occupied_locked():
            if run_start is None:
                run_start = run_end = idx
            elif idx == run_end + 1:
                run_end = idx
            else:
                runs.append((run_start, run_end + 1))
                run_start = run_end = idx
            if len(runs) >= 1024:
                self.db.executemany("INSERT INTO segments(start,end) VALUES(?,?)", runs)
                runs.clear()
        if run_start is not None:
            runs.append((run_start, run_end + 1))
        if runs:
            self.db.executemany("INSERT INTO segments(start,end) VALUES(?,?)", runs)

    def _migrate_locked(self):
        """首次升级:以一次顺序扫描兼容旧 SQLite 卷;空卷不建任何逐块记录。"""
        version = self._meta_get_locked("segment_version", None)
        (seg_rows,) = self.db.execute("SELECT COUNT(*) FROM segments").fetchone()
        (block_rows,) = self.db.execute("SELECT COUNT(*) FROM blocks").fetchone()
        if version == SEGMENT_VERSION:
            if seg_rows == 0 and block_rows > 0:
                # 区间表与块表不一致(异常中断等):重建一次。
                self._rebuild_segments_locked()
                self._meta_set_locked("backfill_cursor", "0")
                log.warning("区间表缺失,已从块表重建 %d 块", block_rows)
            # 一次聚合查询校准计数(不逐块建记录、不载入全卷状态)。
            self._meta_set_locked("present_count", str(block_rows))
            self.db.commit()
            return
        self._rebuild_segments_locked()
        self._meta_set_locked("segment_version", SEGMENT_VERSION)
        self._meta_set_locked("present_count", str(block_rows))
        # 旧卷从第一个真正的空洞续填;只做区间跳转,不逐块扫描。
        self._meta_set_locked("backfill_cursor", "0")
        idx, _ = self._find_free_locked(0)
        self._meta_set_locked(
            "backfill_cursor", str(idx if idx is not None else self.block_count)
        )
        self.db.commit()
        (seg_rows,) = self.db.execute("SELECT COUNT(*) FROM segments").fetchone()
        log.info(
            "已升级选块区间: %d 行区间, 已落盘 %d 块, 起始游标 %s",
            seg_rows,
            block_rows,
            self._meta_get_locked("backfill_cursor", "0"),
        )

    # ---------- 区间维护(均需在调用方事务内) ----------
    def _segment_add_locked(self, idx):
        """把 idx 标记为已占用:与左右相邻区间合并。"""
        prev = self.db.execute(
            "SELECT start,end FROM segments WHERE start<=? ORDER BY start DESC LIMIT 1",
            (idx,),
        ).fetchone()
        nxt = self.db.execute(
            "SELECT start,end FROM segments WHERE start>? ORDER BY start LIMIT 1",
            (idx,),
        ).fetchone()
        in_prev = prev is not None and prev[1] > idx
        adj_prev = prev is not None and prev[1] == idx
        adj_next = nxt is not None and nxt[0] == idx + 1
        if in_prev and not adj_next:
            return
        if not in_prev and not adj_prev and not adj_next:
            self.db.execute("INSERT INTO segments(start,end) VALUES(?,?)", (idx, idx + 1))
            return
        start = prev[0] if (in_prev or adj_prev) else idx
        end = max(prev[1], idx + 1) if (in_prev or adj_prev) else idx + 1
        if in_prev or adj_prev:
            self.db.execute("DELETE FROM segments WHERE start=?", (prev[0],))
        if adj_next:
            end = max(end, nxt[1])
            self.db.execute("DELETE FROM segments WHERE start=?", (nxt[0],))
        self.db.execute("INSERT INTO segments(start,end) VALUES(?,?)", (start, end))

    def _segment_remove_locked(self, idx):
        """把 idx 从已占用集合移除(必要时拆分区间)。"""
        row = self.db.execute(
            "SELECT start,end FROM segments WHERE start<=? ORDER BY start DESC LIMIT 1",
            (idx,),
        ).fetchone()
        if row is None or row[1] <= idx:
            return
        start, end = row
        self.db.execute("DELETE FROM segments WHERE start=?", (start,))
        if start < idx:
            self.db.execute("INSERT INTO segments(start,end) VALUES(?,?)", (start, idx))
        if idx + 1 < end:
            self.db.execute("INSERT INTO segments(start,end) VALUES(?,?)", (idx + 1, end))

    # ---------- 选块:整段跳过占用区间 ----------
    def _find_free_locked(self, cursor):
        """从 cursor 起找第一个既未落盘、也未挂起的块。

        每次只查「包含/位于游标之前最近的区间」,落在区间内就整段跳到区间末尾;
        返回 (idx | None, 跳过的区间段数)。复杂度按区间数而非块数增长。
        """
        hops = 0
        if cursor < 0:
            cursor = 0
        while cursor < self.block_count:
            row = self.db.execute(
                "SELECT start,end FROM segments WHERE start<=? ORDER BY start DESC LIMIT 1",
                (cursor,),
            ).fetchone()
            if row is not None and row[1] > cursor:
                cursor = row[1]
                hops += 1
                continue
            return cursor, hops
        return None, hops

    def _select_locked(self):
        """返回 (idx | None, 跳段数, 选择时的游标值)。

        第三个值用于提交时检测:取数窗口内若有人放行导致游标回拨,
        worker 不能把游标又推回前面,否则被放行的缺块会被再次跳过。
        """
        cursor = self._cursor_locked()
        idx, hops = self._find_free_locked(cursor)
        if idx is None and cursor > 0:
            # 兜底:游标已到末尾但身后仍有空洞,从 0 再找一遍。
            idx2, hops2 = self._find_free_locked(0)
            if idx2 is not None:
                return idx2, hops + hops2, cursor
        return idx, hops, cursor

    def _next_free_locked(self):
        idx, hops, _ = self._select_locked()
        # 没有可派发的块(例如只剩挂起缺块):跳段统计不计入,避免空转刷高。
        return (idx, hops) if idx is not None else (None, 0)

    def segment_rows_locked(self):
        return self.db.execute("SELECT start,end FROM segments ORDER BY start").fetchall()

    # ---------- 备份镜像(只读) ----------
    def read_backup(self, idx):
        """从只读备份镜像读一个块(os.pread 线程安全)。"""
        with self.lock:
            if self._meta_get_locked("backup_disconnected") == "1":
                raise RuntimeError("backup disconnected")
            if self._backup_fd is None:
                self._backup_fd = os.open(self.backup_path, os.O_RDONLY)
            fd = self._backup_fd
            self.stats["backup_reads"] += 1
        data = os.pread(fd, self.block_size, idx * self.block_size)
        if len(data) != self.block_size:
            raise RuntimeError(f"short read from backup at block {idx}")
        return data

    # ---------- 挂起 / 放行 ----------
    def is_suspended_locked(self, idx):
        return self.db.execute("SELECT 1 FROM suspended WHERE idx=?", (idx,)).fetchone() is not None

    def suspend(self, idx):
        """挂起后台备份读取;块进入占用集合,回填选块整段跳过。"""
        with self.lock:
            try:
                self.db.execute(
                    "INSERT OR IGNORE INTO suspended(idx, suspended_at) VALUES(?,?)",
                    (idx, time.time()),
                )
                self._segment_add_locked(idx)
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise

    def resume(self, idx):
        """放行;若它是被跳过的缺块,把游标回拨到该块,后台会回头补齐。"""
        with self.lock:
            try:
                cur = self.db.execute("DELETE FROM suspended WHERE idx=?", (idx,))
                if cur.rowcount:
                    present = self.db.execute(
                        "SELECT 1 FROM blocks WHERE idx=?", (idx,)
                    ).fetchone()
                    if present is None:
                        self._segment_remove_locked(idx)
                        if idx < self._cursor_locked():
                            self._meta_set_locked("backfill_cursor", str(idx))
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise

    # ---------- 块状态 ----------
    def segment_state(self, start, count):
        """只读取当前段的块状态(页面按段加载,不载入全卷)。"""
        start = min(max(0, int(start)), self.block_count)
        count = max(1, min(int(count), MAX_SEGMENT_SIZE))
        end = min(self.block_count, start + count)
        with self.lock:
            present = {
                r[0]: r[1]
                for r in self.db.execute(
                    "SELECT idx, source FROM blocks WHERE idx>=? AND idx<? ORDER BY idx",
                    (start, end),
                )
            }
            suspended = [
                r[0]
                for r in self.db.execute(
                    "SELECT idx FROM suspended WHERE idx>=? AND idx<? ORDER BY idx",
                    (start, end),
                )
            ]
        return {"start": start, "end": end, "present": present, "suspended": suspended}

    def missing_count_locked(self):
        return max(0, self.block_count - self._present_count_locked())

    # ---------- 业务读写 ----------
    def read_block(self, idx):
        """返回 (status, data, origin)。status: local|backup|suspended|gone"""
        with self.lock:
            row = self.db.execute("SELECT data, source FROM blocks WHERE idx=?", (idx,)).fetchone()
            if row:
                return "local", row[0], row[1]
            if self._meta_get_locked("backup_disconnected") == "1":
                return "gone", None, None
            if self.is_suspended_locked(idx):
                return "suspended", None, None
        # 本地缺块:从备份读透,不落盘(由后台回填负责补齐)
        return "backup", self.read_backup(idx), None

    def write_block(self, idx, data):
        """整块写入(必须恰好 block_size 字节),业务写入永远生效。

        成功后该块立即进入占用区间,不会再被派给后台回填。
        """
        assert len(data) == self.block_size
        with self.lock:
            try:
                existed = self.db.execute(
                    "SELECT 1 FROM blocks WHERE idx=?", (idx,)
                ).fetchone() is not None
                self.db.execute(
                    "INSERT INTO blocks(idx,data,source,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(idx) DO UPDATE SET data=excluded.data, "
                    "source=excluded.source, updated_at=excluded.updated_at",
                    (idx, data, "business", time.time()),
                )
                self._segment_add_locked(idx)
                if not existed:
                    self._bump_present_locked(+1)
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            self.stats["business_writes"] += 1

    # ---------- 后台回填 ----------
    def _backfill_loop(self, generation):
        log.info("回填 worker 启动 (gen=%d)", generation)
        while not self._stop.is_set():
            with self.lock:
                if self._closed or generation != self.generation:
                    log.info("回填 worker 退出:世代已变更")
                    return
                if self._meta_get_locked("backfill_running") != "1":
                    log.info("回填 worker 退出:已停止")
                    return
                if self._meta_get_locked("backup_disconnected") == "1":
                    log.info("回填 worker 退出:备份已断开")
                    return
                if self.missing_count_locked() == 0:
                    self._meta_set_locked("backfill_running", "0")
                    self.db.commit()
                    log.info("回填完成:无缺块")
                    return
                idx, hops, used_cursor = self._select_locked()
                if hops:
                    self.stats["segment_hops"] += hops
            if idx is None:
                # 剩余缺块都在挂起集合里:保持 running,等页面放行后再回头补齐。
                time.sleep(0.2)
                continue
            try:
                data = self.read_backup(idx)
            except Exception:
                log.exception("备份读取失败,稍后重试 (block=%d)", idx)
                time.sleep(1.0)
                continue
            # 模拟备份端拷贝耗时,也让进度在页面上可见;
            # 这个窗口内若业务写入抢先落盘,下面的 INSERT OR IGNORE 会丢弃本份迟到数据。
            time.sleep(self.backfill_delay)
            if self._stop.is_set():
                return
            outcome = None
            with self.lock:
                if self._closed or generation != self.generation:
                    return
                if self._meta_get_locked("backfill_running") != "1":
                    return
                try:
                    if self.is_suspended_locked(idx):
                        # 取数后被挂起:丢弃本次备份数据,等放行后再取
                        outcome = "late"
                    else:
                        cur = self.db.execute(
                            "INSERT OR IGNORE INTO blocks(idx,data,source,updated_at) "
                            "VALUES(?,?,?,?)",
                            (idx, data, "backfill", time.time()),
                        )
                        if cur.rowcount:
                            self._segment_add_locked(idx)
                            self._bump_present_locked(+1)
                            outcome = "filled"
                        else:
                            # 业务写入已抢先落盘:迟到的备份数据不能覆盖它
                            outcome = "late"
                    # 游标持久化:重启后从已落盘的进度继续,而不是从 0 重扫。
                    # 取数窗口内若有块被放行导致游标回拨,保留回拨值,让后台回头补齐。
                    if self._cursor_locked() == used_cursor:
                        self._meta_set_locked("backfill_cursor", str(idx + 1))
                    self.db.commit()
                except Exception:
                    self.db.rollback()
                    raise
                if outcome == "filled":
                    self.stats["backfilled"] += 1
                elif outcome == "late":
                    self.stats["late_ignored"] += 1

    def start_backfill(self):
        with self.lock:
            if self._meta_get_locked("backup_disconnected") == "1":
                return False, "备份已断开,无法回填"
            if self.missing_count_locked() == 0:
                self._meta_set_locked("backfill_running", "0")
                self.db.commit()
                return True, "没有缺块,无需回填"
            if (
                self._meta_get_locked("backfill_running") == "1"
                and self._worker is not None
                and self._worker.is_alive()
            ):
                return True, "回填已在进行中"
            self._meta_set_locked("backfill_running", "1")
            self.db.commit()
            self._stop.clear()
            self._worker = threading.Thread(
                target=self._backfill_loop, args=(self.generation,),
                daemon=True, name="backfill",
            )
            self._worker.start()
            return True, "后台回填已启动"

    def maybe_resume(self):
        """进程重启后:若上次回填未完成,自动继续(游标持久化,从进度续填)。"""
        with self.lock:
            should = (
                self._meta_get_locked("backfill_running") == "1"
                and self._meta_get_locked("backup_disconnected") != "1"
                and self.missing_count_locked() > 0
            )
        if should:
            ok, msg = self.start_backfill()
            log.info("检测到未完成的回填,已自动继续: %s", msg)
            return True
        return False

    def _join_worker(self):
        worker, self._worker = self._worker, None
        if worker is not None and worker.is_alive():
            worker.join(timeout=3)

    def stop(self):
        """停止 worker(不清持久状态,模拟进程停止;重启后 maybe_resume 继续)。"""
        with self.lock:
            self.generation += 1
            self._stop.set()
        self._join_worker()
        with self.lock:
            self._stop.clear()

    def reset(self):
        """清空本地卷(演示用):块、挂起、区间、游标、断开标记全部复位。

        先递增世代并停掉旧 worker,旧 worker 的任何在途选块结果都不会写入新卷。
        """
        with self.lock:
            self.generation += 1
            self._stop.set()
            self._meta_set_locked("backfill_running", "0")
            self.db.commit()
        self._join_worker()
        with self.lock:
            try:
                self.db.execute("DELETE FROM blocks")
                self.db.execute("DELETE FROM suspended")
                self.db.execute("DELETE FROM segments")
                self._meta_set_locked("backfill_cursor", "0")
                self._meta_set_locked("present_count", "0")
                self._meta_set_locked("backup_disconnected", "0")
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            for k in self.stats:
                self.stats[k] = 0
            self._stop.clear()

    # ---------- 断开备份 ----------
    def disconnect(self):
        with self.lock:
            if self._meta_get_locked("backup_disconnected") == "1":
                return False, "备份已断开,无需重复操作"
            missing = self.missing_count_locked()
            if missing:
                return False, f"还有 {missing} 个缺块未回填(含挂起未放行),拒绝断开备份"
            try:
                self._meta_set_locked("backup_disconnected", "1")
                self._meta_set_locked("backfill_running", "0")
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            if self._backup_fd is not None:
                os.close(self._backup_fd)
                self._backup_fd = None
            return True, "缺块已补齐,备份已断开;数据盘转为纯本地读写"

    # ---------- 状态 ----------
    def status(self):
        with self.lock:
            present = min(self._present_count_locked(), self.block_count)
            (seg_rows,) = self.db.execute("SELECT COUNT(*) FROM segments").fetchone()
            (susp_count,) = self.db.execute("SELECT COUNT(*) FROM suspended").fetchone()
            return {
                "block_size": self.block_size,
                "block_count": self.block_count,
                "present": present,
                "missing": self.block_count - present,
                "progress": round(present * 100.0 / self.block_count, 1),
                "backfill_running": self._meta_get_locked("backfill_running") == "1",
                "backfill_complete": present == self.block_count,
                "backup_disconnected": self._meta_get_locked("backup_disconnected") == "1",
                # 只给计数;具体挂起块按段从 /api/blocks 取,避免把全卷块状态载入内存/页面。
                "suspended_count": susp_count,
                "backfill_cursor": self._cursor_locked(),
                "segment_rows": seg_rows,
                "stats": dict(self.stats),
            }

    def close(self):
        self.stop()
        with self.lock:
            self._closed = True
            if self._backup_fd is not None:
                os.close(self._backup_fd)
                self._backup_fd = None
            self.db.close()


def _preview_text(data: bytes) -> str:
    return data.split(b"\x00")[0].decode("utf-8", "replace")[:96]


def create_app(config=None):
    cfg = {
        "BLOCK_SIZE": int(os.environ.get("BLOCK_SIZE", 4096)),
        "BLOCK_COUNT": int(os.environ.get("BLOCK_COUNT", 256)),
        "BACKFILL_DELAY": float(os.environ.get("BACKFILL_DELAY", 0.05)),
        "DATA_DIR": os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data")),
        "BACKUP_IMAGE": os.environ.get(
            "BACKUP_IMAGE", os.path.join(os.path.dirname(__file__), "backup.img")
        ),
    }
    if config:
        cfg.update(config)

    if not os.path.exists(cfg["BACKUP_IMAGE"]):
        log.info("备份镜像不存在,生成确定性镜像: %s", cfg["BACKUP_IMAGE"])
        make_backup_image(cfg["BACKUP_IMAGE"], cfg["BLOCK_COUNT"], cfg["BLOCK_SIZE"])

    vol = Volume(
        cfg["DATA_DIR"], cfg["BACKUP_IMAGE"],
        cfg["BLOCK_SIZE"], cfg["BLOCK_COUNT"], cfg["BACKFILL_DELAY"],
    )
    resumed = vol.maybe_resume()

    app = Flask(__name__)
    app.vol = vol  # 便于测试关停

    def err(message, code):
        return jsonify(ok=False, message=message), code

    def check_idx(idx):
        return 0 <= idx < vol.block_count

    # ---------- 页面 ----------
    @app.get("/")
    def index():
        return render_template("index.html")

    # ---------- 状态 ----------
    @app.get("/api/status")
    def api_status():
        st = vol.status()
        st["resumed_on_boot"] = resumed
        return jsonify(st)

    @app.get("/api/blocks")
    def api_blocks():
        """按段读取块状态:?start=<块号>&count=<块数>,默认当前第一段。"""
        start = request.args.get("start", 0, type=int)
        count = request.args.get("count", DEFAULT_SEGMENT_SIZE, type=int)
        if start is None or start < 0:
            start = 0
        if count is None or count <= 0:
            count = DEFAULT_SEGMENT_SIZE
        j = vol.segment_state(start, count)
        j["block_count"] = vol.block_count
        return jsonify(j)

    # ---------- 块读写 ----------
    @app.get("/api/block/<int:idx>")
    def api_read(idx):
        if not check_idx(idx):
            return err("块号越界", 404)
        status, data, origin = vol.read_block(idx)
        if status == "local":
            return jsonify(ok=True, idx=idx, source="local", origin=origin,
                           text=_preview_text(data), size=len(data))
        if status == "backup":
            return jsonify(ok=True, idx=idx, source="backup",
                           text=_preview_text(data), size=len(data))
        if status == "suspended":
            return err(f"块 {idx} 本地缺失,且其后台备份读取已挂起", 409)
        return err(f"块 {idx} 本地缺失,且备份已断开", 410)

    @app.post("/api/block/<int:idx>/write")
    def api_write(idx):
        if not check_idx(idx):
            return err("块号越界", 404)
        body = request.get_json(silent=True) or {}
        text = body.get("data")
        if text is None:
            return err("缺少 data 字段", 400)
        raw = str(text).encode("utf-8")
        if len(raw) > vol.block_size:
            return err(f"数据 {len(raw)} 字节,超过整块大小 {vol.block_size} 字节", 400)
        vol.write_block(idx, raw.ljust(vol.block_size, b"\x00"))
        return jsonify(ok=True, message=f"块 {idx} 已整块写入本地({vol.block_size} 字节)")

    # ---------- 挂起 / 放行 ----------
    @app.post("/api/block/<int:idx>/suspend")
    def api_suspend(idx):
        if not check_idx(idx):
            return err("块号越界", 404)
        vol.suspend(idx)
        return jsonify(ok=True, message=f"块 {idx} 的后台备份读取已挂起")

    @app.post("/api/block/<int:idx>/resume")
    def api_resume(idx):
        if not check_idx(idx):
            return err("块号越界", 404)
        vol.resume(idx)
        return jsonify(ok=True, message=f"块 {idx} 的后台备份读取已放行")

    # ---------- 回填 / 断开 / 重置 ----------
    @app.post("/api/backfill/start")
    def api_backfill_start():
        ok, msg = vol.start_backfill()
        return jsonify(ok=ok, message=msg), (200 if ok else 409)

    @app.post("/api/disconnect")
    def api_disconnect():
        ok, msg = vol.disconnect()
        return jsonify(ok=ok, message=msg), (200 if ok else 409)

    @app.post("/api/reset")
    def api_reset():
        vol.reset()
        return jsonify(ok=True, message="本地卷已重置(全部缺块,备份恢复连接)")

    return app


if __name__ == "__main__":
    application = create_app()
    port = int(os.environ.get("PORT", 5000))
    log.info("监听 0.0.0.0:%d", port)
    application.run(host="0.0.0.0", port=port, threaded=True)
