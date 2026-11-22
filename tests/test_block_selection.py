"""选块 / 区间 / 升级 / 分段加载的行为自测。

运行: python3 -m pytest tests/ -v
"""
import os
import sqlite3
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from app import SCHEMA, Volume, create_app  # noqa: E402
from backup_util import make_image  # noqa: E402


def wait_for(pred, timeout=20.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(0.03)
    return False


def status(c):
    return c.get("/api/status").get_json()


def make_app(tmp_path, count=64, delay=0.001, **extra):
    cfg = dict(
        BLOCK_SIZE=4096,
        BLOCK_COUNT=count,
        BACKFILL_DELAY=delay,
        DATA_DIR=str(tmp_path / "data"),
        BACKUP_IMAGE=str(tmp_path / "backup.img"),
    )
    cfg.update(extra)
    app = create_app(cfg)
    app.testing = True
    return app


def count_sql(vol, fn):
    """统计一次操作里执行的 SQL 语句条数。"""
    seen = []

    def trace(sql):
        if not sql.lstrip().upper().startswith(("BEGIN", "COMMIT", "ROLLBACK", "PRAGMA")):
            seen.append(sql)

    vol.db.set_trace_callback(trace)
    try:
        result = fn()
    finally:
        vol.db.set_trace_callback(None)
    return result, len(seen)


@pytest.fixture()
def client(tmp_path):
    app = make_app(tmp_path)
    with app.test_client() as c:
        yield c
    app.vol.close()


# ---------------------------------------------------------------- 选块效率
def test_consecutive_suspended_blocks_skipped_as_one_segment(tmp_path):
    """连续挂起块整段跳过:选块不逐块检查,统计里只记一次跳段。"""
    app = make_app(tmp_path, count=200)
    v = app.vol
    for i in range(50, 80):          # 30 个连续块挂起
        v.suspend(i)
    with v.lock:
        assert v.segment_rows_locked() == [(50, 80)]
        # 选块从 0 开始:0..49 都缺失,应返回 0,不触碰挂起段
        assert v._next_free_locked() == (0, 0)
        # 游标正好落在挂起段首:整段跳到 80,只跳 1 段
        v._meta_set_locked("backfill_cursor", "50")
        v.db.commit()
        assert v._next_free_locked() == (80, 1)
        # 游标在挂起段前一个空洞:仍返回那个空洞,不触碰挂起段
        v._meta_set_locked("backfill_cursor", "49")
        v.db.commit()
        assert v._next_free_locked() == (49, 0)
        # 游标在段中间:同样整段跳到 80
        v._meta_set_locked("backfill_cursor", "60")
        v.db.commit()
        assert v._next_free_locked() == (80, 1)
    v.close()

    # 空卷 / 大卷上的选块 SQL 条数应是常数级,而不是随块数增长。
    import pathlib
    (tmp_path / "b").mkdir(parents=True, exist_ok=True)
    app2 = make_app(tmp_path / "b", count=4096)
    v2 = app2.vol
    (result, n) = count_sql(v2, lambda: v2._find_free_locked(0))
    assert result == (0, 0)
    assert n <= 2, f"空卷选块不应逐块检查,实际执行 {n} 条 SQL"
    v2.write_block(0, b"x" * 4096)
    v2.suspend(1)
    (result, n) = count_sql(v2, lambda: v2._find_free_locked(0))
    assert result == (2, 1), "应一次跳过区间 [0,2)"
    assert n <= 4, f"选块应整段跳过,实际执行 {n} 条 SQL"
    v2.close()


def test_present_count_stays_exact_across_write_and_backfill(tmp_path):
    """已落盘计数随写入/回填精确维护,不依赖每轮全表 COUNT。"""
    app = make_app(tmp_path, count=24, delay=0.001)
    v = app.vol
    with app.test_client() as c:
        assert status(c)["present"] == 0
        c.post("/api/block/3/write", json={"data": "BIZ"})
        assert status(c)["present"] == 1
        c.post("/api/block/3/write", json={"data": "BIZ-AGAIN"})  # 覆盖已存在块不重复计数
        assert status(c)["present"] == 1
        c.post("/api/backfill/start")
        assert wait_for(lambda: status(c)["missing"] == 0)
        assert status(c)["present"] == 24
        c.post("/api/reset")
        assert status(c)["present"] == 0
    v.close()


def test_empty_volume_creates_no_per_block_records(tmp_path):
    """空卷启动不为每块建任务/记录:区间表零行,状态查询只返回当前段。"""
    app = make_app(tmp_path, count=4096)
    v = app.vol
    with v.lock:
        (seg_rows,) = v.db.execute("SELECT COUNT(*) FROM segments").fetchone()
        (blocks,) = v.db.execute("SELECT COUNT(*) FROM blocks").fetchone()
    assert seg_rows == 0 and blocks == 0
    st = v.status()
    assert st["segment_rows"] == 0 and st["missing"] == 4096 and st["backfill_cursor"] == 0
    with app.test_client() as c:
        j = c.get("/api/blocks?start=1000&count=16").get_json()
        assert j["start"] == 1000 and j["end"] == 1016
        assert j["block_count"] == 4096
        assert j["present"] == {} and j["suspended"] == []
        # 只返回当前段的块状态,不返回全卷映射
        c.post("/api/block/1005/suspend")
        j = c.get("/api/blocks?start=1000&count=16").get_json()
        assert j["suspended"] == [1005]
        j2 = c.get("/api/blocks?start=0&count=16").get_json()
        assert 1005 not in j2["suspended"]
    v.close()


# ---------------------------------------------------------------- 放行回头补齐
def test_resume_of_skipped_block_is_backfilled(tmp_path):
    """放行已被跳过的缺块后,后台回头把它补齐。"""
    app = make_app(tmp_path, count=80, delay=0.001)
    v = app.vol
    for i in (10, 11, 12):
        v.suspend(i)
    with app.test_client() as c:
        assert c.post("/api/backfill/start").status_code == 200
        assert wait_for(lambda: status(c)["backfill_cursor"] > 20)
        assert wait_for(lambda: status(c)["missing"] == 3)  # 只剩挂起的 3 块
        st = status(c)
        assert st["backfill_running"], "只剩挂起缺块时后台应继续等放行"
        assert st["stats"]["segment_hops"] >= 1
        sleep_before = st["stats"]["backup_reads"]

        # 放行中间一块:游标回拨,后台回头补齐
        assert c.post("/api/block/11/resume").status_code == 200
        assert wait_for(lambda: status(c)["missing"] == 2)
        assert status(c)["stats"]["backup_reads"] > sleep_before

        assert c.post("/api/block/10/resume").status_code == 200
        assert c.post("/api/block/12/resume").status_code == 200
        assert wait_for(lambda: status(c)["missing"] == 0)
        for i in (10, 11, 12):
            j = c.get(f"/api/block/{i}").get_json()
            assert j["source"] == "local" and j["origin"] == "backfill"
        assert c.post("/api/disconnect").status_code == 200
    v.close()


def test_only_suspended_missing_keeps_worker_waiting_and_blocks_disconnect(tmp_path):
    """只剩挂起缺块:后台不退出、断开被拒;放行后补齐才允许断开。"""
    app = make_app(tmp_path, count=32, delay=0.001)
    v = app.vol
    v.suspend(4)
    with app.test_client() as c:
        c.post("/api/backfill/start")
        assert wait_for(lambda: status(c)["missing"] == 1)
        time.sleep(0.5)
        st = status(c)
        assert st["missing"] == 1 and st["backfill_running"], "后台应继续等放行,不能算完成"
        assert not st["backfill_complete"]
        r = c.post("/api/disconnect")
        assert r.status_code == 409 and "拒绝" in r.get_json()["message"]
        # 放行后自动补齐,无需再点启动
        assert c.post("/api/block/4/resume").status_code == 200
        assert wait_for(lambda: status(c)["missing"] == 0 and not status(c)["backfill_running"])
        assert c.post("/api/disconnect").status_code == 200
    v.close()


def test_resume_during_backfill_read_is_not_clobbered_by_cursor(tmp_path, monkeypatch):
    """取数窗口内放行一个已被跳过的缺块:worker 提交时不能把游标推回去,该块要回头补齐。"""
    app = make_app(tmp_path, count=60, delay=0.0)
    v = app.vol
    v.suspend(4)
    real_read = v.read_backup
    fired = {"done": False}

    def read_and_resume(idx):
        data = real_read(idx)
        if idx == 20 and not fired["done"]:
            fired["done"] = True
            v.resume(4)      # 游标此刻已越过 4,放行会把它回拨到 4
        return data

    monkeypatch.setattr(v, "read_backup", read_and_resume)
    try:
        with app.test_client() as c:
            c.post("/api/backfill/start")
            assert wait_for(lambda: status(c)["missing"] == 0)
            j = c.get("/api/block/4").get_json()
            assert j["source"] == "local" and j["origin"] == "backfill", "放行的块未被回头补齐"
            assert status(c)["stats"]["backfilled"] == 60
            assert fired["done"], "测试前置条件未触发"
    finally:
        v.close()


# ---------------------------------------------------------------- 业务写入
def test_business_written_block_never_dispatched_to_backfill(tmp_path):
    """业务整块写入成功的块不再派给后台:回填既不读也不覆盖它。"""
    app = make_app(tmp_path, count=40, delay=0.001)
    v = app.vol
    with app.test_client() as c:
        assert c.post("/api/block/5/write", json={"data": "BIZ-5"}).status_code == 200
        c.post("/api/backfill/start")
        assert wait_for(lambda: status(c)["missing"] == 0)
        st = status(c)
        assert st["stats"]["backfilled"] == 39, "已写块不应再被回填"
        assert st["stats"]["backup_reads"] == 39, "已写块不应再被后台读取"
        j = c.get("/api/block/5").get_json()
        assert j["origin"] == "business" and j["text"] == "BIZ-5"
    v.close()


# ---------------------------------------------------------------- 失败回滚
def test_write_failure_leaves_selection_state_unchanged(tmp_path, monkeypatch):
    """写入失败:块、区间、游标回滚,选块仍按原状态。"""
    v = make_app(tmp_path, count=32).vol
    v.write_block(0, b"a" * 4096)
    with v.lock:
        before = (v.segment_rows_locked(), v._cursor_locked())

    def boom(idx):
        raise sqlite3.OperationalError("injected")

    monkeypatch.setattr(v, "_segment_add_locked", boom)
    with pytest.raises(sqlite3.OperationalError):
        v.write_block(1, b"b" * 4096)
    with v.lock:
        assert v.segment_rows_locked() == before[0]
        assert v._cursor_locked() == before[1]
        assert v.db.execute("SELECT 1 FROM blocks WHERE idx=1").fetchone() is None
        # 选择仍然认为 1 是缺块
        assert v._find_free_locked(0) == (1, 1)
    monkeypatch.undo()
    v.write_block(1, b"b" * 4096)
    with v.lock:
        assert v._find_free_locked(0) == (2, 1)
    v.close()


def test_suspend_and_resume_failure_keep_original_state(tmp_path, monkeypatch):
    """挂起 / 放行失败:挂起表与区间一起回滚,选块不受影响。"""
    v = make_app(tmp_path, count=32).vol

    def boom(idx):
        raise sqlite3.OperationalError("injected")

    monkeypatch.setattr(v, "_segment_add_locked", boom)
    with pytest.raises(sqlite3.OperationalError):
        v.suspend(3)
    assert v.db.execute("SELECT COUNT(*) FROM suspended").fetchone()[0] == 0
    with v.lock:
        assert v.segment_rows_locked() == []
        assert v._find_free_locked(0) == (0, 0)
    monkeypatch.undo()

    v.suspend(3)
    monkeypatch.setattr(v, "_segment_remove_locked", boom)
    with pytest.raises(sqlite3.OperationalError):
        v.resume(3)
    assert [r[0] for r in v.db.execute("SELECT idx FROM suspended")] == [3]
    with v.lock:
        assert v.segment_rows_locked() == [(3, 4)]
        assert v._find_free_locked(3) == (4, 1)
    monkeypatch.undo()
    v.resume(3)
    assert v.db.execute("SELECT COUNT(*) FROM suspended").fetchone()[0] == 0
    v.close()


# ---------------------------------------------------------------- 重置 / 重启
def test_reset_discards_old_selection_results(tmp_path):
    """重置卷后旧 worker 的在途选块结果不能写进新卷。"""
    app = make_app(tmp_path, count=48, delay=0.2)  # 拉长取数窗口,制造在途任务
    v = app.vol
    with app.test_client() as c:
        c.post("/api/backfill/start")
        assert wait_for(lambda: status(c)["present"] >= 1)
        assert c.post("/api/reset").status_code == 200
        st = status(c)
        assert st["present"] == 0 and st["missing"] == 48
        assert st["segment_rows"] == 0 and st["backfill_cursor"] == 0
        assert not st["backfill_running"]
        time.sleep(0.6)  # 给旧 worker 醒来并尝试落盘的机会
        st = status(c)
        assert st["present"] == 0, "旧选块结果影响了新卷"
        assert st["segment_rows"] == 0
        # 新卷可正常重新回填
        c.post("/api/backfill/start")
        assert wait_for(lambda: status(c)["missing"] == 0)
    v.close()


def test_restart_resumes_from_persisted_cursor(tmp_path):
    """重启后从已落盘的游标续填,而不是从 0 重扫。"""
    app = make_app(tmp_path, count=120, delay=0.001)
    v = app.vol
    for i in range(50):
        v.write_block(i, b"x" * 4096)
    with v.lock:
        v._meta_set_locked("backfill_cursor", "50")
        v._meta_set_locked("backfill_running", "1")
        v.db.commit()
    v.close()

    app2 = make_app(tmp_path, count=120, delay=0.001)
    v2 = app2.vol
    with app2.test_client() as c:
        st = status(c)
        assert st["resumed_on_boot"] is True
        assert st["backfill_cursor"] == 50
        assert wait_for(lambda: status(c)["missing"] == 0)
        st = status(c)
        assert st["stats"]["backfilled"] == 70
        assert st["stats"]["backup_reads"] == 70, "重启后不应重读已落盘的块"
    v2.close()


def test_legacy_sqlite_volume_is_upgraded_with_one_scan(tmp_path):
    """兼容旧 SQLite 卷:首次升级顺序扫描已有记录,之后从空洞继续。"""
    data = tmp_path / "data"
    data.mkdir()
    backup = str(tmp_path / "backup.img")
    make_image(backup, 64, 4096)
    db = sqlite3.connect(str(data / "volume.db"))
    # 旧版 schema:只有 blocks / suspended / meta,没有 segments 与选块游标。
    db.executescript(
        """
        CREATE TABLE blocks(idx INTEGER PRIMARY KEY, data BLOB NOT NULL,
                            source TEXT NOT NULL, updated_at REAL NOT NULL);
        CREATE TABLE suspended(idx INTEGER PRIMARY KEY, suspended_at REAL NOT NULL);
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """
    )
    # 旧库:0..9 已落盘,13 被挂起(未落盘),没有 segments 表、没有选块游标
    for i in range(10):
        db.execute(
            "INSERT INTO blocks(idx,data,source,updated_at) VALUES(?,?,?,?)",
            (i, bytes([i]) * 4096, "backfill", 0.0),
        )
    db.execute("INSERT INTO suspended(idx, suspended_at) VALUES(13, 0)")
    db.execute("INSERT INTO meta(key,value) VALUES('backfill_running','1')")
    db.execute("INSERT INTO meta(key,value) VALUES('backup_disconnected','0')")
    db.commit()
    assert db.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name='segments'").fetchone()[0] == 0
    db.close()

    app = make_app(tmp_path, count=64, delay=0.001)
    v = app.vol
    with v.lock:
        # 连续块合并成区间,不是每块一条记录
        assert v.segment_rows_locked() == [(0, 10), (13, 14)]
        assert v._cursor_locked() == 10, "升级后应从第一个空洞续填"
        assert v._meta_get_locked("segment_version") == "1"
    with app.test_client() as c:
        assert wait_for(lambda: status(c)["missing"] == 1)  # 只剩挂起的 13
        st = status(c)
        assert st["backfill_running"]
        assert c.post("/api/block/13/resume").status_code == 200
        assert wait_for(lambda: status(c)["missing"] == 0)
        j = c.get("/api/block/13").get_json()
        assert j["source"] == "local" and j["origin"] == "backfill"
    v.close()
