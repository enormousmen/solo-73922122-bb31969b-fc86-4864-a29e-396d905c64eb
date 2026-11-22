"""单卷恢复服务关键操作自测。

运行: python3 -m pytest tests/ -v
"""
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from app import create_app  # noqa: E402

CFG = dict(BLOCK_SIZE=4096, BLOCK_COUNT=32, BACKFILL_DELAY=0.002)


def wait_for(pred, timeout=15.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(0.05)
    return False


def status(c):
    return c.get("/api/status").get_json()


@pytest.fixture()
def client(tmp_path):
    cfg = dict(CFG, DATA_DIR=str(tmp_path / "data"), BACKUP_IMAGE=str(tmp_path / "backup.img"))
    app = create_app(cfg)
    app.testing = True
    with app.test_client() as c:
        yield c
    app.vol.close()


def test_read_through_backup_and_not_persisted(client):
    """本地缺块 → 从备份读透;读透不落盘(由后台回填负责补齐)。"""
    r = client.get("/api/block/5")
    assert r.status_code == 200
    j = r.get_json()
    assert j["source"] == "backup"
    assert j["text"].startswith("BACKUP-BLOCK-000005|")
    assert j["size"] == 4096
    st = status(client)
    assert st["missing"] == 32, "读透不应落盘"
    assert st["stats"]["backup_reads"] == 1


def test_backfill_completes_and_reads_become_local(client):
    """启动回填 → 全部补齐 → 读取变为本地命中。"""
    r = client.post("/api/backfill/start")
    assert r.status_code == 200
    assert wait_for(lambda: status(client)["missing"] == 0)
    st = status(client)
    assert st["backfill_complete"] and not st["backfill_running"]
    assert st["stats"]["backfilled"] == 32
    j = client.get("/api/block/5").get_json()
    assert j["source"] == "local" and j["origin"] == "backfill"
    assert j["text"].startswith("BACKUP-BLOCK-000005|")


def test_suspend_write_resume_and_late_backup_never_overwrites(client):
    """挂起期间:读被拒、整块写入可提交;放行补齐后业务数据不被覆盖。"""
    assert client.post("/api/block/7/suspend").status_code == 200
    client.post("/api/backfill/start")
    # 其余块都补齐,只剩被挂起的 7
    assert wait_for(lambda: status(client)["missing"] == 1)
    assert 7 in status(client)["suspended"]

    # 挂起期间:缺块读取被拒(不读备份)
    r = client.get("/api/block/7")
    assert r.status_code == 409

    # 挂起期间:同一块的整块写入仍可提交
    r = client.post("/api/block/7/write", json={"data": "BUSINESS-DATA-7"})
    assert r.status_code == 200
    j = client.get("/api/block/7").get_json()
    assert j["source"] == "local" and j["origin"] == "business"
    assert j["text"] == "BUSINESS-DATA-7"

    # 还有缺块时拒绝断开备份(此时其实已补齐 7 以外的块,先验证拒绝逻辑见下)
    client.post("/api/block/7/resume")
    assert wait_for(lambda: status(client)["missing"] == 0)

    # 迟到的备份数据不能覆盖业务写入
    j = client.get("/api/block/7").get_json()
    assert j["origin"] == "business" and j["text"] == "BUSINESS-DATA-7"


def test_disconnect_refused_until_full_then_detached_volume_rw(client):
    """缺块时拒绝断开;补齐后可断开;断开后数据盘仍可读写。"""
    client.post("/api/backfill/start")
    r = client.post("/api/disconnect")
    assert r.status_code == 409
    assert "拒绝" in r.get_json()["message"]

    assert wait_for(lambda: status(client)["missing"] == 0)
    r = client.post("/api/disconnect")
    assert r.status_code == 200
    assert status(client)["backup_disconnected"]

    # 断开后:读本地、写本地都正常
    j = client.get("/api/block/3").get_json()
    assert j["source"] == "local"
    assert client.post("/api/block/3/write", json={"data": "AFTER-DISCONNECT"}).status_code == 200
    j = client.get("/api/block/3").get_json()
    assert j["text"] == "AFTER-DISCONNECT"

    # 断开后不能再回填、不能重复断开
    assert client.post("/api/backfill/start").status_code == 409
    assert client.post("/api/disconnect").status_code == 409


def test_persistence_across_restart(tmp_path):
    """服务重启后保留已落盘的块,并自动继续回填。"""
    cfg = dict(CFG, DATA_DIR=str(tmp_path / "data"), BACKUP_IMAGE=str(tmp_path / "backup.img"))

    app1 = create_app(cfg)
    c1 = app1.test_client()
    c1.post("/api/block/3/write", json={"data": "PERSIST-ME"})
    c1.post("/api/backfill/start")
    assert wait_for(lambda: c1.get("/api/status").get_json()["present"] >= 8)
    app1.vol.stop()  # 模拟进程停止(保留持久状态)

    app2 = create_app(cfg)
    c2 = app2.test_client()
    try:
        j = c2.get("/api/block/3").get_json()
        assert j["origin"] == "business" and j["text"] == "PERSIST-ME", "已落盘的块应保留"
        st = c2.get("/api/status").get_json()
        assert st["backfill_running"], "重启后应自动继续回填"
        assert wait_for(lambda: c2.get("/api/status").get_json()["missing"] == 0)
    finally:
        app2.vol.close()


def test_write_validation(client):
    """整块写入校验:超 4 KiB 拒绝;块号越界 404。"""
    r = client.post("/api/block/0/write", json={"data": "x" * 5000})
    assert r.status_code == 400
    r = client.post("/api/block/0/write", json={})
    assert r.status_code == 400
    assert client.post("/api/block/999/write", json={"data": "x"}).status_code == 404
    assert client.get("/api/block/999").status_code == 404
    # 恰好 4096 字节可以
    r = client.post("/api/block/0/write", json={"data": "y" * 4096})
    assert r.status_code == 200


def test_concurrent_business_writes_survive_backfill(client):
    """回填进行中并发业务写入:所有业务数据都不被回填覆盖。"""
    client.post("/api/backfill/start")
    written = {}
    for i in range(0, 32, 3):
        assert client.post(f"/api/block/{i}/write", json={"data": f"BIZ-{i}"}).status_code == 200
        written[i] = f"BIZ-{i}"
    assert wait_for(lambda: status(client)["missing"] == 0)
    for i, text in written.items():
        j = client.get(f"/api/block/{i}").get_json()
        assert j["origin"] == "business" and j["text"] == text, f"块 {i} 被回填覆盖"
