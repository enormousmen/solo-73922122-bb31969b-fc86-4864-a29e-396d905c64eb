#!/usr/bin/env python3
"""只读备份镜像工具。

镜像内容确定性生成:每个 4 KiB 块的内容可由块号推导,
便于端到端校验"读到的确实是备份数据"。
"""
import hashlib
import os
import sys


def block_bytes(idx: int, size: int) -> bytes:
    """第 idx 个备份块的确定性内容。"""
    header = f"BACKUP-BLOCK-{idx:06d}|".encode()
    seed = hashlib.sha256(header).digest()
    body = (seed * (size // len(seed) + 1))[: max(0, size - len(header))]
    return (header + body)[:size]


def make_image(path: str, count: int, size: int) -> None:
    """生成备份镜像文件(count 个块,每块 size 字节)。"""
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        for i in range(count):
            f.write(block_bytes(i, size))
    os.replace(tmp, path)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit("usage: backup_util.py <image-path> <block-count> <block-size>")
    _path, _count, _size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    make_image(_path, _count, _size)
    print(f"wrote {_path}: {_count} blocks x {_size} B = {_count * _size} B")
