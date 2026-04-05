#!/usr/bin/env python3
"""
SGS 录像批量下载脚本

用法:
  python pipeline/download.py [--days=N] [--modes=8,36] [--workers=50]
  python pipeline/download.py --use-indexes [--days=N] [--workers=50]
  python pipeline/download.py --cleanup --days=30

输入:
  默认模式:  data/gameids/*.json（所有 GameID 文件自动合并）
  索引模式:  data/indexes/index_ranked.json + index_doudizhu.json
             （跳过 identity，仅下载 replayDownloaded=False 的 GameID）

输出:  data/replays/{2v2,doudizhu,other}/  +  data/output/index.csv

去重:  基于 data/output/index.csv（O(1) 查找，下载前过滤）

支持:
  - aiohttp 异步并发 + HTTP keep-alive 长连接
  - 内存中解析 mode_id → 自动分类到模式子目录
  - --use-indexes 从 per-mode index JSON 读取待下载列表（跳过身份）
  - --days=N      只下载最近 N 天的对局（按 GameID 时间戳过滤）
  - --modes=8,36  只下载指定模式（8=2v2, 36=斗地主, 4=八人; 默认全部）
  - --workers=N   并发连接数（默认 50）
  - --cleanup     删除本地 data/replays/ 中超过 --days 天的 .sgs 文件
"""

import json
import os
import sys
import time
import csv
import glob
import asyncio
import argparse
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print('❌ 缺少 aiohttp，请执行：pip install aiohttp')
    sys.exit(1)

# ─────────────────── 导入公共库 ───────────────────

sys.path.insert(0, str(Path(__file__).parent))
from common import parse_proto, decode_varint, gameid_to_time, gameid_to_timestamp

# ─────────────────── 常量 ───────────────────

CDN_BASE  = 'https://yjcmgamevideofile.sanguosha.com/service/'
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36'

MODE_MAP = {
    4:  '八人',
    8:  '2v2',
    36: '斗地主',
}

ROOT       = Path(__file__).resolve().parent.parent
DATA_DIR   = ROOT / 'data'
REPLAY_DIR = DATA_DIR / 'replays'
OUTPUT_DIR = DATA_DIR / 'output'
INDEX_PATH = OUTPUT_DIR / 'index.csv'
GAMEID_DIR = DATA_DIR / 'gameids'
INDEXES_DIR = DATA_DIR / 'indexes'

# 索引模式下需要下载的 mode（跳过身份 mode=4）
DOWNLOAD_MODES = {8, 36}

INDEX_FIELDS = ['GameID', 'mode', '模式', '来源', '对局时间', '下载时间', '文件大小']

# ─────────────────── mode 检测（内存中） ───────────────────

def detect_mode(data):
    """从 .sgs 二进制数据解析 mode_id，失败返回 None"""
    if len(data) < 0x40 or data[:4] != b'sgsz':
        return None
    hdr = data[0x37:min(len(data), 0x37 + 500)]
    pos = 0
    while pos < len(hdr):
        try:
            key, new_pos = decode_varint(hdr, pos)
        except Exception:
            break
        if key == 0 or new_pos == pos:
            break
        fn = key >> 3
        wt = key & 7
        if fn == 0 or fn > 10000:
            break
        pos = new_pos
        if wt == 0:
            val, pos = decode_varint(hdr, pos)
            if fn == 1:
                return val  # field 1 = mode_id
        elif wt == 2:
            length, pos = decode_varint(hdr, pos)
            if length < 0 or pos + length > len(hdr):
                break
            pos += length
        elif wt == 1:
            pos += 8
        elif wt == 5:
            pos += 4
        else:
            break
    return None


def replay_subdir(mode):
    """mode_id → 子目录名"""
    return MODE_MAP.get(mode, 'other')

# ─────────────────── index.csv 管理 ───────────────────

def load_index():
    """加载已有 index.csv → set of GameID strings"""
    existing = set()
    if INDEX_PATH.is_file():
        with open(INDEX_PATH, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                gid = row.get('GameID', '').strip()
                if gid:
                    existing.add(gid)
    return existing


class IndexWriter:
    def __init__(self):
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        need_header = not INDEX_PATH.is_file() or INDEX_PATH.stat().st_size == 0
        self._fh = open(INDEX_PATH, 'a', newline='', encoding='utf-8-sig')
        self._writer = csv.DictWriter(self._fh, fieldnames=INDEX_FIELDS)
        if need_header:
            self._writer.writeheader()
            self._fh.flush()

    def append(self, row):
        self._writer.writerow(row)

    def flush(self):
        self._fh.flush()

    def close(self):
        self._fh.flush()
        self._fh.close()

# ─────────────────── GameID 加载 ───────────────────

def load_all_gameids(source_tag_out=None):
    """
    扫描 data/gameids/*.json，合并所有 GameID。
    支持格式：
      - {"uniqueGameIds": [...]}
      - [{"userId": ..., "gameIds": [...]}, ...]
      - ["gid1", "gid2", ...]
    返回 list[str]（已去重，保留顺序）。
    """
    json_files = sorted(GAMEID_DIR.glob('*.json'))
    if not json_files:
        print(f'⚠️  {GAMEID_DIR} 中没有 GameID JSON 文件')
        return []

    seen = set()
    result = []
    sources = []

    for jf in json_files:
        with open(jf, 'r', encoding='utf-8') as f:
            data = json.load(f)

        ids_from_file = []
        if isinstance(data, dict) and 'uniqueGameIds' in data:
            ids_from_file = data['uniqueGameIds']
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            for entry in data:
                ids_from_file.extend(entry.get('gameIds', []))
        elif isinstance(data, list):
            ids_from_file = data

        before = len(result)
        for gid in ids_from_file:
            gid = str(gid)
            if gid not in seen:
                seen.add(gid)
                result.append(gid)
        sources.append(f'{jf.name}(+{len(result)-before})')

    print(f'📋 GameID 来源: {", ".join(sources)}')
    print(f'   合计去重后: {len(result)} 个')
    return result

# ─────────────────── 索引模式加载 ───────────────────

INDEX_MODE_FILES = {
    8:  'index_ranked.json',
    36: 'index_doudizhu.json',
}


def load_gameids_from_indexes():
    """从 per-mode index JSON 读取 replayDownloaded=False 的 GameID。
    跳过 identity (mode 4)。返回 list[str]。
    """
    result = []
    seen = set()

    for mode, fname in INDEX_MODE_FILES.items():
        fp = INDEXES_DIR / fname
        if not fp.is_file():
            continue
        with open(fp, 'r', encoding='utf-8') as f:
            data = json.load(f)

        count = 0
        for gid, entry in data.get('games', {}).items():
            if entry.get('replayDownloaded'):
                continue
            if gid not in seen:
                seen.add(gid)
                result.append(gid)
                count += 1

        print(f'  📋 {fname}: {count} 个待下载')

    print(f'  合计: {len(result)} 个 GameID（从索引）')
    return result


def update_indexes_downloaded(downloaded_ids):
    """将成功下载的 GameID 在 per-mode index 中标记 replayDownloaded=True。"""
    if not downloaded_ids:
        return

    for mode, fname in INDEX_MODE_FILES.items():
        fp = INDEXES_DIR / fname
        if not fp.is_file():
            continue
        with open(fp, 'r', encoding='utf-8') as f:
            data = json.load(f)

        updated = 0
        for gid in downloaded_ids:
            if gid in data.get('games', {}):
                data['games'][gid]['replayDownloaded'] = True
                updated += 1

        if updated > 0:
            with open(fp, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f'  ✅ {fname}: 标记 {updated} 个已下载')


# ─────────────────── 异步下载 ───────────────────

async def download_one(session, game_id, allowed_modes, index_writer, sem, source, downloaded_set):
    url = f'{CDN_BASE}{game_id}.sgs'
    today = time.strftime('%Y-%m-%d')

    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    if resp.status == 404:
                        index_writer.append({
                            'GameID': game_id, 'mode': '', '模式': f'http_{resp.status}',
                            '来源': source, '对局时间': gameid_to_time(game_id),
                            '下载时间': today, '文件大小': 0,
                        })
                    return game_id, f'http_{resp.status}', 0
                data = await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            return game_id, 'timeout', 0

    mode = detect_mode(data)

    # 模式过滤：不在白名单则记录但不保存文件
    if allowed_modes and mode not in allowed_modes:
        index_writer.append({
            'GameID': game_id, 'mode': mode if mode else '', '模式': f'skip_{mode}',
            '来源': source, '对局时间': gameid_to_time(game_id),
            '下载时间': today, '文件大小': len(data),
        })
        return game_id, 'filtered', 0

    subdir = replay_subdir(mode)
    out_dir = REPLAY_DIR / subdir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'{game_id}.sgs'

    with open(out_path, 'wb') as f:
        f.write(data)

    index_writer.append({
        'GameID': game_id,
        'mode': mode if mode is not None else '',
        '模式': subdir,
        '来源': source,
        '对局时间': gameid_to_time(game_id),
        '下载时间': today,
        '文件大小': len(data),
    })
    downloaded_set.add(game_id)
    return game_id, 'ok', len(data)


async def run_downloads(to_download, allowed_modes, workers, source):
    connector = aiohttp.TCPConnector(
        limit=workers,
        ttl_dns_cache=600,
        keepalive_timeout=30,
        enable_cleanup_closed=True,
    )
    sem = asyncio.Semaphore(workers)
    index_writer = IndexWriter()
    downloaded_set = set()  # 成功下载的 GameID 集合

    ok = err = filtered = 0
    total_bytes = 0
    total = len(to_download)
    t0 = time.time()

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            headers={'User-Agent': USER_AGENT},
        ) as session:
            tasks = [
                asyncio.ensure_future(
                    download_one(session, gid, allowed_modes, index_writer, sem, source, downloaded_set)
                )
                for gid in to_download
            ]
            for i, coro in enumerate(asyncio.as_completed(tasks), 1):
                gid, status, size = await coro
                total_bytes += size
                if status == 'ok':
                    ok += 1
                elif status == 'filtered':
                    filtered += 1
                else:
                    err += 1

                if i % 100 == 0 or i == total:
                    elapsed = time.time() - t0
                    rate = ok / elapsed if elapsed > 0 else 0
                    mb = total_bytes / 1024 / 1024
                    print(
                        f'\r  [{i}/{total}] ✓{ok} 过滤{filtered} ✗{err} | '
                        f'{mb:.1f}MB | {rate:.1f}个/秒',
                        end='', flush=True,
                    )
                    if i % 500 == 0:
                        index_writer.flush()
        print()
    finally:
        index_writer.close()

    return ok, err, filtered, total_bytes, time.time() - t0, downloaded_set

# ─────────────────── 本地清理 ───────────────────

def cleanup_old_replays(days):
    """删除 data/replays/ 中超过 days 天的 .sgs 文件（按 GameID 时间戳判断）"""
    cutoff = time.time() - days * 86400
    deleted = 0
    freed = 0
    for sgs in REPLAY_DIR.rglob('*.sgs'):
        ts = gameid_to_timestamp(sgs.stem)
        if ts and ts < cutoff:
            size = sgs.stat().st_size
            sgs.unlink()
            deleted += 1
            freed += size
    print(f'🗑️  清理完成：删除 {deleted} 个文件，释放 {freed/1024/1024:.1f} MB')

# ─────────────────── 主流程 ───────────────────

def main():
    ap = argparse.ArgumentParser(description='SGS 录像批量下载')
    ap.add_argument('--days',    type=int, default=None, help='只下载最近 N 天的对局')
    ap.add_argument('--modes',   type=str, default=None, help='只下载指定模式，逗号分隔，如 8,36')
    ap.add_argument('--workers', type=int, default=50,   help='并发数（默认 50）')
    ap.add_argument('--source',  type=str, default='auto', help='来源标签（写入 index.csv）')
    ap.add_argument('--cleanup', action='store_true', help='清理旧 .sgs 文件（需搭配 --days）')
    ap.add_argument('--use-indexes', action='store_true',
                    help='从 per-mode index JSON 读取待下载列表（跳过身份 mode 4）')
    args = ap.parse_args()

    # 清理模式
    if args.cleanup:
        days = args.days or 30
        print(f'🗑️  清理超过 {days} 天的本地录像...')
        cleanup_old_replays(days)
        return

    use_indexes = getattr(args, 'use_indexes', False)

    # 解析模式过滤
    allowed_modes = None
    if args.modes:
        allowed_modes = set(int(m.strip()) for m in args.modes.split(','))
        print(f'🎯 模式过滤：{allowed_modes}')

    # 加载所有 GameID
    if use_indexes:
        print('📋 索引模式：从 per-mode index JSON 读取...')
        all_ids = load_gameids_from_indexes()
    else:
        all_ids = load_all_gameids()
    if not all_ids:
        return

    # 时间过滤
    if args.days:
        cutoff_ts = time.time() - args.days * 86400
        before = len(all_ids)
        all_ids = [g for g in all_ids if gameid_to_timestamp(g) >= cutoff_ts]
        print(f'📅 --days={args.days}：过滤掉 {before - len(all_ids)} 个旧 GameID，剩余 {len(all_ids)} 个')

    # 去重
    existing = load_index()
    to_download = [g for g in all_ids if g not in existing]
    print(f'📦 总计 {len(all_ids)} | 已有 {len(existing)} | 待下载 {len(to_download)}')

    if not to_download:
        print('✅ 全部已下载')
        return

    print(f'🚀 开始下载（并发={args.workers}，keep-alive）...\n')
    ok, err, filtered, total_bytes, elapsed, downloaded_set = asyncio.run(
        run_downloads(to_download, allowed_modes, args.workers, args.source)
    )

    mb = total_bytes / 1024 / 1024
    print(f'\n✅ 完成！成功={ok}  过滤={filtered}  失败={err}')
    print(f'   总大小={mb:.1f}MB  耗时={elapsed:.1f}s')

    # 索引模式：回写 replayDownloaded 标记
    if use_indexes and downloaded_set:
        print(f'\n📝 更新索引（{len(downloaded_set)} 个已下载）...')
        update_indexes_downloaded(downloaded_set)

    # 各子目录统计
    for subdir in REPLAY_DIR.iterdir():
        if subdir.is_dir():
            cnt = len(list(subdir.glob('*.sgs')))
            if cnt:
                print(f'   {subdir.name}/: {cnt} 个录像')


if __name__ == '__main__':
    main()
