#!/usr/bin/env python3
"""
一次性脚本：为已有斗地主记录回填 rank_score。

从 CDN 下载 .sgs 文件，解析 header 中的 rank_score，
UPDATE doudizhu SET rank_score = ? WHERE game_id = ? AND seat = ?

用法:
  python pipeline/backfill_rank_score.py
  python pipeline/backfill_rank_score.py --dry-run
  python pipeline/backfill_rank_score.py --workers=30
"""

import sys
import time
import asyncio
import argparse
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print('❌ 缺少 aiohttp，请执行：pip install aiohttp')
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).parent))
from common import parse_header_only
from db import get_conn, DB_PATH

# ─────────────────── 常量 ───────────────────

CDN_BASE   = 'https://yjcmgamevideofile.sanguosha.com/service/'
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36'

BATCH_SIZE    = 500   # commit every N games
PROGRESS_EVERY = 1000  # print progress every N games
WORKERS       = 50    # default concurrent connections


# ─────────────────── 查找待回填的 game_id ───────────────────

def find_null_rank_score_games(conn):
    """
    返回所有 doudizhu game_id，其中该 game_id 的所有行 rank_score 都是 NULL。
    即：没有任何一个 seat 有 rank_score 的 game_id。
    """
    cur = conn.execute("""
        SELECT DISTINCT game_id
        FROM doudizhu
        WHERE game_id NOT IN (
            SELECT DISTINCT game_id FROM doudizhu WHERE rank_score IS NOT NULL
        )
    """)
    return [row[0] for row in cur.fetchall()]


# ─────────────────── 异步下载 + 解析 ───────────────────

async def fetch_and_parse(session, game_id, sem):
    """下载 .sgs 并解析 header，返回 (game_id, players_list) 或 (game_id, None)。"""
    url = f'{CDN_BASE}{game_id}.sgs'
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return game_id, None, f'http_{resp.status}'
                data = await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            return game_id, None, f'network_error'

    try:
        header = parse_header_only(data)
        if header is None or header['game_id'] is None:
            return game_id, None, 'bad_header'
        return game_id, header['players'], None
    except Exception as e:
        return game_id, None, f'parse_error: {e}'


# ─────────────────── 主流程 ───────────────────

async def backfill(dry_run=False, workers=WORKERS):
    conn = get_conn()

    print(f'🔍 查找 rank_score 全为 NULL 的斗地主 game_id ...')
    game_ids = find_null_rank_score_games(conn)
    total = len(game_ids)
    print(f'   找到 {total} 个待回填的 game_id')

    if total == 0:
        print('✅ 无需回填')
        conn.close()
        return

    if dry_run:
        print(f'🏃 --dry-run 模式，不会写入数据库')

    connector = aiohttp.TCPConnector(
        limit=workers,
        ttl_dns_cache=600,
        keepalive_timeout=30,
        enable_cleanup_closed=True,
    )
    sem = asyncio.Semaphore(workers)

    updated = 0
    failed = 0
    errors = {}  # error_type → count
    t0 = time.time()
    pending_updates = []  # list of (rank_score, game_id, seat)

    def flush_updates():
        nonlocal pending_updates
        if pending_updates and not dry_run:
            conn.executemany(
                "UPDATE doudizhu SET rank_score = ? WHERE game_id = ? AND seat = ?",
                pending_updates,
            )
            conn.commit()
        pending_updates = []

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            headers={'User-Agent': USER_AGENT},
        ) as session:
            tasks = [
                asyncio.ensure_future(fetch_and_parse(session, gid, sem))
                for gid in game_ids
            ]

            for i, coro in enumerate(asyncio.as_completed(tasks), 1):
                game_id, players, err = await coro

                if err is not None:
                    failed += 1
                    errors[err] = errors.get(err, 0) + 1
                else:
                    updated += 1
                    for p in players:
                        rank_score = p.get('rank_score')
                        if rank_score is not None:
                            # header seat is 0-indexed, DB seat is 1-indexed
                            pending_updates.append((rank_score, game_id, p['seat'] + 1))

                # Batch commit
                if updated > 0 and updated % BATCH_SIZE == 0:
                    flush_updates()

                # Progress
                if i % PROGRESS_EVERY == 0 or i == total:
                    elapsed = time.time() - t0
                    rate = i / elapsed if elapsed > 0 else 0
                    print(
                        f'\r  [{i}/{total}] ✓{updated} ✗{failed} | {rate:.1f} games/sec',
                        end='', flush=True,
                    )

        # Final flush
        flush_updates()
        print()

    finally:
        conn.close()

    # ─── Summary ───
    elapsed = time.time() - t0
    print(f'\n{"[DRY RUN] " if dry_run else ""}完成！')
    print(f'  更新: {updated} 个 game_id')
    print(f'  失败: {failed} 个 game_id')
    print(f'  耗时: {elapsed:.1f}s')
    if errors:
        print(f'  错误明细:')
        for err_type, count in sorted(errors.items(), key=lambda x: -x[1]):
            print(f'    {err_type}: {count}')
    if not dry_run:
        print(f'  💾 {DB_PATH}')


def main():
    ap = argparse.ArgumentParser(
        description='一次性回填斗地主 rank_score（从 CDN 下载 .sgs header）'
    )
    ap.add_argument('--dry-run', action='store_true',
                    help='只统计，不写入数据库')
    ap.add_argument('--workers', type=int, default=WORKERS,
                    help=f'并发连接数（默认 {WORKERS}）')
    args = ap.parse_args()

    asyncio.run(backfill(dry_run=args.dry_run, workers=args.workers))


if __name__ == '__main__':
    main()
