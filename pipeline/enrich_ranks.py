#!/usr/bin/env python3
"""
逃跑玩家官阶补全  pipeline/enrich_ranks.py

流程：
  1. 从 DB 找出 rank_name 为空且 user_id 有效的记录（两个模式表都查）
  2. 与本地缓存（data/cache/rank_cache.json）对比，过滤掉已查过的
  3. 将待查 user_id 写入 data/cache/missing_ranks.json
  4. 调用 collect/query_ranks.js 发起 WebSocket 查询
  5. 读取结果 data/cache/queried_ranks.json，更新 DB + 缓存

用法：
  python pipeline/enrich_ranks.py
  python pipeline/enrich_ranks.py --dry-run   # 只打印待查数量，不执行
"""

import json
import os
import sys
import subprocess
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from db import get_conn, DB_PATH
from common import rank_name, RANK_NAMES

ROOT       = Path(__file__).resolve().parent.parent
CACHE_DIR  = ROOT / 'data' / 'cache'
COLLECT_JS = ROOT / 'collect' / 'query_ranks.js'

MISSING_PATH = CACHE_DIR / 'missing_ranks.json'
QUERIED_PATH = CACHE_DIR / 'queried_ranks.json'
RANK_CACHE   = CACHE_DIR / 'rank_cache.json'  # user_id → {nickname, rankLevel, rankName}


def load_rank_cache() -> dict:
    if RANK_CACHE.is_file():
        return json.loads(RANK_CACHE.read_text('utf-8'))
    return {}


def save_rank_cache(cache: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    RANK_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), 'utf-8')


def find_missing_user_ids(conn) -> list[str]:
    """
    找出两张表（ranked_2v2、doudizhu）中 rank_name 为空且 user_id 有效的去重 user_id。
    排除明显无效值（0、空字符串）。
    """
    uids: set[str] = set()
    for table in ('ranked_2v2', 'doudizhu'):
        rows = conn.execute(
            f"SELECT DISTINCT user_id FROM {table} "
            f"WHERE (rank_name IS NULL OR rank_name = '') "
            f"AND user_id IS NOT NULL AND user_id != '' AND user_id != '0'"
        ).fetchall()
        uids.update(r[0] for r in rows)
    return sorted(uids)


def apply_results(conn, queried: dict, cache: dict):
    """将查询结果写回 DB（两张表），并更新本地缓存。"""
    updated = 0
    for uid, info in queried.items():
        level = info.get('rankLevel')
        if level is None:
            continue
        r_name = rank_name(level)   # 复用 common.py 的映射
        cache[uid] = {
            'nickname':  info.get('nickname', ''),
            'rankLevel': level,
            'rankName':  r_name,
        }
        for table in ('ranked_2v2', 'doudizhu'):
            cur = conn.execute(
                f"UPDATE {table} SET rank_name = ? "
                f"WHERE user_id = ? AND (rank_name IS NULL OR rank_name = '')",
                (r_name, uid)
            )
            updated += cur.rowcount
    conn.commit()
    return updated


def main():
    ap = argparse.ArgumentParser(description='逃跑玩家官阶补全')
    ap.add_argument('--dry-run', action='store_true', help='只打印待查数量，不实际执行')
    args = ap.parse_args()

    conn  = get_conn()
    cache = load_rank_cache()

    # ── 找出缺失的 user_id ──────────────────────────────────────
    all_missing = find_missing_user_ids(conn)
    # 过滤掉本地缓存里已经查过（但仍为空官阶）的
    to_query = [uid for uid in all_missing if uid not in cache]

    print(f'🔍 rank_name 缺失：{len(all_missing)} 个 user_id')
    print(f'   本地缓存命中：{len(all_missing) - len(to_query)} 个')
    print(f'   需要新查询：{len(to_query)} 个')

    # 缓存里有但 DB 还是空的 → 直接回填（不需要重新查）
    cached_fill = {uid: cache[uid] for uid in all_missing if uid in cache}
    if cached_fill:
        filled = apply_results(conn, cached_fill, cache)
        print(f'   缓存回填：{filled} 条记录')

    if not to_query:
        print('✅ 无需新查询')
        conn.close()
        return

    if args.dry_run:
        print(f'[dry-run] 将查询 {len(to_query)} 个 user_id，跳过实际执行')
        conn.close()
        return

    # ── 写入待查列表 ─────────────────────────────────────────────
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    MISSING_PATH.write_text(
        json.dumps({'userIds': to_query}, ensure_ascii=False),
        'utf-8'
    )

    # ── 调用 query_ranks.js ──────────────────────────────────────
    print(f'\n🚀 调用 query_ranks.js 查询 {len(to_query)} 个玩家...')
    env = os.environ.copy()
    result = subprocess.run(
        ['node', str(COLLECT_JS)],
        env=env,
        cwd=str(ROOT),
    )
    if result.returncode != 0:
        print('⚠️  query_ranks.js 异常退出，跳过本次补全')
        conn.close()
        return

    # ── 读取结果并写回 DB ────────────────────────────────────────
    if not QUERIED_PATH.is_file():
        print('⚠️  找不到 queried_ranks.json，跳过')
        conn.close()
        return

    queried = json.loads(QUERIED_PATH.read_text('utf-8'))
    updated = apply_results(conn, queried, cache)
    save_rank_cache(cache)

    # 清理临时文件
    MISSING_PATH.unlink(missing_ok=True)
    QUERIED_PATH.unlink(missing_ok=True)

    success = sum(1 for v in queried.values() if v.get('rankLevel') is not None)
    print(f'✅ 补全完成：{success}/{len(to_query)} 成功查询，更新 {updated} 条 DB 记录')
    conn.close()


if __name__ == '__main__':
    main()
