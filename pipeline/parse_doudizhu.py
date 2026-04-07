#!/usr/bin/env python3
"""
SGS 斗地主录像解析（mode=36，3人）

输入:  data/replays/斗地主/*.sgs
输出:  data/sgs.db（SQLite，由 export_csv.py 导出 CSV）

地主判定：叫分事件 (MSG_BID) 中叫分最高者为地主（~5.2% 非 seat 0）。
农民同胜负；与地主对立。

用法:
  python pipeline/parse_doudizhu.py
  python pipeline/parse_doudizhu.py --quiet
  python pipeline/parse_doudizhu.py --update-index  # 回写 parsed 到 index_doudizhu.json
"""

import json
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import (
    parse_header_only, parse_events, load_mapping, gname,
    propagate_results, RESULT_MAP, gameid_to_time,
    parse_landlord_seat, parse_swaps,
)
from db import get_conn, refresh_generals, insert_doudizhu, existing_game_ids, DB_PATH

# ─────────────────── 常量 ───────────────────

MODE_ID      = 36
PLAYER_COUNT = 3

FLUSH_EVERY = 500

ROOT        = Path(__file__).resolve().parent.parent
INPUT_DIR   = ROOT / 'data' / 'replays' / '斗地主'
INDEXES_DIR = ROOT / 'data' / 'indexes'
INDEX_FILE  = INDEXES_DIR / 'index_doudizhu.json'

# ─────────────────── 构建行 ───────────────────

def build_rows(header, picks, candidates, results, mapping, landlord_seat, swaps):
    farmers   = [s for s in range(PLAYER_COUNT) if s != landlord_seat]
    teams     = [[landlord_seat], farmers]
    camp      = {s: '农民' for s in range(PLAYER_COUNT)}
    camp[landlord_seat] = '地主'

    seat_player = {p['seat']: p for p in header['players']}
    propagate_results(results, teams)

    gid_str   = str(header['game_id'])
    game_time = gameid_to_time(gid_str)

    rows = []
    for rs in range(PLAYER_COUNT):
        p          = seat_player.get(rs, {})
        seat_swaps = swaps.get(rs, [])
        rows.append({
            'game_id':     gid_str,
            'game_time':   game_time,
            'seat':        rs + 1,
            'player_name': p.get('name', '') or f'逃跑_{str(p.get("pid","0000"))[-4:]}',
            'user_id':     str(p.get('pid', '')),
            'rank_name':   _rank_name(p.get('rank_code')),
            'general_id':  picks.get(rs),
            'camp':        camp.get(rs, ''),
            'result':      RESULT_MAP.get(results.get(rs), ''),
            'candidates':  ','.join(str(c) for c in candidates.get(rs, [])),
            'swapped_out': ','.join(str(old) for old, _ in seat_swaps),
            'swapped_in':  ','.join(str(new) for _, new in seat_swaps),
            'rank_score':  p.get('rank_score'),
        })
    return rows


def _rank_name(code):
    from common import rank_name
    return rank_name(code)

# ─────────────────── index 标记已解析 ───────────────────

def update_index(quiet=False):
    """将 index_doudizhu.json 中已解析的 game 标记 parsed=true（从 SQLite 读取已解析列表）。"""
    if not INDEX_FILE.is_file():
        if not quiet:
            print('ℹ️  索引文件不存在，跳过标记')
        return

    conn = get_conn()
    parsed_ids = existing_game_ids(conn, 'doudizhu')
    conn.close()

    with open(INDEX_FILE, 'r', encoding='utf-8') as f:
        index_data = json.load(f)

    updated = 0
    for gid, entry in index_data.get('games', {}).items():
        if gid in parsed_ids:
            if entry.get('parsed') is not True:
                entry['parsed'] = True
                updated += 1

    if updated > 0:
        with open(INDEX_FILE, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)

    if not quiet:
        print(f'📝 索引标记: {updated} 个斗地主录像标记为已解析')


# ─────────────────── 主流程 ───────────────────

def process(quiet=False):
    sgs_files = sorted(INPUT_DIR.glob('*.sgs'))
    if not sgs_files:
        print(f'❌ {INPUT_DIR} 中没有 .sgs 文件')
        return

    print(f'📂 发现 {len(sgs_files)} 个 .sgs 文件')
    mapping = load_mapping()

    conn = get_conn()
    refresh_generals(conn)
    seen_ids = existing_game_ids(conn, 'doudizhu')
    if seen_ids and not quiet:
        print(f'🔍 数据库已有 {len(seen_ids)} 个 GameID（跳过重复）')

    buf = []
    total = skipped_dup = skipped_other = bad_header = 0

    def flush():
        nonlocal buf
        if buf:
            insert_doudizhu(conn, buf)
            conn.commit()
            buf = []

    try:
        for i, fpath in enumerate(sgs_files, 1):
            if not quiet and (i % 3000 == 0 or i == len(sgs_files)):
                print(f'  … {i}/{len(sgs_files)} 文件，已解析 {total} 场')

            try:
                with open(fpath, 'rb') as f:
                    data = f.read()
            except Exception as e:
                print(f'❌ 读取失败：{fpath.name} — {e}')
                continue

            header = parse_header_only(data)
            if header is None or header['game_id'] is None:
                bad_header += 1
                continue

            gid_str = str(header['game_id'])
            if header.get('mode_id') != MODE_ID:
                skipped_other += 1
                continue
            if gid_str in seen_ids:
                skipped_dup += 1
                continue
            seen_ids.add(gid_str)

            picks, candidates, results = parse_events(data, header)
            swaps    = parse_swaps(data)
            landlord = parse_landlord_seat(data)
            if landlord is None:
                landlord = 0

            rows = build_rows(header, picks, candidates, results, mapping, landlord, swaps)
            buf.extend(rows)
            total += 1

            if total % FLUSH_EVERY == 0:
                flush()

        flush()
    finally:
        conn.close()

    print(f'\n✅ 完成！解析 {total} 场（{total * PLAYER_COUNT} 行）')
    if skipped_dup:
        print(f'⏭️  跳过重复 {skipped_dup} 个')
    if skipped_other:
        print(f'⚠️  跳过非斗地主 {skipped_other} 个')
    if bad_header:
        print(f'⚠️  无法解析 header {bad_header} 个')
    print(f'💾 {DB_PATH}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='解析斗地主 .sgs → CSV')
    ap.add_argument('--quiet', action='store_true', help='减少输出，适合大批量')
    ap.add_argument('--update-index', action='store_true',
                    help='回写 parsed 到 index_doudizhu.json')
    args = ap.parse_args()
    process(quiet=args.quiet)
    if args.update_index:
        update_index(quiet=args.quiet)
