#!/usr/bin/env python3
"""
SGS 斗地主录像解析（mode=36，3人）

输入:  data/replays/斗地主/*.sgs
输出:  data/output/parsed_doudizhu.csv（增量 append + 去重）

地主判定：叫分事件 (MSG_BID) 中叫分最高者为地主（~5.2% 非 seat 0）。
农民同胜负；与地主对立。

用法:
  python pipeline/parse_doudizhu.py
  python pipeline/parse_doudizhu.py --quiet
"""

import sys
import csv
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import (
    parse_header_only, parse_events, load_mapping, gname,
    propagate_results, RESULT_MAP, gameid_to_time,
    parse_landlord_seat, parse_swaps,
)

# ─────────────────── 常量 ───────────────────

MODE_ID      = 36
PLAYER_COUNT = 3

HEADERS = [
    'GameID', '对局时间', '座位', '玩家昵称', 'UserID', '官阶',
    '选将ID', '选将', '阵营', '胜负', '初始出框', '换走', '换入',
]

FLUSH_EVERY = 500

ROOT       = Path(__file__).resolve().parent.parent
INPUT_DIR  = ROOT / 'data' / 'replays' / '斗地主'
OUTPUT_DIR = ROOT / 'data' / 'output'
OUT_PATH   = OUTPUT_DIR / 'parsed_doudizhu.csv'

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
            'GameID':   gid_str,
            '对局时间': game_time,
            '座位':     str(rs + 1),
            '玩家昵称': p.get('name', '') or f'逃跑_{str(p.get("pid","0000"))[-4:]}',
            'UserID':   str(p.get('pid', '')),
            '官阶':     _rank_name(p.get('rank_code')),
            '选将ID':   str(picks.get(rs, '')),
            '选将':     gname(mapping, picks.get(rs)) if picks.get(rs) else '',
            '阵营':     camp.get(rs, ''),
            '胜负':     RESULT_MAP.get(results.get(rs), ''),
            '初始出框': ', '.join(gname(mapping, c) for c in candidates.get(rs, [])),
            '换走':     ', '.join(gname(mapping, old) for old, _ in seat_swaps),
            '换入':     ', '.join(gname(mapping, new) for _, new in seat_swaps),
        })
    return rows


def _rank_name(code):
    from common import rank_name
    return rank_name(code)

# ─────────────────── 主流程 ───────────────────

def process(quiet=False):
    sgs_files = sorted(INPUT_DIR.glob('*.sgs'))
    if not sgs_files:
        print(f'❌ {INPUT_DIR} 中没有 .sgs 文件')
        return

    print(f'📂 发现 {len(sgs_files)} 个 .sgs 文件')
    mapping = load_mapping()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 加载已处理的 GameID
    seen_ids = set()
    if OUT_PATH.is_file():
        with open(OUT_PATH, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                gid = row.get('GameID', '').strip()
                if gid:
                    seen_ids.add(gid)
        if not quiet:
            print(f'🔍 已有 {len(seen_ids)} 个 GameID（跳过重复）')

    first_run = not OUT_PATH.is_file()
    csv_file  = open(OUT_PATH, 'w' if first_run else 'a', newline='', encoding='utf-8-sig')
    writer    = csv.DictWriter(csv_file, fieldnames=HEADERS)
    if first_run or OUT_PATH.stat().st_size == 0:
        writer.writeheader()

    buf = []
    total = skipped_dup = skipped_other = bad_header = 0

    def flush():
        nonlocal buf
        if buf:
            writer.writerows(buf)
            csv_file.flush()
            buf = []

    try:
        for i, fpath in enumerate(sgs_files, 1):
            if quiet and (i % 3000 == 0 or i == len(sgs_files)):
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
                landlord = 0  # 回退：seat 0 为地主

            rows = build_rows(header, picks, candidates, results, mapping, landlord, swaps)
            buf.extend(rows)
            total += 1

            if not quiet:
                landlord_tag = f' 地主=seat{landlord}' if landlord != 0 else ''
                print(f'  ✓ {gid_str} | 玩家={len(header["players"])} 选将={len(picks)}{landlord_tag}')

            if total % FLUSH_EVERY == 0:
                flush()

        flush()
    finally:
        csv_file.close()

    print(f'\n✅ 完成！解析 {total} 场（{total * PLAYER_COUNT} 行）')
    if skipped_dup:
        print(f'⏭️  跳过重复 {skipped_dup} 个')
    if skipped_other:
        print(f'⚠️  跳过非斗地主 {skipped_other} 个')
    if bad_header:
        print(f'⚠️  无法解析 header {bad_header} 个')
    print(f'📄 {OUT_PATH}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='解析斗地主 .sgs → CSV')
    ap.add_argument('--quiet', action='store_true', help='减少输出，适合大批量')
    args = ap.parse_args()
    process(quiet=args.quiet)
