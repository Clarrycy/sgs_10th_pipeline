#!/usr/bin/env python3
"""
SGS 2v2 排位录像解析（mode=8，4人）

输入:  data/replays/2v2/*.sgs
输出:  data/output/parsed_2v2.csv（首次创建，后续增量 append + 去重）

座次判定:
  事件 0xBB935C80 payload[1]==0x02 → Pattern A（忠先手）
  事件 0xBB935C80 payload[1]==0x04 → Pattern B（反先手）

  Pattern A: SEAT_NAMES = [四, 一, 二, 三]（忠 0&1 = 先手 14）
  Pattern B: SEAT_NAMES = [二, 三, 四, 一]（反 2&3 = 先手 14）

用法:
  python pipeline/parse_2v2.py
  python pipeline/parse_2v2.py --quiet     # 减少输出，适合大批量
"""

import os
import sys
import glob
import csv
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import (
    parse_header_only, parse_proto, find_events_start, iter_frames,
    parse_events, load_mapping, propagate_results, gname,
    RESULT_MAP, gameid_to_time,
)

# ─────────────────── 常量 ───────────────────

MODE_ID      = 8
PLAYER_COUNT = 4
MSG_SEAT     = 0xBB935C80   # 座次判定事件
MSG_ELO      = 0x000335A7   # Elo 匹配分事件

# Pattern A: seat 0=四号位, 1=一号位, 2=二号位, 3=三号位 → 忠(0&1)=先手
# Pattern B: seat 0=二号位, 1=三号位, 2=四号位, 3=一号位 → 反(2&3)=先手
DISPLAY_A = {0: 4, 1: 1, 2: 2, 3: 3}
DISPLAY_B = {0: 2, 1: 3, 2: 4, 3: 1}

HEADERS = [
    'GameID', '对局时间', '座位', '玩家昵称', 'UserID', '官阶',
    '选将', '阵营', '胜负', '出框武将', '官阶积分', 'Elo',
]

FLUSH_EVERY = 500

ROOT       = Path(__file__).resolve().parent.parent
INPUT_DIR  = ROOT / 'data' / 'replays' / '2v2'
OUTPUT_DIR = ROOT / 'data' / 'output'
OUT_PATH   = OUTPUT_DIR / 'parsed_2v2.csv'

# ─────────────────── 座次 Pattern + Elo 提取 ───────────────────

def parse_seat_pattern_and_elo(data):
    """
    从事件流提取座次 Pattern 和 Elo 分。
    返回 (pattern, elo_dict)
      pattern: 'A' / 'B' / None
      elo_dict: {proto_seat: elo_score}
    """
    ev_start = find_events_start(data)
    if ev_start is None:
        return None, {}

    pattern = None
    elo = {}

    for msg_type, payload in iter_frames(data, ev_start):
        if msg_type == MSG_SEAT and pattern is None:
            if len(payload) >= 2:
                if payload[1] == 0x02:
                    pattern = 'A'
                elif payload[1] == 0x04:
                    pattern = 'B'

        elif msg_type == MSG_ELO and not elo:
            for fn, wt, val in parse_proto(payload):
                if fn == 2 and wt == 2:
                    seat_val = elo_val = None
                    for sf, sw, sv in parse_proto(val):
                        if sf == 1 and sw == 0:
                            seat_val = sv
                        elif sf == 2 and sw == 0:
                            elo_val = sv
                    if seat_val is not None and elo_val is not None:
                        elo[seat_val] = elo_val

        if pattern and elo:
            break

    return pattern, elo

# ─────────────────── 构建行 ───────────────────

def build_rows(data, header, picks, candidates, results, mapping):
    pattern, elo = parse_seat_pattern_and_elo(data)

    if pattern == 'A':
        display_map = DISPLAY_A
        camp = {0: '先手(14)', 1: '先手(14)', 2: '后手(23)', 3: '后手(23)'}
        teams = [[0, 1], [2, 3]]
    elif pattern == 'B':
        display_map = DISPLAY_B
        camp = {0: '后手(23)', 1: '后手(23)', 2: '先手(14)', 3: '先手(14)'}
        teams = [[0, 1], [2, 3]]
    else:
        display_map = {0: 1, 1: 2, 2: 3, 3: 4}
        camp = {i: '未知' for i in range(4)}
        teams = [[0, 1], [2, 3]]

    seat_player = {p['seat']: p for p in header['players']}
    propagate_results(results, teams)

    gid_str   = str(header['game_id'])
    game_time = gameid_to_time(gid_str)

    rows = []
    for proto_seat in range(PLAYER_COUNT):
        p    = seat_player.get(proto_seat, {})
        disp = display_map[proto_seat]
        rows.append({
            'GameID':   gid_str,
            '对局时间': game_time,
            '座位':     str(disp),
            '玩家昵称': p.get('name', '') or f'逃跑_{str(p.get("pid","0000"))[-4:]}',
            'UserID':   str(p.get('pid', '')),
            '官阶':     _rank_name(p.get('rank_code')),
            '选将':     gname(mapping, picks.get(proto_seat)),
            '阵营':     camp[proto_seat],
            '胜负':     RESULT_MAP.get(results.get(proto_seat), ''),
            '出框武将': ', '.join(gname(mapping, c) for c in candidates.get(proto_seat, [])),
            '官阶积分': str(p.get('rank_score', '')) if p.get('rank_score') is not None else '',
            'Elo':      str(elo.get(proto_seat, '')),
        })

    rows.sort(key=lambda r: int(r['座位']))
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
        print(f'🔍 已有 {len(seen_ids)} 个 GameID（跳过重复）')

    first_run = not OUT_PATH.is_file()
    csv_file  = open(OUT_PATH, 'w' if first_run else 'a', newline='', encoding='utf-8-sig')
    writer    = csv.DictWriter(csv_file, fieldnames=HEADERS)
    if first_run or OUT_PATH.stat().st_size == 0:
        writer.writeheader()

    buf = []
    total = skipped_dup = skipped_other = no_pattern = 0

    def flush():
        nonlocal buf
        if buf:
            writer.writerows(buf)
            csv_file.flush()
            buf = []

    try:
        for i, fpath in enumerate(sgs_files, 1):
            if not quiet and (i % 5000 == 0 or i == len(sgs_files)):
                print(f'  … {i}/{len(sgs_files)} 文件，已解析 {total} 场')

            with open(fpath, 'rb') as f:
                raw = f.read()

            header = parse_header_only(raw)
            if header is None or header['game_id'] is None:
                skipped_other += 1
                continue
            if header.get('mode_id') != MODE_ID:
                skipped_other += 1
                continue

            gid_str = str(header['game_id'])
            if gid_str in seen_ids:
                skipped_dup += 1
                continue
            seen_ids.add(gid_str)

            picks, candidates, results = parse_events(raw, header)
            rows = build_rows(raw, header, picks, candidates, results, mapping)

            if rows and rows[0]['阵营'] == '未知':
                no_pattern += 1

            buf.extend(rows)
            total += 1

            if total % FLUSH_EVERY == 0:
                flush()

        flush()
    finally:
        csv_file.close()

    print(f'\n✅ 完成！解析 {total} 场（{total * PLAYER_COUNT} 行）')
    if skipped_dup:
        print(f'⏭️  跳过重复 {skipped_dup} 个')
    if skipped_other:
        print(f'⚠️  跳过无效/非2v2 {skipped_other} 个')
    if no_pattern:
        print(f'⚠️  {no_pattern} 局未能识别座次 Pattern')
    print(f'📄 {OUT_PATH}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='解析 2v2 .sgs → CSV')
    ap.add_argument('--quiet', action='store_true', help='减少输出')
    args = ap.parse_args()
    process(quiet=args.quiet)
