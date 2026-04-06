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
  python pipeline/parse_2v2.py --quiet          # 减少输出，适合大批量
  python pipeline/parse_2v2.py --update-index   # 回写 parsed 到 index_ranked.json
"""

import json
import os
import sys
import csv
import shutil
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import (
    parse_header_only, parse_proto, find_events_start, iter_frames,
    parse_events, load_mapping, propagate_results, gname,
    RESULT_MAP, gameid_to_time,
)
from db import get_conn, refresh_generals, insert_ranked_2v2, existing_game_ids, DB_PATH

# ─────────────────── 常量 ───────────────────

MODE_ID      = 8
PLAYER_COUNT = 4
MSG_SEAT     = 0xBB935C80   # 座次判定事件
MSG_ELO      = 0x000335A7   # Elo 匹配分事件

# Pattern A: seat 0=四号位, 1=一号位, 2=二号位, 3=三号位 → 忠(0&1)=先手
# Pattern B: seat 0=二号位, 1=三号位, 2=四号位, 3=一号位 → 反(2&3)=先手
DISPLAY_A = {0: 4, 1: 1, 2: 2, 3: 3}
DISPLAY_B = {0: 2, 1: 3, 2: 4, 3: 1}

FLUSH_EVERY = 500

ROOT         = Path(__file__).resolve().parent.parent
INPUT_DIR    = ROOT / 'data' / 'replays' / '2v2'
INDEXES_DIR  = ROOT / 'data' / 'indexes'
INDEX_FILE   = INDEXES_DIR / 'index_ranked.json'
ANOMALY_DIR  = ROOT / 'data' / 'replays' / '2v2_anomaly'  # 异常对局留存

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
            'game_id':     gid_str,
            'game_time':   game_time,
            'seat':        disp,
            'player_name': p.get('name', '') or f'逃跑_{str(p.get("pid","0000"))[-4:]}',
            'user_id':     str(p.get('pid', '')),
            'rank_name':   _rank_name(p.get('rank_code')),
            'general_id':  picks.get(proto_seat),
            'camp':        camp[proto_seat],
            'result':      RESULT_MAP.get(results.get(proto_seat), ''),
            'candidates':  ','.join(str(c) for c in candidates.get(proto_seat, [])),
            'rank_score':  p.get('rank_score'),
            'elo':         elo.get(proto_seat),
        })

    rows.sort(key=lambda r: r['seat'])
    return rows


def _rank_name(code):
    from common import rank_name
    return rank_name(code)

# ─────────────────── index 标记已解析 ───────────────────

def update_index(quiet=False):
    """将 index_ranked.json 中已解析的 game 标记 parsed=true（从 SQLite 读取已解析列表）。"""
    if not INDEX_FILE.is_file():
        if not quiet:
            print('ℹ️  索引文件不存在，跳过标记')
        return

    conn = get_conn()
    parsed_ids = existing_game_ids(conn, 'ranked_2v2')
    conn.close()

    with open(INDEX_FILE, 'r', encoding='utf-8') as f:
        index_data = json.load(f)

    updated = 0
    for gid, entry in index_data.get('games', {}).items():
        if gid in parsed_ids and not entry.get('parsed'):
            entry['parsed'] = True
            updated += 1

    if updated > 0:
        with open(INDEX_FILE, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)

    if not quiet:
        print(f'📝 索引标记: {updated} 个 2v2 录像标记为已解析')


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
    seen_ids = existing_game_ids(conn, 'ranked_2v2')
    if seen_ids:
        print(f'🔍 数据库已有 {len(seen_ids)} 个 GameID（跳过重复）')

    buf = []
    total = skipped_dup = skipped_other = no_pattern = 0
    no_pattern_ids = []

    def flush():
        nonlocal buf
        if buf:
            insert_ranked_2v2(conn, buf)
            conn.commit()
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

            if rows and rows[0]['camp'] == '未知':
                no_pattern += 1
                no_pattern_ids.append(gid_str)
                # 复制到异常目录留存
                ANOMALY_DIR.mkdir(parents=True, exist_ok=True)
                shutil.copy2(fpath, ANOMALY_DIR / fpath.name)

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
        print(f'⚠️  跳过无效/非2v2 {skipped_other} 个')
    if no_pattern:
        print(f'⚠️  {no_pattern} 局未能识别座次 Pattern：{", ".join(no_pattern_ids)}')
        print(f'   已复制到 {ANOMALY_DIR}')
    print(f'💾 {DB_PATH}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='解析 2v2 .sgs → CSV')
    ap.add_argument('--quiet', action='store_true', help='减少输出')
    ap.add_argument('--update-index', action='store_true',
                    help='回写 parsed 到 index_ranked.json')
    args = ap.parse_args()
    process(quiet=args.quiet)
    if args.update_index:
        update_index(quiet=args.quiet)
