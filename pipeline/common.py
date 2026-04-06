#!/usr/bin/env python3
"""
sgs-pipeline 公共解析库

提供 .sgs 录像文件的全部底层解析能力：
  - Protobuf varint / length-delimited 解析
  - 录像 Header 提取（game_id / mode_id / players）
  - 事件帧扫描与迭代
  - 核心事件提取（选将 / 出框 / 胜负 / 斗地主叫分 / 换将）
  - 武将映射表加载
  - GameID → 时间戳转换
  - 胜负传播工具

不依赖任何第三方库（仅标准库）。
"""

import struct
import os
import csv
import time
from pathlib import Path

# ─────────────────── Protobuf 解析 ───────────────────

def decode_varint(data, pos):
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if (b & 0x80) == 0:
            break
        shift += 7
        if shift > 63:
            break
    return result, pos


def parse_proto(data):
    """解析 protobuf 二进制片段，返回 [(field_num, wire_type, value), ...]"""
    fields = []
    pos = 0
    while pos < len(data):
        try:
            key, new_pos = decode_varint(data, pos)
        except Exception:
            break
        if key == 0 or new_pos == pos:
            break
        fn = key >> 3
        wt = key & 7
        if fn == 0 or fn > 100000:
            break
        pos = new_pos
        if wt == 0:
            try:
                val, pos = decode_varint(data, pos)
            except Exception:
                break
            fields.append((fn, wt, val))
        elif wt == 2:
            try:
                length, pos = decode_varint(data, pos)
            except Exception:
                break
            if length < 0 or pos + length > len(data):
                break
            fields.append((fn, wt, data[pos:pos + length]))
            pos += length
        elif wt == 1:
            if pos + 8 > len(data):
                break
            fields.append((fn, wt, struct.unpack_from('<Q', data, pos)[0]))
            pos += 8
        elif wt == 5:
            if pos + 4 > len(data):
                break
            fields.append((fn, wt, struct.unpack_from('<I', data, pos)[0]))
            pos += 4
        else:
            break
    return fields

# ─────────────────── 官阶映射 ───────────────────

RANK_NAMES = {
    1:  '骁卒·步卒',    2:  '骁卒·伍长',    3:  '骁卒·什长',    4:  '骁卒·队率',
    5:  '骁卒·屯长',    6:  '骁卒·部曲',
    7:  '校尉·县尉',    8:  '校尉·都尉',     9:  '校尉·步兵校尉', 10: '校尉·典军校尉',
    11: '郎将·骑郎将',  12: '郎将·车郎将',   13: '郎将·羽林中郎将', 14: '郎将·虎贲中郎将',
    15: '偏将军·折冲将军', 16: '偏将军·虎威将军', 17: '偏将军·征虏将军', 18: '偏将军·荡寇将军',
    19: '将军·监军将军', 20: '将军·抚军将军',  21: '将军·典军将军',  22: '将军·领军将军',
    23: '上将军·后将军', 24: '上将军·左将军',  25: '上将军·右将军',  26: '上将军·前将军',
    27: '国护军·护军',  28: '国护军·左护军',  29: '国护军·右护军',  30: '国护军·中护军',
    31: '国都护·都护',  32: '国都护·左都护',  33: '国都护·右都护',  34: '国都护·中都护',
    35: '卫将军',       36: '车骑将军',       37: '骠骑将军',       38: '大将军',
}


def rank_name(code):
    """官阶等级码 → '大将军 (38)' 格式"""
    if code is None:
        return ''
    name = RANK_NAMES.get(code, '未知')
    return f'{name} ({code})'

# ─────────────────── Header 解析 ───────────────────

def _parse_player_sub(data):
    fields = parse_proto(data)
    p = {'seat': 0, 'pid': None, 'name': '', 'team': None, 'rank_code': None, 'rank_score': None}
    for fn, wt, val in fields:
        if fn == 1 and wt == 0:
            p['seat'] = val
        elif fn == 2 and wt == 0:
            p['pid'] = val
        elif fn == 5 and wt == 2:
            try:
                p['name'] = val.decode('utf-8')
            except Exception:
                p['name'] = ''
        elif fn == 7 and wt == 0:
            p['team'] = val
        elif fn == 15 and wt == 2:
            for sf, sw, sv in parse_proto(val):
                if sf == 1 and sw == 0:
                    p['rank_score'] = sv
                elif sf == 2 and sw == 0:
                    p['rank_code'] = sv
    return p


def parse_header_only(data):
    """
    快速解析 .sgs header，提取 game_id / mode_id / players。
    返回 dict 或 None（文件格式不符）。
    """
    if len(data) < 0x40 or data[:4] != b'sgsz':
        return None
    hdr_data = data[0x37:min(len(data), 0x37 + 3000)]
    fields = parse_proto(hdr_data)
    info = {'game_id': None, 'mode_id': None, 'players': []}
    for fn, wt, val in fields:
        if fn == 1 and wt == 0 and info['mode_id'] is None:
            info['mode_id'] = val
        elif fn == 3 and wt == 0 and info['game_id'] is None:
            info['game_id'] = val
        elif fn == 8 and wt == 2:
            player = _parse_player_sub(val)
            if player['pid'] is not None:
                info['players'].append(player)
    return info

# ─────────────────── 事件帧扫描 ───────────────────

def find_events_start(data):
    """
    扫描事件帧起始位置（marker=1 或 2，连续两帧合理性验证）。
    返回起始偏移量，未找到返回 None。
    """
    pos = 0x100
    limit = min(len(data), 0x8000)  # 扩大到 32KB，兼容超长对局的大 header
    while pos + 32 < limit:
        marker = struct.unpack_from('<I', data, pos + 4)[0]
        if marker in (1, 2):
            size = struct.unpack_from('<I', data, pos + 12)[0]
            if 0 <= size < 50000:
                nxt = pos + 16 + size
                if nxt + 16 <= len(data):
                    m2 = struct.unpack_from('<I', data, nxt + 4)[0]
                    s2 = struct.unpack_from('<I', data, nxt + 12)[0]
                    if m2 in (1, 2) and 0 <= s2 < 50000:
                        return pos
        pos += 1
    return None


def iter_frames(data, start):
    """迭代事件帧，yield (msg_type, payload_bytes)"""
    pos = start
    while pos + 16 <= len(data):
        marker = struct.unpack_from('<I', data, pos + 4)[0]
        if marker not in (1, 2):
            break
        msg_type = struct.unpack_from('<I', data, pos + 8)[0]
        size = struct.unpack_from('<I', data, pos + 12)[0]
        if size > 50000 or pos + 16 + size > len(data):
            break
        yield msg_type, data[pos + 16: pos + 16 + size]
        pos += 16 + size

# ─────────────────── 核心事件 ID ───────────────────

MSG_PICK   = 0x6CA210E3   # 选将锁定
MSG_PANEL  = 0xCFF321A4   # 出框（初始/换将）
MSG_RESULT = 0x5804210C   # 胜负结果
MSG_BID    = 0xD18B1F3E   # 斗地主叫分

# ─────────────────── 事件提取 ───────────────────

def parse_events(data, header):
    """
    扫描事件帧，提取选将 / 出框 / 胜负。
    返回 (picks, candidates, results)
      picks:      {seat: general_id}
      candidates: {seat: [general_id, ...]}
      results:    {seat: 1(胜)/2(负)/3(平)}
    """
    ev_start = find_events_start(data)
    if ev_start is None:
        return {}, {}, {}

    pid_seat = {p['pid']: p['seat'] for p in header['players'] if p['pid'] is not None}
    picks = {}
    candidates = {}
    results = {}

    for msg_type, payload in iter_frames(data, ev_start):
        if msg_type == MSG_PICK:
            fields = parse_proto(payload)
            pid = gen_id = None
            for fn, wt, val in fields:
                if fn == 2 and wt == 0:
                    pid = val
                elif fn == 4 and wt == 0:
                    gen_id = val
            if pid is not None and gen_id is not None and pid in pid_seat:
                picks[pid_seat[pid]] = gen_id  # 末帧即最终锁定

        elif msg_type == MSG_PANEL:
            fields = parse_proto(payload)
            flag = seat_val = None
            cands = []
            for fn, wt, val in fields:
                if fn == 1 and wt == 0:
                    flag = val
                elif fn == 6 and wt == 0:
                    seat_val = val
                elif fn == 4 and wt == 2:
                    for sfn, swt, sval in parse_proto(val):
                        if sfn == 1 and swt == 0:
                            cands.append(sval)
                            break
            if flag == 1 and seat_val is not None and seat_val not in candidates:
                candidates[seat_val] = cands

        elif msg_type == MSG_RESULT:
            for fn, wt, val in parse_proto(payload):
                if fn == 3 and wt == 2:
                    r_seat = 0
                    r_res = None
                    for sfn, swt, sval in parse_proto(val):
                        if sfn == 1 and swt == 0:
                            r_seat = sval
                        elif sfn == 4 and swt == 0:
                            r_res = sval
                    if r_res is not None:
                        results[r_seat] = r_res

    return picks, candidates, results


def parse_landlord_seat(data):
    """
    从斗地主叫分事件 (MSG_BID) 提取地主座位。
    叫分最高者为地主；全员不叫时返回 None（调用方回退到默认值）。
    """
    ev_start = find_events_start(data)
    if ev_start is None:
        return None

    landlord = None
    bid_order = []

    for msg_type, payload in iter_frames(data, ev_start):
        if msg_type != MSG_BID:
            continue
        fields = parse_proto(payload)
        f5_val = None
        bids = {}
        for fn, wt, val in fields:
            if fn == 5 and wt == 0:
                f5_val = val
            elif fn == 6 and wt == 2:
                sub = parse_proto(val)
                seat = bid_val = 0
                for sf, sw, sv in sub:
                    if sf == 1 and sw == 0:
                        seat = sv
                    if sf == 2 and sw == 0:
                        bid_val = sv
                bids[seat] = bid_val
                if seat not in bid_order:
                    bid_order.append(seat)
        if f5_val == 1 and bids:
            max_bid = max(bids.values())
            winners = [s for s in bid_order if bids.get(s) == max_bid]
            landlord = winners[0] if winners else min(bids)

    return landlord


def parse_swaps(data):
    """
    从斗地主 PANEL 事件提取每个座位的换将记录。
    返回 {seat: [(old_general_id, new_general_id), ...]}
    """
    ev_start = find_events_start(data)
    if ev_start is None:
        return {}

    initial_panels = {}
    swap_events = []

    for msg_type, payload in iter_frames(data, ev_start):
        if msg_type != MSG_PANEL:
            continue
        fields = parse_proto(payload)
        flag = seat_val = slot = None
        gens = []
        for fn, wt, val in fields:
            if fn == 1 and wt == 0:
                flag = val
            elif fn == 6 and wt == 0:
                seat_val = val
            elif fn == 3 and wt == 0:
                slot = val
            elif fn == 4 and wt == 2:
                for sf, sw, sv in parse_proto(val):
                    if sf == 1 and sw == 0:
                        gens.append(sv)
                        break
                else:
                    gens.append(0)

        if flag == 1 and seat_val is not None and seat_val not in initial_panels:
            initial_panels[seat_val] = [g for g in gens if g > 0]
        elif flag == 3 and seat_val is not None and slot is not None and gens:
            swap_events.append((seat_val, slot, gens[0]))

    result = {}
    for seat, slot, new_id in swap_events:
        panel = initial_panels.get(seat, [])
        old_id = panel[slot] if slot < len(panel) else 0
        result.setdefault(seat, []).append((old_id, new_id))

    return result

# ─────────────────── 武将映射 ───────────────────

def load_mapping(path=None):
    """
    加载武将映射表 generals_mapping.csv → {GeneralID: GeneralName}。
    path: 显式指定 CSV 路径；默认读取 data/generals_mapping.csv（相对于本文件所在包）。
    """
    if path is None:
        path = Path(__file__).resolve().parent.parent / 'data' / 'generals_mapping.csv'
    path = Path(path)
    if not path.is_file():
        print(f'⚠️  未找到武将映射表：{path}，将仅显示 ID')
        return {}
    mapping = {}
    with open(path, 'r', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            try:
                mapping[int(row['GeneralID'])] = row['GeneralName']
            except (ValueError, KeyError):
                continue
    print(f'📖 武将映射表：{path.name}（{len(mapping)} 条）')
    return mapping


def gname(mapping, gid):
    """武将名(ID) 格式，如 '刘备(1)'"""
    if gid is None:
        return ''
    name = mapping.get(gid, '未知')
    return f'{name}({gid})'

# ─────────────────── GameID 工具 ───────────────────

def gameid_to_time(gid_str):
    """GameID 高 32 位为 Unix 时间戳，转换为 'YYYY-MM-DD HH:MM:SS'"""
    try:
        ts = int(gid_str) >> 32
        if 1_600_000_000 < ts < 2_000_000_000:
            return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
    except Exception:
        pass
    return ''


def gameid_to_timestamp(gid_str):
    """返回 Unix 时间戳（秒），解析失败返回 0"""
    try:
        ts = int(gid_str) >> 32
        if 1_600_000_000 < ts < 2_000_000_000:
            return ts
    except Exception:
        pass
    return 0

# ─────────────────── 胜负推导 ───────────────────

RESULT_MAP = {1: '胜', 2: '负', 3: '平局'}
_OPPOSITE  = {1: 2, 2: 1, 3: 3}


def propagate_results(results, teams):
    """
    通用胜负传播：同队填充 + 对手取反。
    teams: list of seat lists，如 [[0,1],[2,3]] (2v2) 或 [[0],[1,2]] (斗地主)
    results dict 原地修改。
    """
    # 同队传播
    for team in teams:
        known = next((results[s] for s in team if s in results), None)
        if known:
            for s in team:
                results.setdefault(s, known)

    # 对手取反
    for i in range(len(teams)):
        for j in range(i + 1, len(teams)):
            ti = next((results[s] for s in teams[i] if s in results), None)
            tj = next((results[s] for s in teams[j] if s in results), None)
            if ti and not tj:
                for s in teams[j]:
                    results.setdefault(s, _OPPOSITE[ti])
            elif tj and not ti:
                for s in teams[i]:
                    results.setdefault(s, _OPPOSITE[tj])
