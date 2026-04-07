#!/usr/bin/env python3
"""
merge_indexes.py — 将 collect.js 输出的 batch 文件按 mode 分流到三个独立索引，
                   并通过 union-find 检测跨 batch 的连续对局（session）。

用法：
    python pipeline/merge_indexes.py [--quiet]

数据流：
    data/gameids/*.json  →  data/indexes/index_identity.json   (mode 4, 不下载)
                         →  data/indexes/index_ranked.json     (mode 8)
                         →  data/indexes/index_doudizhu.json   (mode 36)
                         →  data/indexes/session_state.json    (连续对局状态)
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import get_conn, insert_war_records

ROOT = Path(__file__).resolve().parent.parent
GAMEIDS_DIR = ROOT / 'data' / 'gameids'
INDEXES_DIR = ROOT / 'data' / 'indexes'

# 只保留最近 N 天的对局（按 GameID 时间戳）
MERGE_DAYS = 7

MODE_CONFIG = {
    4:  {'name': 'identity',  'modeName': '身份竞技',   'file': 'index_identity.json'},
    8:  {'name': 'ranked',    'modeName': 'ranked_2v2', 'file': 'index_ranked.json'},
    36: {'name': 'doudizhu',  'modeName': '斗地主',     'file': 'index_doudizhu.json'},
}

SESSION_STATE_FILE = INDEXES_DIR / 'session_state.json'
QUIET = '--quiet' in sys.argv


def log(msg):
    if not QUIET:
        print(msg)


# ─── 工具函数 ──────────────────────────────────────────────────

def gameid_to_time(gid_str):
    """GameID 高 32 位为 Unix 时间戳"""
    import time
    try:
        ts = int(gid_str) >> 32
        if 1_600_000_000 < ts < 2_000_000_000:
            return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
    except (ValueError, OSError):
        pass
    return ''


# ─── Index I/O ─────────────────────────────────────────────────

def load_index(mode):
    """加载 per-mode index JSON，不存在则返回空结构。"""
    cfg = MODE_CONFIG[mode]
    fp = INDEXES_DIR / cfg['file']
    if fp.exists():
        with open(fp, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        'mode': mode,
        'modeName': cfg['modeName'],
        'lastBatchId': None,
        'games': {},
    }


def save_index(mode, data):
    """保存 per-mode index JSON。"""
    cfg = MODE_CONFIG[mode]
    fp = INDEXES_DIR / cfg['file']
    INDEXES_DIR.mkdir(parents=True, exist_ok=True)
    with open(fp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_session_state():
    if SESSION_STATE_FILE.exists():
        with open(SESSION_STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        'lastBatchId': None,
        'processedBatches': [],
        'perMode': {},
    }


def save_session_state(state):
    INDEXES_DIR.mkdir(parents=True, exist_ok=True)
    with open(SESSION_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ─── Batch 加载 ───────────────────────────────────────────────

def is_new_format(batch):
    """检查 batch 是否为新格式（gameIds 为 {gameId, modeId} 对象数组）。"""
    results = batch.get('results', [])
    for r in results:
        game_ids = r.get('gameIds', [])
        if game_ids:
            first = game_ids[0]
            return isinstance(first, dict) and 'gameId' in first
    return False  # 空结果也跳过


def load_batch_files(processed_set):
    """加载所有新格式 batch 文件，跳过已处理的和旧格式的。按文件名排序。"""
    if not GAMEIDS_DIR.exists():
        return []

    batches = []
    for fp in sorted(GAMEIDS_DIR.glob('*.json')):
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log(f'  ⚠️ 跳过损坏文件: {fp.name} ({e})')
            continue

        batch_id = data.get('metadata', {}).get('batchId')
        if not batch_id:
            # 从文件名推断 batchId: 2026-04-05_1800.json → 2026-04-05_1800
            batch_id = fp.stem

        if batch_id in processed_set:
            continue

        if not is_new_format(data):
            log(f'  ⏭️ 跳过旧格式: {fp.name}')
            continue

        data['_batchId'] = batch_id
        data['_filename'] = fp.name
        batches.append(data)

    return batches


# ─── Merge ─────────────────────────────────────────────────────

def merge_batch(batch, indexes):
    """将一个 batch 中的 games 按 modeId 分发到对应 index。
    返回 (per_mode_user_games, war_rows)
      per_mode_user_games: {mode: {userId: [gameId, ...]}}
      war_rows: list of war_record dicts for SQLite
    """
    batch_id = batch['_batchId']
    per_mode_user_games = {}
    war_rows = []

    for entry in batch.get('results', []):
        user_id = str(entry.get('userId', ''))
        if not user_id:
            continue

        for game_obj in entry.get('gameIds', []):
            if not isinstance(game_obj, dict):
                continue
            game_id = str(game_obj.get('gameId', ''))
            mode_id = game_obj.get('modeId', 0)

            if not game_id or game_id == '0' or mode_id not in MODE_CONFIG:
                continue

            # 7 天以外的对局跳过：退坑玩家的历史战绩不入库
            try:
                game_ts = int(game_id) >> 32
                if game_ts < time.time() - MERGE_DAYS * 86400:
                    continue
            except (ValueError, OverflowError):
                pass

            idx = indexes[mode_id]

            if game_id in idx['games']:
                existing = idx['games'][game_id]
                if user_id not in existing['collectedFrom']:
                    existing['collectedFrom'].append(user_id)
            else:
                idx['games'][game_id] = {
                    'gameId': game_id,
                    'modeId': mode_id,
                    'gameTime': gameid_to_time(game_id),
                    'collectedFrom': [user_id],
                    'batchId': batch_id,
                    'sessionId': None,
                    'replayDownloaded': False,
                    'parsed': None,
                }

            # 战绩元数据 → war_records（新格式 batch 才有这些字段）
            war_rows.append({
                'game_id':      game_id,
                'user_id':      user_id,
                'mode_id':      mode_id,
                'game_time':    game_obj.get('gameTime', 0),
                'result':       game_obj.get('result', ''),
                'is_mvp':       1 if game_obj.get('isMvp') else 0,
                'is_escape':    1 if game_obj.get('isEscape') else 0,
                'figure':       game_obj.get('figure', 0),
                'general_id':   game_obj.get('generalId', 0),
                'score_change': game_obj.get('scoreChange', 0),
                'batch_id':     batch_id,
            })

            per_mode_user_games.setdefault(mode_id, {}).setdefault(user_id, []).append(game_id)

    # 更新每个 mode index 的 lastBatchId
    for mode in MODE_CONFIG:
        if indexes[mode]['games']:
            indexes[mode]['lastBatchId'] = batch_id

    return per_mode_user_games, war_rows


# ─── Union-Find ────────────────────────────────────────────────

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1

    def components(self, keys):
        """返回 {root: [members]}"""
        groups = {}
        for k in keys:
            r = self.find(k)
            groups.setdefault(r, []).append(k)
        return groups


# ─── Session 检测 ──────────────────────────────────────────────

def detect_sessions(mode, index, prev_state, per_mode_user_games, batch_id):
    """
    检测连续对局。仅靠跨 batch GameID 重叠合并。

    返回更新后的 mode state (gameIds + sessionCounter)。
    """
    mode_key = str(mode)
    prev_mode_state = prev_state.get('perMode', {}).get(mode_key, {})
    prev_game_ids = set(prev_mode_state.get('gameIds', []))
    counter = prev_mode_state.get('sessionCounter', 0)

    # 当前 batch 中该 mode 的所有 gameId
    curr_user_games = per_mode_user_games.get(mode, {})
    curr_game_ids = set()
    for gids in curr_user_games.values():
        curr_game_ids.update(gids)

    if not curr_game_ids:
        return {'gameIds': [], 'sessionCounter': counter}

    # 找跨 batch 重叠
    overlap = prev_game_ids & curr_game_ids

    # 构建 union-find
    uf = UnionFind()

    # 确保所有 curr game 都注册到 UF 中
    for gid in curr_game_ids:
        uf.find(gid)

    # 跨 batch 重叠合并：overlap 中的 game 作为桥梁
    # 如果一个 overlap game 在 prev 中有 sessionId，它会把 prev session 中的其他
    # overlap game 联系起来。但我们只需要把 overlap game 与 curr 中同用户的其他 game 连接。
    # 注意：我们不做同 userId 自动 union，仅通过 overlap game 桥接。

    # 对于 overlap 中的每个 gameId，找到在 curr batch 中哪些 userId 贡献了它
    # 以及这些 userId 还贡献了哪些其他 overlap game → union
    if overlap:
        # 收集 overlap game → 在 curr 中出现的 userId 列表
        overlap_game_to_users = {}
        for uid, gids in curr_user_games.items():
            for gid in gids:
                if gid in overlap:
                    overlap_game_to_users.setdefault(gid, []).append(uid)

        # 所有 overlap games 之间，如果它们在 prev batch 中有相同的 sessionId → union
        overlap_by_prev_session = {}
        for gid in overlap:
            prev_entry = index['games'].get(gid)
            if prev_entry and prev_entry.get('sessionId'):
                sid = prev_entry['sessionId']
                overlap_by_prev_session.setdefault(sid, []).append(gid)

        # union 同一 prev session 下的 overlap games
        for sid, gids in overlap_by_prev_session.items():
            for i in range(1, len(gids)):
                uf.union(gids[0], gids[i])

        # 对于每个 overlap game，与同 userId 在 curr batch 中的其他 overlap game union
        # （只 union overlap 之间，不扩展到非 overlap games）
        for uid, gids in curr_user_games.items():
            user_overlap = [g for g in gids if g in overlap]
            for i in range(1, len(user_overlap)):
                uf.union(user_overlap[0], user_overlap[i])

        # 最后，将 curr batch 中与 overlap game 同用户的 curr-only game
        # 也 union 到 overlap game（通过 overlap game 桥接）
        for uid, gids in curr_user_games.items():
            user_overlap = [g for g in gids if g in overlap]
            user_curr_only = [g for g in gids if g not in overlap]
            if user_overlap and user_curr_only:
                # 这些 curr-only games 通过 overlap game 桥接到 prev session
                anchor = user_overlap[0]
                for g in user_curr_only:
                    uf.union(anchor, g)

    # 分配 sessionId
    components = uf.components(list(curr_game_ids))

    # 对每个 component，检查是否有已有 sessionId 可复用
    for root, members in components.items():
        existing_sid = None
        for gid in members:
            entry = index['games'].get(gid)
            if entry and entry.get('sessionId'):
                existing_sid = entry['sessionId']
                break

        if existing_sid is None:
            counter += 1
            existing_sid = f's_{mode}_{counter}'

        # 回写 sessionId 到 index
        for gid in members:
            if gid in index['games']:
                index['games'][gid]['sessionId'] = existing_sid

    return {'gameIds': sorted(curr_game_ids), 'sessionCounter': counter}


# ─── Main ──────────────────────────────────────────────────────

def main():
    log('📋 merge_indexes.py — 索引合并 + 连续对局检测')

    # 加载状态
    state = load_session_state()
    processed_set = set(state.get('processedBatches', []))

    # 加载 batch 文件
    batches = load_batch_files(processed_set)
    if not batches:
        log('  ℹ️ 没有新的 batch 文件需要处理')
        return

    log(f'  📦 发现 {len(batches)} 个新 batch 文件')

    # 加载现有 indexes
    indexes = {mode: load_index(mode) for mode in MODE_CONFIG}

    # SQLite: 收集所有 war_record 行
    all_war_rows = []

    # 按时间顺序处理每个 batch
    for batch in batches:
        batch_id = batch['_batchId']
        log(f'\n  ▶ 处理 batch: {batch["_filename"]} (batchId={batch_id})')

        # Merge：分流到 indexes + 收集 war_records
        per_mode_user_games, war_rows = merge_batch(batch, indexes)
        all_war_rows.extend(war_rows)

        # Session 检测
        new_per_mode = {}
        for mode in MODE_CONFIG:
            new_mode_state = detect_sessions(
                mode, indexes[mode], state, per_mode_user_games, batch_id,
            )
            new_per_mode[str(mode)] = new_mode_state

            mode_games = per_mode_user_games.get(mode, {})
            game_count = sum(len(v) for v in mode_games.values())
            if game_count > 0:
                log(f'    mode {mode} ({MODE_CONFIG[mode]["modeName"]}): '
                    f'{game_count} games, session counter={new_mode_state["sessionCounter"]}')

        # 更新状态
        state['lastBatchId'] = batch_id
        state['processedBatches'].append(batch_id)
        state['perMode'] = new_per_mode

    # 保存 JSON indexes
    for mode in MODE_CONFIG:
        save_index(mode, indexes[mode])
        total = len(indexes[mode]['games'])
        log(f'\n  💾 {MODE_CONFIG[mode]["file"]}: {total} games')

    save_session_state(state)
    log(f'\n  💾 session_state.json: {len(state["processedBatches"])} batches processed')

    # 保存 war_records 到 SQLite
    if all_war_rows:
        conn = get_conn()
        insert_war_records(conn, all_war_rows)
        conn.commit()
        conn.close()
        log(f'\n  💾 sgs.db: {len(all_war_rows)} 条战绩写入 war_records')

    log('\n✅ merge_indexes 完成')


if __name__ == '__main__':
    main()
