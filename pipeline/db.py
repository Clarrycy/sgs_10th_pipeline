#!/usr/bin/env python3
"""
sgs-pipeline SQLite 数据库层

单文件数据库 data/sgs.db，提供：
  - 建表 / 迁移
  - 武将映射表刷新（generals_mapping.csv → generals 表）
  - 各表的批量 INSERT（去重）
  - 常用查询快捷方法

表结构：
  generals     — 武将 ID↔名称（可随版本更新全量刷新）
  war_records  — collect.js 采集的战绩元数据（含 MVP / 阵营 / 逃跑）
  ranked_2v2   — 2v2 录像解析（每行 = 1 玩家 × 1 局）
  doudizhu     — 斗地主录像解析
"""

import csv
import sqlite3
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'data' / 'sgs.db'
MAPPING_CSV = ROOT / 'data' / 'generals_mapping.csv'

# ─────────────────── 建表 DDL ───────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS generals (
    general_id  INTEGER PRIMARY KEY,
    name        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS war_records (
    game_id       TEXT    NOT NULL,
    user_id       TEXT    NOT NULL,
    mode_id       INTEGER NOT NULL,
    game_time     INTEGER,
    result        TEXT,
    is_mvp        INTEGER DEFAULT 0,
    is_escape     INTEGER DEFAULT 0,
    figure        INTEGER,
    general_id    INTEGER,
    score_change  INTEGER DEFAULT 0,
    batch_id      TEXT,
    PRIMARY KEY (game_id, user_id)
);

CREATE TABLE IF NOT EXISTS ranked_2v2 (
    game_id     TEXT    NOT NULL,
    game_time   TEXT,
    seat        INTEGER NOT NULL,
    player_name TEXT,
    user_id     TEXT,
    rank_name   TEXT,
    general_id  INTEGER,
    camp        TEXT,
    result      TEXT,
    candidates  TEXT,
    rank_score  INTEGER,
    elo         INTEGER,
    PRIMARY KEY (game_id, seat)
);

CREATE TABLE IF NOT EXISTS doudizhu (
    game_id     TEXT    NOT NULL,
    game_time   TEXT,
    seat        INTEGER NOT NULL,
    player_name TEXT,
    user_id     TEXT,
    rank_name   TEXT,
    general_id  INTEGER,
    camp        TEXT,
    result      TEXT,
    candidates  TEXT,
    swapped_out TEXT,
    swapped_in  TEXT,
    rank_score  INTEGER,
    PRIMARY KEY (game_id, seat)
);

CREATE INDEX IF NOT EXISTS idx_2v2_user    ON ranked_2v2 (user_id);
CREATE INDEX IF NOT EXISTS idx_2v2_general ON ranked_2v2 (general_id);
CREATE INDEX IF NOT EXISTS idx_2v2_time    ON ranked_2v2 (game_time);
CREATE INDEX IF NOT EXISTS idx_ddz_user    ON doudizhu   (user_id);
CREATE INDEX IF NOT EXISTS idx_ddz_general ON doudizhu   (general_id);
CREATE INDEX IF NOT EXISTS idx_ddz_time    ON doudizhu   (game_time);
CREATE INDEX IF NOT EXISTS idx_war_user    ON war_records (user_id);
CREATE INDEX IF NOT EXISTS idx_war_mode    ON war_records (mode_id);
"""

# ─────────────────── 连接 ───────────────────

def get_conn():
    """获取 SQLite 连接（自动建表 + 迁移）。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    _migrate(conn)
    return conn


def _migrate(conn):
    """增量迁移：为已有表补充新列。"""
    # doudizhu.rank_score — 斗地主积分 (header f15.f1, 2026-04-06 新增)
    try:
        conn.execute("SELECT rank_score FROM doudizhu LIMIT 0")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE doudizhu ADD COLUMN rank_score INTEGER")
        conn.commit()


# ─────────────────── 武将映射 ───────────────────

def refresh_generals(conn=None, path=None):
    """从 generals_mapping.csv 全量刷新 generals 表。"""
    path = Path(path) if path else MAPPING_CSV
    if not path.is_file():
        print(f'⚠️  未找到武将映射表：{path}')
        return 0

    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    rows = []
    with open(path, 'r', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            try:
                rows.append((int(row['GeneralID']), row['GeneralName']))
            except (ValueError, KeyError):
                continue

    conn.executemany(
        "INSERT OR REPLACE INTO generals (general_id, name) VALUES (?, ?)",
        rows,
    )
    conn.commit()
    print(f'📖 武将映射表刷新：{len(rows)} 条')

    if own_conn:
        conn.close()
    return len(rows)


# ─────────────────── 批量写入 ───────────────────

def insert_ranked_2v2(conn, rows):
    """批量插入 2v2 解析结果，跳过已存在的 (game_id, seat)。"""
    conn.executemany(
        """INSERT OR IGNORE INTO ranked_2v2
           (game_id, game_time, seat, player_name, user_id, rank_name,
            general_id, camp, result, candidates, rank_score, elo)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        [(r['game_id'], r['game_time'], r['seat'], r['player_name'],
          r['user_id'], r['rank_name'], r['general_id'], r['camp'],
          r['result'], r['candidates'], r['rank_score'], r['elo'])
         for r in rows],
    )


def insert_doudizhu(conn, rows):
    """批量插入斗地主解析结果，跳过已存在的 (game_id, seat)。"""
    conn.executemany(
        """INSERT OR IGNORE INTO doudizhu
           (game_id, game_time, seat, player_name, user_id, rank_name,
            general_id, camp, result, candidates, swapped_out, swapped_in,
            rank_score)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [(r['game_id'], r['game_time'], r['seat'], r['player_name'],
          r['user_id'], r['rank_name'], r['general_id'], r['camp'],
          r['result'], r['candidates'], r['swapped_out'], r['swapped_in'],
          r.get('rank_score'))
         for r in rows],
    )


def insert_war_records(conn, rows):
    """批量插入战绩采集数据，跳过已存在的 (game_id, user_id)。"""
    conn.executemany(
        """INSERT OR IGNORE INTO war_records
           (game_id, user_id, mode_id, game_time, result,
            is_mvp, is_escape, figure, general_id, score_change, batch_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        [(r['game_id'], r['user_id'], r['mode_id'], r['game_time'],
          r['result'], r['is_mvp'], r['is_escape'], r['figure'],
          r['general_id'], r['score_change'], r['batch_id'])
         for r in rows],
    )


# ─────────────────── 查询 ───────────────────

def query(sql, params=(), conn=None):
    """执行查询，返回 list of dict。"""
    own_conn = conn is None
    if own_conn:
        conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    results = [dict(row) for row in cur.fetchall()]
    if own_conn:
        conn.close()
    return results


def existing_game_ids(conn, table):
    """返回指定表中已有的 game_id 集合。"""
    cur = conn.execute(f"SELECT DISTINCT game_id FROM {table}")
    return {row[0] for row in cur.fetchall()}
