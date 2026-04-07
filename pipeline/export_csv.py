#!/usr/bin/env python3
"""
export_csv.py — 从 sgs.db 导出 CSV（DB → CSV 增量 append）

每次 push 前执行，将 DB 中比 CSV 更新的记录追加到 CSV 末尾。
若 CSV 不存在则全量写入（含表头）。

用法：
    python pipeline/export_csv.py
"""

import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import get_conn

ROOT       = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / 'data' / 'output'

# CSV 列名（带 BOM 兼容 Excel）
RANKED_2V2_COLS = [
    'GameID', '对局时间', '座位', '玩家昵称', 'UserID', '官阶',
    '选将', '阵营', '胜负', '出框武将', '官阶积分', 'Elo',
]

DOUDIZHU_COLS = [
    'GameID', '对局时间', '座位', '玩家昵称', 'UserID', '官阶',
    '选将', '阵营', '胜负', '初始出框', '换走', '换入', '斗地主积分',
]


def _get_csv_latest_game_time(csv_path):
    """读取 CSV 末尾，返回已有数据中最新的 game_time（ISO 字符串），不存在则 None。
    CSV 按 game_time ASC 排列，所以最后一行是最新记录。
    """
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return None
    with open(csv_path, 'rb') as f:
        # 向前读 8 KB 足以覆盖最后几行
        try:
            f.seek(-8192, 2)
        except OSError:
            f.seek(0)
        tail = f.read().decode('utf-8-sig', errors='replace')
    lines = [l for l in tail.splitlines() if l.strip()]
    # 从末尾找第一行非表头数据行
    for line in reversed(lines):
        row = next(csv.reader([line]))
        if row and row[0] != 'GameID' and len(row) > 1:
            return row[1].strip()  # game_time 在第 2 列
    return None


def export_ranked_2v2(conn):
    """增量导出 ranked_2v2 表：仅追加 CSV 中尚不存在的记录。"""
    out = OUTPUT_DIR / 'parsed_2v2.csv'
    latest = _get_csv_latest_game_time(out)

    if latest:
        cur = conn.execute("""
            SELECT
                r.game_id, r.game_time, r.seat, r.player_name, r.user_id,
                r.rank_name, r.general_id, r.camp, r.result, r.candidates,
                r.rank_score, r.elo,
                g.name AS general_name
            FROM ranked_2v2 r
            LEFT JOIN generals g ON r.general_id = g.general_id
            WHERE r.game_time > ?
            ORDER BY r.game_time ASC, r.game_id, r.seat
        """, (latest,))
        mode = 'a'
    else:
        cur = conn.execute("""
            SELECT
                r.game_id, r.game_time, r.seat, r.player_name, r.user_id,
                r.rank_name, r.general_id, r.camp, r.result, r.candidates,
                r.rank_score, r.elo,
                g.name AS general_name
            FROM ranked_2v2 r
            LEFT JOIN generals g ON r.general_id = g.general_id
            ORDER BY r.game_time ASC, r.game_id, r.seat
        """)
        mode = 'w'

    count = 0
    with open(out, mode, encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        if mode == 'w':
            writer.writerow(RANKED_2V2_COLS)
        for row in cur:
            gid, gtime, seat, pname, uid, rank, gen_id, camp, result, \
                cands, rscore, elo, gen_name = row
            general_str = f'{gen_name}({gen_id})' if gen_name else str(gen_id or '')
            writer.writerow([
                gid, gtime, seat, pname, uid, rank or '',
                general_str, camp, result, cands or '',
                rscore if rscore is not None else '',
                elo if elo is not None else '',
            ])
            count += 1

    total_games = conn.execute('SELECT COUNT(DISTINCT game_id) FROM ranked_2v2').fetchone()[0]
    action = '追加' if mode == 'a' else '全量写入'
    print(f'  ✅ parsed_2v2.csv: {action} {count} 行（DB 共 {total_games} 把）')
    return count


def export_doudizhu(conn):
    """增量导出 doudizhu 表：仅追加 CSV 中尚不存在的记录。"""
    out = OUTPUT_DIR / 'parsed_doudizhu.csv'
    latest = _get_csv_latest_game_time(out)

    if latest:
        cur = conn.execute("""
            SELECT
                d.game_id, d.game_time, d.seat, d.player_name, d.user_id,
                d.rank_name, d.general_id, d.camp, d.result, d.candidates,
                d.swapped_out, d.swapped_in, d.rank_score,
                g.name AS general_name
            FROM doudizhu d
            LEFT JOIN generals g ON d.general_id = g.general_id
            WHERE d.game_time > ?
            ORDER BY d.game_time ASC, d.game_id, d.seat
        """, (latest,))
        mode = 'a'
    else:
        cur = conn.execute("""
            SELECT
                d.game_id, d.game_time, d.seat, d.player_name, d.user_id,
                d.rank_name, d.general_id, d.camp, d.result, d.candidates,
                d.swapped_out, d.swapped_in, d.rank_score,
                g.name AS general_name
            FROM doudizhu d
            LEFT JOIN generals g ON d.general_id = g.general_id
            ORDER BY d.game_time ASC, d.game_id, d.seat
        """)
        mode = 'w'

    count = 0
    with open(out, mode, encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        if mode == 'w':
            writer.writerow(DOUDIZHU_COLS)
        for row in cur:
            gid, gtime, seat, pname, uid, rank, gen_id, camp, result, \
                cands, swp_out, swp_in, rscore, gen_name = row
            general_str = f'{gen_name}({gen_id})' if gen_name else str(gen_id or '')
            writer.writerow([
                gid, gtime, seat, pname, uid, rank or '',
                general_str, camp, result,
                cands or '', swp_out or '', swp_in or '',
                rscore if rscore is not None else '',
            ])
            count += 1

    total_games = conn.execute('SELECT COUNT(DISTINCT game_id) FROM doudizhu').fetchone()[0]
    action = '追加' if mode == 'a' else '全量写入'
    print(f'  ✅ parsed_doudizhu.csv: {action} {count} 行（DB 共 {total_games} 把）')
    return count


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print('📋 export_csv.py — DB → CSV 增量 append')

    conn = get_conn()
    conn.row_factory = None
    export_ranked_2v2(conn)
    export_doudizhu(conn)
    conn.close()

    print('✅ CSV 导出完成')


if __name__ == '__main__':
    main()
