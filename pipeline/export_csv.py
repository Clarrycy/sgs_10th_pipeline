#!/usr/bin/env python3
"""
export_csv.py — 从 sgs.db 导出 CSV（DB → CSV 单向同步）

每次 push 前执行，确保 CSV 始终是 DB 的完整镜像。

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
    '选将ID', '选将', '阵营', '胜负', '初始出框', '换走', '换入',
]


def export_ranked_2v2(conn):
    """导出 ranked_2v2 表到 parsed_2v2.csv"""
    out = OUTPUT_DIR / 'parsed_2v2.csv'

    cur = conn.execute("""
        SELECT
            r.game_id, r.game_time, r.seat, r.player_name, r.user_id,
            r.rank_name, r.general_id, r.camp, r.result, r.candidates,
            r.rank_score, r.elo,
            g.name AS general_name
        FROM ranked_2v2 r
        LEFT JOIN generals g ON r.general_id = g.general_id
        ORDER BY r.game_time DESC, r.game_id, r.seat
    """)

    count = 0
    with open(out, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
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

    games = conn.execute('SELECT COUNT(DISTINCT game_id) FROM ranked_2v2').fetchone()[0]
    print(f'  ✅ parsed_2v2.csv: {count} 行, {games} 把')
    return count


def export_doudizhu(conn):
    """导出 doudizhu 表到 parsed_doudizhu.csv"""
    out = OUTPUT_DIR / 'parsed_doudizhu.csv'

    cur = conn.execute("""
        SELECT
            d.game_id, d.game_time, d.seat, d.player_name, d.user_id,
            d.rank_name, d.general_id, d.camp, d.result, d.candidates,
            d.swapped_out, d.swapped_in, d.rank_score,
            g.name AS general_name
        FROM doudizhu d
        LEFT JOIN generals g ON d.general_id = g.general_id
        ORDER BY d.game_time DESC, d.game_id, d.seat
    """)

    count = 0
    with open(out, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(DOUDIZHU_COLS)
        for row in cur:
            gid, gtime, seat, pname, uid, rank, gen_id, camp, result, \
                cands, swp_out, swp_in, rscore, gen_name = row
            general_str = f'{gen_name}({gen_id})' if gen_name else str(gen_id or '')
            writer.writerow([
                gid, gtime, seat, pname, uid, rank or '',
                gen_id or '', general_str, camp, result,
                cands or '', swp_out or '', swp_in or '',
            ])
            count += 1

    games = conn.execute('SELECT COUNT(DISTINCT game_id) FROM doudizhu').fetchone()[0]
    print(f'  ✅ parsed_doudizhu.csv: {count} 行, {games} 把')
    return count


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print('📋 export_csv.py — DB → CSV 同步')

    conn = get_conn()
    conn.row_factory = None
    export_ranked_2v2(conn)
    export_doudizhu(conn)
    conn.close()

    print('✅ CSV 导出完成')


if __name__ == '__main__':
    main()
