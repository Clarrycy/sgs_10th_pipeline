#!/usr/bin/env python3
"""
backfill_csv_to_db.py — 将历史 CSV 解析数据回灌到 SQLite

用法：
    python pipeline/backfill_csv_to_db.py [--csv-dir DIR]

默认读取 data/output/ 下的 parsed_2v2.csv 和 parsed_doudizhu.csv，
写入 data/sgs.db 的 ranked_2v2 和 doudizhu 表（INSERT OR IGNORE 去重）。
"""

import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import get_conn, insert_ranked_2v2, insert_doudizhu

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV_DIR = ROOT / 'data' / 'output'

BATCH_SIZE = 5000


def strip_bom(fieldnames):
    """去掉 BOM 前缀"""
    if fieldnames and fieldnames[0].startswith('\ufeff'):
        fieldnames[0] = fieldnames[0].lstrip('\ufeff')
    return fieldnames


def extract_general_id(general_str):
    """从 '董卓(1601)' 提取 1601；从纯数字 '218' 返回 218"""
    if not general_str:
        return 0
    m = re.search(r'\((\d+)\)', general_str)
    if m:
        return int(m.group(1))
    try:
        return int(general_str)
    except ValueError:
        return 0


def backfill_2v2(conn, csv_path):
    """回灌 2v2 CSV 到 ranked_2v2 表"""
    if not csv_path.is_file():
        print(f'  ⚠️ 未找到: {csv_path}')
        return 0

    total = 0
    batch = []

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        strip_bom(reader.fieldnames)

        for row in reader:
            batch.append({
                'game_id':     row.get('GameID', ''),
                'game_time':   row.get('对局时间', ''),
                'seat':        int(row.get('座位', 0)),
                'player_name': row.get('玩家昵称', ''),
                'user_id':     row.get('UserID', ''),
                'rank_name':   row.get('官阶', ''),
                'general_id':  extract_general_id(row.get('选将', '')),
                'camp':        row.get('阵营', ''),
                'result':      row.get('胜负', ''),
                'candidates':  row.get('出框武将', ''),
                'rank_score':  int(row['官阶积分']) if row.get('官阶积分', '').strip() else None,
                'elo':         int(row['Elo']) if row.get('Elo', '').strip() else None,
            })

            if len(batch) >= BATCH_SIZE:
                insert_ranked_2v2(conn, batch)
                total += len(batch)
                batch = []
                print(f'\r  ranked_2v2: {total} 行...', end='', flush=True)

    if batch:
        insert_ranked_2v2(conn, batch)
        total += len(batch)

    conn.commit()
    print(f'\r  ranked_2v2: {total} 行 ✅')
    return total


def backfill_doudizhu(conn, csv_path):
    """回灌斗地主 CSV 到 doudizhu 表"""
    if not csv_path.is_file():
        print(f'  ⚠️ 未找到: {csv_path}')
        return 0

    total = 0
    batch = []

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        strip_bom(reader.fieldnames)

        for row in reader:
            batch.append({
                'game_id':     row.get('GameID', ''),
                'game_time':   row.get('对局时间', ''),
                'seat':        int(row.get('座位', 0)),
                'player_name': row.get('玩家昵称', ''),
                'user_id':     row.get('UserID', ''),
                'rank_name':   row.get('官阶', ''),
                'general_id':  extract_general_id(row.get('选将ID', '') or row.get('选将', '')),
                'camp':        row.get('阵营', ''),
                'result':      row.get('胜负', ''),
                'candidates':  row.get('初始出框', ''),
                'swapped_out': row.get('换走', ''),
                'swapped_in':  row.get('换入', ''),
                'rank_score':  None,  # CSV 中无此列
            })

            if len(batch) >= BATCH_SIZE:
                insert_doudizhu(conn, batch)
                total += len(batch)
                batch = []
                print(f'\r  doudizhu: {total} 行...', end='', flush=True)

    if batch:
        insert_doudizhu(conn, batch)
        total += len(batch)

    conn.commit()
    print(f'\r  doudizhu: {total} 行 ✅')
    return total


def main():
    csv_dir = DEFAULT_CSV_DIR
    for arg in sys.argv[1:]:
        if arg.startswith('--csv-dir='):
            csv_dir = Path(arg.split('=', 1)[1])

    print(f'📋 CSV 回灌 DB — 从 {csv_dir}')
    print()

    conn = get_conn()

    # 回灌前统计
    cur = conn.execute('SELECT COUNT(DISTINCT game_id) FROM ranked_2v2')
    before_2v2 = cur.fetchone()[0]
    cur = conn.execute('SELECT COUNT(DISTINCT game_id) FROM doudizhu')
    before_ddz = cur.fetchone()[0]
    print(f'  回灌前: ranked_2v2={before_2v2} 把, doudizhu={before_ddz} 把')
    print()

    # 兼容两种文件名：parsed_2v2.csv 和 output_parsed_2v2.csv
    f2v2 = csv_dir / 'parsed_2v2.csv'
    if not f2v2.is_file():
        f2v2 = csv_dir / 'output_parsed_2v2.csv'
    fddz = csv_dir / 'parsed_doudizhu.csv'
    if not fddz.is_file():
        fddz = csv_dir / 'output_parsed_doudizhu.csv'

    backfill_2v2(conn, f2v2)
    backfill_doudizhu(conn, fddz)

    print()

    # 回灌后统计
    cur = conn.execute('SELECT COUNT(DISTINCT game_id) FROM ranked_2v2')
    after_2v2 = cur.fetchone()[0]
    cur = conn.execute('SELECT COUNT(DISTINCT game_id) FROM doudizhu')
    after_ddz = cur.fetchone()[0]
    print(f'  回灌后: ranked_2v2={after_2v2} 把 (+{after_2v2 - before_2v2}), doudizhu={after_ddz} 把 (+{after_ddz - before_ddz})')
    print(f'  合计: {after_2v2 + after_ddz} 把')

    conn.close()
    print('\n✅ 回灌完成')


if __name__ == '__main__':
    main()
