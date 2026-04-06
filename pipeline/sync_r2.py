#!/usr/bin/env python3
"""
Cloudflare R2 同步脚本

凭证（绝对不要写入代码或提交到 Git）：
  export R2_ENDPOINT='https://<account_id>.r2.cloudflarestorage.com'
  export R2_BUCKET='your-bucket-name'
  export R2_ACCESS_KEY_ID='...'
  export R2_SECRET_ACCESS_KEY='...'

用法:
  python pipeline/sync_r2.py --push             # 上传 data/output/ 到 R2
  python pipeline/sync_r2.py --pull             # 从 R2 拉取 data/output/
  python pipeline/sync_r2.py --push --replays   # 同时上传 data/replays/（体积大，谨慎）
  python pipeline/sync_r2.py --list             # 列出 R2 中的文件

R2 对象结构:
  output/index.csv
  output/parsed_2v2.csv
  output/parsed_doudizhu.csv
  indexes/index_identity.json
  indexes/index_ranked.json
  indexes/index_doudizhu.json
  indexes/session_state.json
  cache/boards_YYYY-MM-DD.json
  gameids/YYYY-MM-DD_HHMM.json
  replays/2v2/<GameID>.sgs      （--replays 时）
  replays/斗地主/<GameID>.sgs   （--replays 时）
"""

import os
import sys
import argparse
from pathlib import Path

ROOT        = Path(__file__).resolve().parent.parent
OUTPUT_DIR  = ROOT / 'data' / 'output'
REPLAY_DIR  = ROOT / 'data' / 'replays'
INDEXES_DIR = ROOT / 'data' / 'indexes'
CACHE_DIR   = ROOT / 'data' / 'cache'
GAMEID_DIR  = ROOT / 'data' / 'gameids'

# ─────────────────── 凭证 ───────────────────

def get_client():
    endpoint  = os.environ.get('R2_ENDPOINT', '').strip()
    key_id    = os.environ.get('R2_ACCESS_KEY_ID', '').strip()
    secret    = os.environ.get('R2_SECRET_ACCESS_KEY', '').strip()
    bucket    = os.environ.get('R2_BUCKET', '').strip()

    missing = []
    if not endpoint: missing.append('R2_ENDPOINT')
    if not key_id:   missing.append('R2_ACCESS_KEY_ID')
    if not secret:   missing.append('R2_SECRET_ACCESS_KEY')
    if not bucket:   missing.append('R2_BUCKET')
    if missing:
        print(f'❌ 缺少环境变量：{", ".join(missing)}')
        sys.exit(1)

    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        print('❌ 缺少 boto3，请执行：pip install boto3')
        sys.exit(1)

    client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name='auto',
        config=Config(signature_version='s3v4'),
    )
    return client, bucket

# ─────────────────── 上传 ───────────────────

def push(include_replays=False):
    client, bucket = get_client()

    # 上传 data/output/ (CSV)
    output_files = list(OUTPUT_DIR.glob('*'))
    data_files = [f for f in output_files if f.suffix == '.csv' and f.is_file()]
    # sgs.db 统一从 data/sgs.db 上传（db.py 写入路径），避免与 data/output/sgs.db 冲突
    db_file = ROOT / 'data' / 'sgs.db'
    if db_file.is_file():
        data_files.append(db_file)
    print(f'📤 上传 data/output/ ({len(data_files)} 个文件)...')
    for local in data_files:
        key = f'output/{local.name}'
        _upload(client, bucket, local, key)

    # 上传 data/indexes/
    if INDEXES_DIR.is_dir():
        idx_files = [f for f in INDEXES_DIR.glob('*.json') if f.is_file()]
        if idx_files:
            print(f'📤 上传 data/indexes/ ({len(idx_files)} 个文件)...')
            for local in idx_files:
                key = f'indexes/{local.name}'
                _upload(client, bucket, local, key)

    # 上传 data/cache/
    if CACHE_DIR.is_dir():
        cache_files = [f for f in CACHE_DIR.glob('*.json') if f.is_file()]
        if cache_files:
            print(f'📤 上传 data/cache/ ({len(cache_files)} 个文件)...')
            for local in cache_files:
                key = f'cache/{local.name}'
                _upload(client, bucket, local, key)

    # 上传 data/gameids/（保留 batch 原始数据）
    if GAMEID_DIR.is_dir():
        gid_files = [f for f in GAMEID_DIR.glob('*.json') if f.is_file()]
        if gid_files:
            print(f'📤 上传 data/gameids/ ({len(gid_files)} 个文件)...')
            for local in gid_files:
                key = f'gameids/{local.name}'
                _upload(client, bucket, local, key)

    # 上传 data/replays/（可选）
    if include_replays:
        sgs_files = list(REPLAY_DIR.rglob('*.sgs'))
        print(f'📤 上传 data/replays/ ({len(sgs_files)} 个 .sgs)...')
        for local in sgs_files:
            rel = local.relative_to(REPLAY_DIR)
            key = f'replays/{rel}'
            _upload(client, bucket, local, key)

    print('✅ 上传完成')


def _upload(client, bucket, local, key):
    size_mb = local.stat().st_size / 1024 / 1024
    print(f'  ↑ {key} ({size_mb:.1f} MB)', end=' ', flush=True)
    client.upload_file(str(local), bucket, key)
    print('✓')

# ─────────────────── 下载 ───────────────────

def _pull_prefix(client, bucket, prefix, local_dir):
    """从 R2 拉取指定前缀的所有文件到本地目录。返回拉取文件数。"""
    local_dir.mkdir(parents=True, exist_ok=True)
    paginator = client.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key  = obj['Key']
            name = Path(key).name
            if not name:
                continue
            local = local_dir / name
            print(f'  ↓ {key}', end=' ', flush=True)
            client.download_file(bucket, key, str(local))
            print('✓')
            count += 1
    return count


def pull():
    client, bucket = get_client()

    total = 0
    for prefix, local_dir, label in [
        ('output/',  OUTPUT_DIR,  'output'),
        ('indexes/', INDEXES_DIR, 'indexes'),
        ('cache/',   CACHE_DIR,   'cache'),
        ('gameids/', GAMEID_DIR,  'gameids'),
    ]:
        print(f'📥 从 R2 拉取 {label}/...')
        count = _pull_prefix(client, bucket, prefix, local_dir)
        if count == 0:
            print(f'  （R2 上没有 {label}/ 文件）')
        total += count

    # output/sgs.db 下载到 data/output/sgs.db，但 db.py 实际使用 data/sgs.db
    # 把它复制到正确位置，确保 pipeline 每次都基于历史数据继续累积
    import shutil
    output_db = OUTPUT_DIR / 'sgs.db'
    actual_db = ROOT / 'data' / 'sgs.db'
    if output_db.is_file():
        shutil.copy2(str(output_db), str(actual_db))
        size_mb = output_db.stat().st_size / 1024 / 1024
        print(f'  📋 sgs.db 同步到 data/sgs.db ({size_mb:.1f} MB)')

    if total > 0:
        print(f'✅ 拉取完成（共 {total} 个文件）')
    else:
        print('  （首次运行，R2 上没有已有数据）')

# ─────────────────── 列表 ───────────────────

def list_r2():
    client, bucket = get_client()
    paginator = client.get_paginator('list_objects_v2')
    total_size = 0
    count = 0
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            size_mb = obj['Size'] / 1024 / 1024
            print(f'  {obj["Key"]}  ({size_mb:.2f} MB)  {obj["LastModified"].strftime("%Y-%m-%d %H:%M")}')
            total_size += obj['Size']
            count += 1
    print(f'\n共 {count} 个对象，总计 {total_size/1024/1024:.1f} MB')

# ─────────────────── 删除 ───────────────────

def delete_prefix(prefix):
    """删除 R2 中指定前缀下的所有对象。"""
    client, bucket = get_client()
    paginator = client.get_paginator('list_objects_v2')
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
            deleted += len(objects)
            for o in objects:
                print(f'  🗑️ {o["Key"]}')
    print(f'✅ 删除完成（{deleted} 个对象，前缀: {prefix}）')


# ─────────────────── 清理过期数据 ───────────────────

def cleanup_old_r2(days_gameids=30, days_cache=7):
    """清理 R2 上超过指定天数的 gameids 和 cache 文件。
    基于文件名中的日期判断（YYYY-MM-DD）。
    """
    import re
    from datetime import datetime, timedelta

    client, bucket = get_client()
    paginator = client.get_paginator('list_objects_v2')

    today = datetime.now()
    date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
    to_delete = []

    for prefix, max_days, label in [
        ('gameids/', days_gameids, 'batch 文件'),
        ('cache/',   days_cache,   '榜单缓存'),
    ]:
        count = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                m = date_pattern.search(key)
                if m:
                    try:
                        file_date = datetime.strptime(m.group(1), '%Y-%m-%d')
                        if (today - file_date).days > max_days:
                            to_delete.append(key)
                            count += 1
                    except ValueError:
                        pass
        if count:
            print(f'  {label}: {count} 个过期（>{max_days} 天）')

    if not to_delete:
        print('  ℹ️ 没有需要清理的过期数据')
        return

    # 批量删除（每次最多 1000 个）
    for i in range(0, len(to_delete), 1000):
        batch = [{'Key': k} for k in to_delete[i:i+1000]]
        client.delete_objects(Bucket=bucket, Delete={'Objects': batch})

    print(f'🗑️ 已清理 {len(to_delete)} 个过期 R2 对象')


def cleanup_old_local(days_gameids=30, days_cache=7):
    """清理本地超过指定天数的 gameids 和 cache 文件。"""
    import re
    from datetime import datetime

    today = datetime.now()
    date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
    deleted = 0

    for local_dir, max_days, label in [
        (GAMEID_DIR, days_gameids, 'batch 文件'),
        (CACHE_DIR,  days_cache,   '榜单缓存'),
    ]:
        if not local_dir.is_dir():
            continue
        for f in local_dir.glob('*.json'):
            m = date_pattern.search(f.name)
            if m:
                try:
                    file_date = datetime.strptime(m.group(1), '%Y-%m-%d')
                    if (today - file_date).days > max_days:
                        f.unlink()
                        deleted += 1
                except ValueError:
                    pass

    if deleted:
        print(f'🗑️ 本地清理 {deleted} 个过期文件')
    else:
        print('  ℹ️ 本地没有过期文件')


# ─────────────────── 主流程 ───────────────────

def main():
    ap = argparse.ArgumentParser(description='Cloudflare R2 同步')
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument('--push', action='store_true', help='上传本地结果到 R2')
    group.add_argument('--pull', action='store_true', help='从 R2 拉取结果到本地')
    group.add_argument('--list', action='store_true', help='列出 R2 中的文件')
    group.add_argument('--delete-prefix', type=str, metavar='PREFIX',
                       help='删除 R2 中指定前缀下的所有对象')
    group.add_argument('--cleanup', action='store_true',
                       help='清理 R2 和本地的过期 gameids (>30天) 和 cache (>7天)')
    ap.add_argument('--replays', action='store_true', help='--push 时同时上传 .sgs 录像（体积大）')
    args = ap.parse_args()

    if args.push:
        push(include_replays=args.replays)
    elif args.pull:
        pull()
    elif args.list:
        list_r2()
    elif args.delete_prefix:
        delete_prefix(args.delete_prefix)
    elif args.cleanup:
        print('🧹 清理过期数据...')
        cleanup_old_r2(days_gameids=30, days_cache=7)
        cleanup_old_local(days_gameids=30, days_cache=7)


if __name__ == '__main__':
    main()
