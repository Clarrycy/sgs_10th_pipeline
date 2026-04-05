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
  replays/2v2/<GameID>.sgs      （--replays 时）
  replays/斗地主/<GameID>.sgs   （--replays 时）
"""

import os
import sys
import argparse
from pathlib import Path

ROOT       = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / 'data' / 'output'
REPLAY_DIR = ROOT / 'data' / 'replays'

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

    # 上传 data/output/
    output_files = list(OUTPUT_DIR.glob('*'))
    csv_files = [f for f in output_files if f.suffix in ('.csv',) and f.is_file()]
    print(f'📤 上传 data/output/ ({len(csv_files)} 个文件)...')
    for local in csv_files:
        key = f'output/{local.name}'
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

def pull():
    client, bucket = get_client()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print('📥 从 R2 拉取 output/...')
    paginator = client.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix='output/'):
        for obj in page.get('Contents', []):
            key  = obj['Key']
            name = Path(key).name
            if not name:
                continue
            local = OUTPUT_DIR / name
            print(f'  ↓ {key}', end=' ', flush=True)
            client.download_file(bucket, key, str(local))
            print('✓')
            count += 1

    if count == 0:
        print('  （R2 上没有找到 output/ 文件，首次运行正常）')
    else:
        print(f'✅ 拉取完成（{count} 个文件）')

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

# ─────────────────── 主流程 ───────────────────

def main():
    ap = argparse.ArgumentParser(description='Cloudflare R2 同步')
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument('--push', action='store_true', help='上传本地结果到 R2')
    group.add_argument('--pull', action='store_true', help='从 R2 拉取结果到本地')
    group.add_argument('--list', action='store_true', help='列出 R2 中的文件')
    ap.add_argument('--replays', action='store_true', help='--push 时同时上传 .sgs 录像（体积大）')
    args = ap.parse_args()

    if args.push:
        push(include_replays=args.replays)
    elif args.pull:
        pull()
    elif args.list:
        list_r2()


if __name__ == '__main__':
    main()
