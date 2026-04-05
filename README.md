# sgs-pipeline

三国杀十周年录像批量采集 & 解析流水线。

## 目录结构

```
sgs-pipeline/
│
├── collect/                   # Step 1：浏览器端 JS 脚本（手动运行）
│   ├── 1_leaderboard.js       # 排行榜采集 → 导出 UserID 列表
│   └── 2_gameids.js           # 按 UserID 查对局记录 → 导出 GameID JSON
│
├── pipeline/                  # Step 2–4：Python 流水线
│   ├── common.py              # 共享解析库（.sgs 二进制解析，无第三方依赖）
│   ├── download.py            # 批量下载 .sgs 录像，按 mode 分类
│   ├── parse_2v2.py           # 解析 2v2 排位对局 → CSV
│   ├── parse_doudizhu.py      # 解析斗地主对局 → CSV
│   └── sync_r2.py             # 同步结果到 Cloudflare R2
│
├── data/
│   ├── generals_mapping.csv   # 武将 ID → 名称映射（静态数据）
│   ├── gameids/               # 输入：从 collect/ 脚本导出的 GameID JSON
│   ├── replays/               # 下载的 .sgs 录像（.gitignore，本地保留）
│   │   ├── 2v2/
│   │   ├── 斗地主/
│   │   └── other/
│   └── output/                # 解析结果
│       ├── index.csv          # 下载索引（去重 + 状态跟踪）
│       ├── parsed_2v2.csv     # 2v2 解析结果
│       └── parsed_doudizhu.csv
│
├── .github/workflows/
│   └── daily.yml              # GitHub Actions 定时任务
│
├── requirements.txt
└── .gitignore
```

## 快速开始

### 1. 安装依赖

```bash
npm install
pip install -r requirements.txt
```

### 2. 首次配置 Cookie（只需做一次）

```bash
node collect/save_cookies.js
# 弹出浏览器 → 手动登录游戏 → 回车
# 把终端输出的 JSON 粘贴到 GitHub Secret: GAME_COOKIES
```

### 3. 采集 GameID（全自动）

```bash
GAME_COOKIES='[...]' node collect/collect.js
# 自动输出到 data/gameids/YYYY-MM-DD.json
```

### 5. 下载录像

```bash
python pipeline/download.py --days=7 --modes=8,36
```

### 4. 解析

```bash
python pipeline/parse_2v2.py
python pipeline/parse_doudizhu.py
```

解析结果输出到 `data/output/parsed_*.csv`。

### 5. 同步到 Cloudflare R2（可选）

```bash
export R2_ENDPOINT='https://<account_id>.r2.cloudflarestorage.com'
export R2_BUCKET='your-bucket-name'
export R2_ACCESS_KEY_ID='...'
export R2_SECRET_ACCESS_KEY='...'

python pipeline/sync_r2.py --push
```

## GitHub Actions 自动化

在仓库 Settings → Secrets 中添加以下 secrets：

| Secret | 说明 |
|--------|------|
| `R2_ENDPOINT` | R2 S3 兼容端点 |
| `R2_BUCKET` | 桶名 |
| `R2_ACCESS_KEY_ID` | 访问密钥 ID |
| `R2_SECRET_ACCESS_KEY` | 访问密钥 |

配置完成后，每天 UTC 18:00（北京时间次日 02:00）自动运行：
下载新录像 → 解析 → 上传 CSV 到 R2。

也可在 Actions 页面手动触发（workflow_dispatch）。

## 支持的模式

| mode_id | 模式 | 解析脚本 |
|---------|------|---------|
| 8 | 2v2 欢乐竞技 | `parse_2v2.py` |
| 36 | 斗地主 | `parse_doudizhu.py` |
| 4 | 八人身份匹配 | 待实现 |

新增模式只需参考 `parse_2v2.py` 新建同结构脚本，无需改动其他文件。

## 去重机制

- `data/output/index.csv` 记录所有已下载的 GameID
- `download.py` 启动时加载 index.csv，已有的 GameID 跳过下载
- 各 parse 脚本读取已有 CSV 中的 GameID，重复的跳过解析
- GitHub Actions 每次运行前先从 R2 拉取最新 index.csv

## 录像保留策略

游戏服务器只保留 **7 天或最近 30 条**（取较早者），务必在此期限内下载。

本地 `.sgs` 文件建议定期清理，30 天以上的可删除：

```bash
python pipeline/download.py --cleanup --days=30
```

如需长期保存特定录像（用于回放），可手动上传到 R2：

```bash
python pipeline/sync_r2.py --push --replays
```
