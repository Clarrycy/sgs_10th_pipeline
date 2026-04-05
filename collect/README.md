# collect/ — 全自动采集脚本

## 文件说明

| 文件 | 作用 |
|------|------|
| `collect.js` | 主脚本：无头浏览器自动采集排行榜 UserID → 查询 GameID → 输出 JSON |
| `save_cookies.js` | 一次性辅助：弹出浏览器供手动登录，导出 Cookie 供 CI 使用 |

## 首次配置（只需做一次）

```bash
# 1. 安装依赖
npm install

# 2. 弹出浏览器，手动登录游戏
node collect/save_cookies.js

# 3. 把终端输出的 JSON 粘贴到 GitHub → Settings → Secrets → GAME_COOKIES
```

Cookie 通常有效期数周到数月。失效时重新运行 `save_cookies.js`，更新 Secret 即可。

## 本地手动运行

```bash
GAME_COOKIES='[...]' node collect/collect.js
```

## 工作原理

1. 加载 Cookie，跳过手动登录
2. 拦截游戏 `console.log`（游戏自动打印所有收到的 protobuf 消息）
3. 捕获 `ProtoSocketClient`（PSC），用于主动发送请求
4. 向所有省份发送 `CReqRankList` → 收集 `CRespRankList` 中的 UserID
5. 逐个发送 `CReqGetNewGameRecord` → 过滤目标模式 → 收集 GameID
6. 输出到 `data/gameids/YYYY-MM-DD.json`
