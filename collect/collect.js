#!/usr/bin/env node
'use strict';

/**
 * SGS 全自动采集脚本  collect/collect.js
 *
 * 流程：
 *   1. 无头浏览器加载游戏（用已保存的 Cookie 免登录）
 *   2. 向服务器主动发送排行榜请求（所有省份），收集 UserID
 *   3. 逐个查询每个 UserID 的对局记录，提取 GameID
 *   4. 按模式过滤，输出到 data/gameids/YYYY-MM-DD.json
 *
 * 环境变量：
 *   GAME_COOKIES   游戏 Cookie JSON（见 save_cookies.js 如何导出）
 *   KEEP_MODES     保留的模式 ID，逗号分隔（默认 "8,36"）
 *   PROVINCE_MAX   最大省份 ID（默认 33，即 0-33 共 34 个省）
 *   REQUEST_DELAY  请求间隔 ms（默认 300）
 *
 * 用法：
 *   node collect/collect.js
 */

const puppeteer     = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs   = require('fs');
const path = require('path');

// ─────────────────── 配置 ───────────────────

const LOGIN_URL    = 'https://web.sanguosha.com/login/index.html';
const GAME_URL     = 'https://web.sanguosha.com/10/';
const KEEP_MODES   = (process.env.KEEP_MODES || '8,36').split(',').map(Number);
const PROVINCE_MAX = parseInt(process.env.PROVINCE_MAX || '33', 10);
const DELAY_MS     = parseInt(process.env.REQUEST_DELAY || '300', 10);

const ROOT       = path.resolve(__dirname, '..');
const GAMEIDS_DIR = path.join(ROOT, 'data', 'gameids');

// ─────────────────── 主流程 ───────────────────

async function main() {
    fs.mkdirSync(GAMEIDS_DIR, { recursive: true });

    const cookiesJson = process.env.GAME_COOKIES;
    if (!cookiesJson) {
        console.error('❌ 缺少环境变量 GAME_COOKIES');
        console.error('   请先运行 node collect/save_cookies.js 导出 Cookie');
        process.exit(1);
    }

    console.log('🚀 启动无头浏览器...');
    const browser = await puppeteer.launch({
        headless: 'new',
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',       // GitHub Actions 必须
            '--disable-blink-features=AutomationControlled',
        ],
    });

    const page = await browser.newPage();
    page.setDefaultTimeout(0);  // 禁用全局超时，长任务需要

    // ── 注入 console.log 拦截（必须在页面加载前） ────────────────
    await page.evaluateOnNewDocument(() => {
        // 用 iframe 恢复原始 console，防止游戏覆盖
        const _iframe = document.createElement('iframe');
        _iframe.style.display = 'none';
        document.head.appendChild(_iframe);
        const _nativeLog = _iframe.contentWindow.console.log;

        // 消息捕获中心
        window.__cap = {
            msgs:  [],                  // 所有收到的消息
            hooks: {},                  // {msgName: [resolve]}  一次性等待钩子
        };

        const _orig = console.log;
        console.log = function (...args) {
            _orig.apply(console, args);
            _nativeLog.apply(console, args);
            try {
                const m = args[0];
                if (m && typeof m === 'object' && typeof m.name === 'string' && m.payload != null) {
                    window.__cap.msgs.push({ name: m.name, payload: m.payload, sent: m.sent });
                    const waiters = window.__cap.hooks[m.name];
                    if (waiters && waiters.length) {
                        waiters.splice(0).forEach(fn => fn(m));
                    }
                }
            } catch (_) {}
        };
    });

    // ── 加载 Cookie，跳过登录 ────────────────────────────────────
    try {
        const cookies = JSON.parse(cookiesJson);
        await page.setCookie(...cookies);
        console.log(`🍪 已加载 ${cookies.length} 个 Cookie`);
    } catch (e) {
        console.error('❌ Cookie 解析失败：', e.message);
        await browser.close();
        process.exit(1);
    }

    // ── 加载游戏（先去登录页，Cookie 有效则自动跳转到游戏） ─────────
    console.log('🌐 加载游戏页面...');
    await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
    // 等待跳转到游戏主页（最多 20 秒）
    await page.waitForFunction(
        (gameUrl) => window.location.href.startsWith(gameUrl),
        { timeout: 20000 },
        GAME_URL,
    ).catch(async () => {
        // 没跳转说明 Cookie 失效，仍在登录页
        console.error('❌ Cookie 已失效，未能自动跳转到游戏页面');
        console.error('   请重新运行 node collect/save_cookies.js 更新 Cookie');
        await browser.close();
        process.exit(1);
    });

    // ── 等待鉴权完成（CRespAuth 或 CRespLogin 收到 userID） ──────
    console.log('⏳ 等待游戏认证...');
    try {
        await page.waitForFunction(() => {
            const msgs = window.__cap?.msgs || [];
            return msgs.some(m =>
                (m.name === 'cmsg.CRespAuth' || m.name === 'cmsg.CRespLogin') &&
                (m.payload?.userID || m.payload?.userId)
            );
        }, { timeout: 60000 });
    } catch (_) {
        console.error('❌ 认证超时，Cookie 可能已过期。请重新运行 save_cookies.js');
        await browser.close();
        process.exit(1);
    }
    console.log('✅ 认证成功');

    // ── 等待 PSC 可用 ────────────────────────────────────────────
    await page.evaluate(() => {
        return new Promise((resolve, reject) => {
            const deadline = Date.now() + 15000;
            const origSend = Laya.Socket.prototype.send;
            Laya.Socket.prototype.send = function (data) {
                if (!window.__psc) {
                    const handlers = this._events?.message;
                    const list = Array.isArray(handlers) ? handlers : [handlers];
                    for (const h of (list || [])) {
                        if (h?.caller?.Send) {
                            window.__psc = h.caller;
                            resolve();
                            break;
                        }
                    }
                }
                return origSend.apply(this, arguments);
            };
            // 如果 PSC 已存在（重复调用时）
            if (window.__psc) resolve();
            setTimeout(() => reject(new Error('PSC timeout')), 15000);
        });
    });
    console.log('✅ PSC 就绪');

    // ── Step 1：采集 UserID（向所有省份发排行榜请求） ────────────
    console.log(`\n📊 Step 1：采集排行榜 UserID（${PROVINCE_MAX + 1} 个省份）...`);
    const userIds = await page.evaluate(async (provinceMax, delayMs) => {
        function encodeVarint(v) {
            let val = BigInt(v), bytes = [];
            while (val > 127n) { bytes.push(Number(val & 0x7Fn) | 0x80); val >>= 7n; }
            bytes.push(Number(val));
            return bytes;
        }
        function encodeField(f, v) { return [(f << 3) | 0, ...encodeVarint(v)]; }

        function waitForMsg(name, timeoutMs = 5000) {
            return new Promise((resolve, reject) => {
                const t = setTimeout(() => reject(new Error(`timeout:${name}`)), timeoutMs);
                const cap = window.__cap;
                cap.hooks[name] = cap.hooks[name] || [];
                cap.hooks[name].push(m => { clearTimeout(t); resolve(m); });
            });
        }

        const seen = new Set();

        // CReqRankList: cmdId=3611896190
        // field1=rankType=5, field2=rangeType=3, field3=modeID=8, field4=provinceID=N
        for (let pid = 0; pid <= provinceMax; pid++) {
            const payload = new Uint8Array([
                ...encodeField(1, 5),    // rankType
                ...encodeField(2, 3),    // rangeType (省级)
                ...encodeField(3, 8),    // modeID (欢乐竞技)
                ...encodeField(4, pid),  // provinceID
            ]);

            const respPromise = waitForMsg('cmsg.CRespRankList', 6000);
            window.__psc.Send(3611896190, payload);

            try {
                const resp = await respPromise;
                for (const u of (resp.payload?.rankList || [])) {
                    if (u.userID) seen.add(String(u.userID));
                }
            } catch (_) { /* 该省无数据，跳过 */ }

            if (pid < provinceMax) await new Promise(r => setTimeout(r, delayMs));
        }

        return [...seen];
    }, PROVINCE_MAX, DELAY_MS);

    console.log(`   ✅ 收集到 ${userIds.length} 个去重 UserID`);

    // ── Step 2：按 UserID 查询 GameID ────────────────────────────
    console.log(`\n🎮 Step 2：查询 ${userIds.length} 个玩家的对局记录...`);
    console.log(`   保留模式: [${KEEP_MODES.join(', ')}]`);

    const allGameIds = await page.evaluate(async (userIds, keepModes, delayMs) => {
        function encodeVarint(v) {
            let val = BigInt(v), bytes = [];
            while (val > 127n) { bytes.push(Number(val & 0x7Fn) | 0x80); val >>= 7n; }
            bytes.push(Number(val));
            return bytes;
        }
        function encodeField(f, v) { return [(f << 3) | 0, ...encodeVarint(v)]; }

        // modeID 可能是 enum 字符串
        const MODE_STR = { MITHuanLeJingJi: 8, MITDouDiZhu: 36, MITBaRenJunZhengZiYou: 4 };
        function modeToInt(m) {
            return typeof m === 'string' ? (MODE_STR[m] || 0) : (m || 0);
        }

        function waitForMsg(name, timeoutMs = 8000) {
            return new Promise((resolve, reject) => {
                const t = setTimeout(() => reject(new Error(`timeout:${name}`)), timeoutMs);
                const cap = window.__cap;
                cap.hooks[name] = cap.hooks[name] || [];
                cap.hooks[name].push(m => { clearTimeout(t); resolve(m); });
            });
        }

        const seen = new Set();
        const results = [];  // [{userId, gameIds:[]}]

        for (let i = 0; i < userIds.length; i++) {
            const uid = userIds[i];
            const payload = new Uint8Array(encodeField(1, uid));

            const respPromise = waitForMsg('cmsg.CRespGetNewGameRecord', 8000);
            window.__psc.Send(1065628532, payload);

            try {
                const resp = await respPromise;
                const records = resp.payload?.recordData?.saveRecordList || [];
                const gameIds = records
                    .filter(r => keepModes.length === 0 || keepModes.includes(modeToInt(r.modeID)))
                    .map(r => String(r.gameID))
                    .filter(g => g && g !== '0' && !seen.has(g));

                for (const g of gameIds) seen.add(g);
                results.push({ userId: uid, gameIds });
            } catch (_) {
                results.push({ userId: uid, gameIds: [] });
            }

            if ((i + 1) % 50 === 0 || i === userIds.length - 1) {
                const pct = ((i + 1) / userIds.length * 100).toFixed(1);
                console.log(`[进度] ${i + 1}/${userIds.length} (${pct}%) | GameID: ${seen.size}`);
            }

            if (i < userIds.length - 1) await new Promise(r => setTimeout(r, delayMs));
        }

        return { results, totalGameIds: seen.size };
    }, userIds, KEEP_MODES, DELAY_MS);

    console.log(`   ✅ 共 ${allGameIds.totalGameIds} 个去重 GameID`);

    await browser.close();

    // ── 保存到 data/gameids/ ─────────────────────────────────────
    const today    = new Date().toISOString().slice(0, 10);
    const outPath  = path.join(GAMEIDS_DIR, `${today}.json`);
    const outData  = {
        metadata: {
            date:       today,
            userIds:    userIds.length,
            gameIds:    allGameIds.totalGameIds,
            keepModes:  KEEP_MODES,
            provinces:  PROVINCE_MAX + 1,
        },
        results: allGameIds.results,
    };
    fs.writeFileSync(outPath, JSON.stringify(outData, null, 2), 'utf8');
    console.log(`\n💾 已保存：${outPath}`);
    console.log(`   ${userIds.length} 个 UserID → ${allGameIds.totalGameIds} 个 GameID`);
}

main().catch(err => {
    console.error('❌ 采集失败：', err.message);
    process.exit(1);
});
