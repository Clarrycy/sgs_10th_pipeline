#!/usr/bin/env node
'use strict';

/**
 * 逃跑玩家官阶补全  collect/query_ranks.js
 *
 * 输入：data/cache/missing_ranks.json（由 pipeline/enrich_ranks.py 生成）
 *   格式: { "userIds": ["111...", "222...", ...] }
 *
 * 输出：data/cache/queried_ranks.json
 *   格式: { "111...": { "nickname": "...", "rankLevel": 30 }, ... }
 *
 * 用法：
 *   SGS_ACCOUNTS=a SGS_PASSWORDS=p node collect/query_ranks.js
 */

const puppeteer = require('puppeteer');
const fs        = require('fs');
const path      = require('path');

const LOGIN_URL = 'https://web.sanguosha.com/login/index.html';
const GAME_URL  = 'https://web.sanguosha.com/10/';
const DELAY_MS  = parseInt(process.env.REQUEST_DELAY || '300', 10);
const ROOT      = path.resolve(__dirname, '..');
const CACHE_DIR = path.join(ROOT, 'data', 'cache');

const CMD_USER_INFO = 876549255;  // CReqUserInfo

async function main() {
    // ── 读取待查 user_id 列表 ───────────────────────────────────
    const inputPath = path.join(CACHE_DIR, 'missing_ranks.json');
    if (!fs.existsSync(inputPath)) {
        console.log('ℹ️  没有待补全的玩家，跳过');
        process.exit(0);
    }
    const { userIds } = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
    if (!userIds || userIds.length === 0) {
        console.log('ℹ️  待补全列表为空，跳过');
        process.exit(0);
    }
    console.log(`📋 待查玩家：${userIds.length} 人`);

    // ── 账号（与 collect.js 相同逻辑）───────────────────────────
    const rawAccounts  = process.env.SGS_ACCOUNTS  || process.env.SGS_ACCOUNT  || '';
    const rawPasswords = process.env.SGS_PASSWORDS || process.env.SGS_PASSWORD || '';
    const accounts  = rawAccounts.replace(/，/g, ',').split(',').map(s => s.trim()).filter(Boolean);
    const passwords = rawPasswords.replace(/，/g, ',').split(',').map(s => s.trim()).filter(Boolean);
    if (!accounts.length || accounts.length !== passwords.length) {
        console.error('❌ 缺少环境变量 SGS_ACCOUNTS / SGS_PASSWORDS');
        process.exit(1);
    }
    // 按当前小时 +1 偏移轮替（与 collect.js 错开，减少会话冲突）
    const hour = new Date().getUTCHours();
    const idx = (hour + 1) % accounts.length;
    const SGS_ACCOUNT  = accounts[idx];
    const SGS_PASSWORD = passwords[idx];
    console.log(`📋 使用账号 ${idx + 1}/${accounts.length}：${SGS_ACCOUNT}`);

    // ── 启动浏览器 ──────────────────────────────────────────────
    const browser = await puppeteer.launch({
        headless: 'new',
        args: [
            '--no-sandbox', '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
            '--incognito',
        ],
    });
    const ctx  = await browser.createBrowserContext();
    const page = await ctx.newPage();
    page.setDefaultTimeout(0);
    page.on('dialog', async d => d.dismiss());

    await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    );
    await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    // ── 注入工具函数 + console hook ─────────────────────────────
    await page.evaluateOnNewDocument(() => {
        window.encodeVarint = function(v) {
            let val = BigInt(v), bytes = [];
            while (val > 127n) { bytes.push(Number(val & 0x7Fn) | 0x80); val >>= 7n; }
            bytes.push(Number(val));
            return bytes;
        };
        window.encodeField = function(f, v) { return [(f << 3) | 0, ...encodeVarint(v)]; };
        window.delay = ms => new Promise(r => setTimeout(r, ms));

        window.__cap = { hooks: {} };
        const _orig = console.log.bind(console);
        const _hook = function(...args) {
            _orig(...args);
            try {
                if (args[0] !== '%o' || typeof args[1] !== 'string') return;
                const header = args[1];
                if (!header.startsWith('--------[')) return;
                let name, payload;
                if (header.includes('[Received]')) {
                    name = typeof args[2] === 'string' ? args[2] : '';
                    payload = args[4];
                } else if (header.includes('[  Sent  ]') || header.includes('[Cached]')) {
                    const m = header.match(/name:(cmsg\.\w+)/);
                    if (m) { name = m[1]; payload = args[2]; }
                }
                if (!name) return;
                const waiters = window.__cap.hooks[name];
                if (waiters && waiters.length) waiters.splice(0).forEach(fn => fn({ name, payload: payload || {} }));
            } catch (_) {}
        };
        Object.defineProperty(console, 'log', { get: () => _hook, set: () => {}, configurable: true });
    });

    // ── 登录 ────────────────────────────────────────────────────
    console.log('🌐 登录...');
    await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForSelector('#SGS_login-account', { timeout: 10000 });
    await page.type('#SGS_login-account', SGS_ACCOUNT, { delay: 50 });
    await page.type('#SGS_login-password', SGS_PASSWORD, { delay: 50 });
    const agreed = await page.$eval('#SGS_userProto', el => el.checked);
    if (!agreed) await page.click('#SGS_userProto');
    await page.click('#SGS_login-btn');

    await page.waitForSelector('#selectGame', { visible: true, timeout: 60000 }).catch(async () => {
        console.error('❌ 登录失败');
        await browser.close();
        process.exit(1);
    });
    await page.evaluate(() => {
        const items = document.querySelectorAll('#oL10th .game-item');
        if (items.length >= 3) items[2].click();
    });
    await new Promise(r => setTimeout(r, 500));
    await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
        page.click('#goInGameBtn'),
    ]).catch(() => {});
    if (!page.url().startsWith(GAME_URL)) {
        await page.evaluate(() => { const el = document.querySelector('#goInGameBtn'); if (el) el.click(); });
        await page.waitForFunction(u => window.location.href.startsWith(u), { timeout: 15000 }, GAME_URL).catch(() => {});
    }

    // ── 等待认证 + PSC ──────────────────────────────────────────
    await page.waitForFunction(() => typeof Laya !== 'undefined' && Laya.stage, { timeout: 30000 });
    await new Promise(r => setTimeout(r, 5000));

    // 处理弹窗
    await page.evaluate(() => {
        const canvas = document.querySelector('canvas');
        if (!canvas) return;
        const rect = canvas.getBoundingClientRect();
        const x = rect.left + (340 / 800) * rect.width;
        const y = rect.top  + (300 / 450) * rect.height;
        for (const t of ['mousedown', 'mouseup', 'click'])
            canvas.dispatchEvent(new MouseEvent(t, { clientX: x, clientY: y, bubbles: true, button: 0 }));
    });
    await new Promise(r => setTimeout(r, 2000));

    await page.waitForFunction(() => {
        const msgs = Object.keys(window.__cap?.hooks || {});
        return window.__cap && true;  // hooks 初始化即可
    }, { timeout: 60000 });

    // 等待 PSC
    await page.evaluate(() => new Promise((resolve, reject) => {
        const orig = Laya.Socket.prototype.send;
        Laya.Socket.prototype.send = function(data) {
            if (!window.__psc) {
                const handlers = this._events?.message;
                const list = Array.isArray(handlers) ? handlers : [handlers];
                for (const h of (list || [])) {
                    if (h?.caller?.Send) { window.__psc = h.caller; resolve(); break; }
                }
            }
            return orig.apply(this, arguments);
        };
        if (window.__psc) resolve();
        setTimeout(() => reject(new Error('PSC timeout')), 20000);
    }));
    console.log('✅ PSC 就绪，开始查询...');

    // ── 批量查询 CReqUserInfo ────────────────────────────────────
    const results = await page.evaluate(async (userIds, CMD, delayMs) => {
        const out = {};
        for (let i = 0; i < userIds.length; i++) {
            const uid = userIds[i];
            const payload = new Uint8Array([
                ...encodeField(1, uid),   // userID
                ...encodeField(4, 0),     // typ = 0
            ]);
            const respPromise = new Promise((resolve, reject) => {
                const t = setTimeout(() => reject(new Error('timeout')), 8000);
                window.__cap.hooks['cmsg.CRespUserInfo'] = window.__cap.hooks['cmsg.CRespUserInfo'] || [];
                window.__cap.hooks['cmsg.CRespUserInfo'].push(m => { clearTimeout(t); resolve(m); });
            });
            window.__psc.Send(CMD, payload);
            try {
                const resp = await respPromise;
                const brief = resp.payload?.userBrief || resp.payload?.userInfo || resp.payload;
                const nickname   = brief?.nickname || brief?.name || '';
                const rankLevel  = brief?.officialRankSimpleData?.level ?? brief?.officialRank?.level ?? null;
                out[uid] = { nickname, rankLevel };
            } catch (_) {
                out[uid] = { nickname: '', rankLevel: null };
            }
            if ((i + 1) % 50 === 0 || i === userIds.length - 1)
                console.log('[补全进度] ' + (i + 1) + '/' + userIds.length);
            if (i < userIds.length - 1) await delay(delayMs);
        }
        return out;
    }, userIds, CMD_USER_INFO, DELAY_MS);

    await browser.close();

    // ── 写出结果 ────────────────────────────────────────────────
    const outPath = path.join(CACHE_DIR, 'queried_ranks.json');
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2), 'utf8');
    const success = Object.values(results).filter(v => v.rankLevel !== null).length;
    console.log(`✅ 完成：${success}/${userIds.length} 成功补全 → ${outPath}`);
}

main().catch(err => {
    console.error('❌ query_ranks 失败：', err.message);
    process.exit(1);
});
