#!/usr/bin/env node
'use strict';

/**
 * SGS 全自动采集脚本  collect/collect.js
 *
 * 三种运行模式：
 *   --mode=boards   榜单线：单账号轮替，抓排行榜 + 查战绩（Line A, 每小时）
 *   --mode=friends  好友线：全量账号并行，好友推荐 + 查战绩（Line B, 每 4 小时）
 *   (无参数)        完整模式：向后兼容，单账号跑全部
 *
 * 环境变量：
 *   SGS_ACCOUNTS    登录账号（逗号分隔）
 *   SGS_PASSWORDS   登录密码（逗号分隔，与账号一一对应）
 *   KEEP_MODES      保留的模式 ID（默认 "4,8,36"）
 *   PROVINCE_MAX    最大省份 ID（默认 33）
 *   FRIEND_ROUNDS   好友推荐刷新轮数（friends 模式默认 200，其他默认 300）
 *   REQUEST_DELAY   请求间隔 ms（默认 300）
 */

const puppeteer = require('puppeteer');
const fs        = require('fs');
const path      = require('path');

// ─────────────────── 模式解析 ───────────────────

const MODE = process.argv.includes('--mode=friends') ? 'friends'
           : process.argv.includes('--mode=boards')  ? 'boards'
           : 'full';

// ─────────────────── 配置 ───────────────────

const LOGIN_URL      = 'https://web.sanguosha.com/login/index.html';
const GAME_URL       = 'https://web.sanguosha.com/10/';
const KEEP_MODES     = (process.env.KEEP_MODES || '4,8,36').split(',').map(Number);
const PROVINCE_MAX   = parseInt(process.env.PROVINCE_MAX || '33', 10);
const FRIEND_ROUNDS  = parseInt(process.env.FRIEND_ROUNDS || (MODE === 'friends' ? '200' : '300'), 10);
const DELAY_MS       = parseInt(process.env.REQUEST_DELAY || '300', 10);

const ROOT        = path.resolve(__dirname, '..');
const GAMEIDS_DIR = path.join(ROOT, 'data', 'gameids');
const CACHE_DIR   = path.join(ROOT, 'data', 'cache');

const GAME_BATCH      = 100;   // 每批并发请求数
const BATCH_SLEEP_MIN = 2000;  // 批次间隔下限 ms
const BATCH_SLEEP_MAX = 5000;  // 批次间隔上限 ms

// ─────────────────── cmdId 常量 ───────────────────

const CMD_RANK_LIST          = 3611896190;
const CMD_FRIEND_RECOMMEND   = 2818936274;
const CMD_GET_GAME_RECORD    = 1065628532;

// ─────────────────── 工具函数 ───────────────────

function parseAccounts() {
    const rawAccounts  = process.env.SGS_ACCOUNTS  || process.env.SGS_ACCOUNT  || '';
    const rawPasswords = process.env.SGS_PASSWORDS || process.env.SGS_PASSWORD || '';
    const accounts  = rawAccounts.replace(/，/g, ',').split(',').map(s => s.trim()).filter(Boolean);
    const passwords = rawPasswords.replace(/，/g, ',').split(',').map(s => s.trim()).filter(Boolean);
    if (!accounts.length || accounts.length !== passwords.length) {
        console.error('❌ 缺少环境变量 SGS_ACCOUNTS / SGS_PASSWORDS（逗号分隔，数量需一致）');
        console.error(`   当前: accounts=${accounts.length}, passwords=${passwords.length}`);
        process.exit(1);
    }
    return { accounts, passwords };
}

function partitionArray(arr, n) {
    const result = Array.from({ length: n }, () => []);
    arr.forEach((item, i) => result[i % n].push(item));
    return result;
}

// ═══════════════════════════════════════════════════
//  会话建立（可复用，支持多账号并行）
// ═══════════════════════════════════════════════════

async function setupSession(account, password) {
    const tag = `[${account}]`;

    console.log(`🚀 ${tag} 启动浏览器...`);
    const browser = await puppeteer.launch({
        headless: 'new',
        protocolTimeout: 0,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
            '--incognito',
            '--use-fake-ui-for-media-stream',
            '--use-fake-device-for-media-stream',
        ],
    });

    const ctx = await browser.createBrowserContext();
    await ctx.overridePermissions('https://web.sanguosha.com', []);
    const page = await ctx.newPage();
    page.setDefaultTimeout(0);
    page.on('dialog', async dialog => { await dialog.dismiss(); });

    // ── 反检测 ──
    await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    );
    await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en'] });
    });

    // ── 注入采集工具函数 ──
    await page.evaluateOnNewDocument(() => {
        window.__utils = true;

        window.encodeVarint = function(v) {
            let val = BigInt(v), bytes = [];
            while (val > 127n) { bytes.push(Number(val & 0x7Fn) | 0x80); val >>= 7n; }
            bytes.push(Number(val));
            return bytes;
        };
        window.encodeField = function(f, v) { return [(f << 3) | 0, ...encodeVarint(v)]; };

        window.waitForMsg = function(name, timeoutMs) {
            timeoutMs = timeoutMs || 8000;
            return new Promise((resolve, reject) => {
                const t = setTimeout(() => reject(new Error('timeout:' + name)), timeoutMs);
                const cap = window.__cap;
                cap.hooks[name] = cap.hooks[name] || [];
                cap.hooks[name].push(m => { clearTimeout(t); resolve(m); });
            });
        };

        window.collectMsgs = function(name, timeoutMs) {
            timeoutMs = timeoutMs || 20000;
            return new Promise(resolve => {
                const results = [];
                let timer = null;
                const cap = window.__cap;
                const deadline = setTimeout(() => resolve(results), timeoutMs);
                function resetTimer() {
                    if (timer) clearTimeout(timer);
                    timer = setTimeout(() => { clearTimeout(deadline); resolve(results); }, 3000);
                }
                cap.hooks[name] = cap.hooks[name] || [];
                function handler(m) {
                    results.push(m);
                    if (m.payload?.isFinish) {
                        clearTimeout(deadline);
                        if (timer) clearTimeout(timer);
                        resolve(results);
                    } else {
                        resetTimer();
                        cap.hooks[name].push(handler);
                    }
                }
                cap.hooks[name].push(handler);
                resetTimer();
            });
        };

        window.collectNMsgs = function(name, n, timeoutMs) {
            timeoutMs = timeoutMs || 30000;
            return new Promise(resolve => {
                const results = [];
                const deadline = setTimeout(() => resolve(results), timeoutMs);
                const cap = window.__cap;
                cap.hooks[name] = cap.hooks[name] || [];
                function handler(m) {
                    results.push(m);
                    if (results.length >= n) {
                        clearTimeout(deadline);
                        resolve(results);
                    } else {
                        cap.hooks[name].push(handler);
                    }
                }
                cap.hooks[name].push(handler);
            });
        };

        const MODE_STR = { MITHuanLeJingJi: 8, MITDouDiZhu: 36, MITBaRenJunZhengZiYou: 4, MITShenFenJingji: 4 };
        window.modeToInt = function(m) { return typeof m === 'string' ? (MODE_STR[m] || 0) : (m || 0); };
        window.delay = function(ms) { return new Promise(r => setTimeout(r, ms)); };
    });

    // ── console.log 拦截 ──
    await page.evaluateOnNewDocument(() => {
        window.__cap = { msgs: [], hooks: {} };

        const _orig = console.log.bind(console);

        function _dispatch(name, payload) {
            const sent = name.startsWith('cmsg.CReq') || name.startsWith('cmsg.CNotify') === false;
            window.__cap.msgs.push({ name, payload: payload || {}, sent });
            const waiters = window.__cap.hooks[name];
            if (waiters && waiters.length) {
                waiters.splice(0).forEach(fn => fn({ name, payload: payload || {}, sent }));
            }
        }

        const _hook = function (...args) {
            _orig(...args);
            try {
                if (args[0] !== '%o' || typeof args[1] !== 'string') return;
                const header = args[1];
                if (!header.startsWith('--------[')) return;

                if (header.includes('[Received]')) {
                    const name = typeof args[2] === 'string' ? args[2] : '';
                    const payload = args[4];
                    if (name) _dispatch(name, payload);
                } else if (header.includes('[  Sent  ]') || header.includes('[Cached]')) {
                    const m = header.match(/name:(cmsg\.\w+)/);
                    if (m) _dispatch(m[1], args[2]);
                }
            } catch (_) {}
        };

        Object.defineProperty(console, 'log', {
            get: () => _hook,
            set: () => {},
            configurable: true,
        });
    });

    // ── 登录 ──
    console.log(`🌐 ${tag} 打开登录页...`);
    await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });

    console.log(`✏️  ${tag} 填写账号密码...`);
    await page.waitForSelector('#SGS_login-account', { timeout: 10000 });
    await page.type('#SGS_login-account', account, { delay: 50 });
    await page.type('#SGS_login-password', password, { delay: 50 });

    const agreed = await page.$eval('#SGS_userProto', el => el.checked);
    if (!agreed) await page.click('#SGS_userProto');

    console.log(`🔑 ${tag} 点击登录...`);
    await page.click('#SGS_login-btn');

    // ── 游戏选择 ──
    console.log(`🎮 ${tag} 等待游戏选择界面...`);
    await page.waitForSelector('#selectGame', { visible: true, timeout: 60000 }).catch(async () => {
        console.error(`❌ ${tag} 登录失败`);
        await browser.close();
        throw new Error(`login failed: ${account}`);
    });

    console.log(`🎮 ${tag} 选择三国杀：一将成名...`);
    await page.evaluate(() => {
        const items = document.querySelectorAll('#oL10th .game-item');
        if (items.length >= 3) items[2].click();
    });
    await new Promise(r => setTimeout(r, 500));

    console.log(`⏳ ${tag} 点击进入游戏...`);
    await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
        page.click('#goInGameBtn'),
    ]).catch(async () => {
        console.log(`   ${tag} 导航未触发，检查 URL...`);
        await new Promise(r => setTimeout(r, 5000));
    });
    if (!page.url().startsWith(GAME_URL)) {
        await page.evaluate(() => {
            const el = document.querySelector('#goInGameBtn');
            if (el) el.click();
        });
        await page.waitForFunction(
            (u) => window.location.href.startsWith(u), { timeout: 15000 }, GAME_URL,
        ).catch(async () => {
            console.error(`❌ ${tag} 无法进入游戏页面`);
            await browser.close();
            throw new Error(`game entry failed: ${account}`);
        });
    }

    // ── 处理 Laya 弹窗 ──
    console.log(`🔇 ${tag} 处理游戏内弹窗...`);
    await page.waitForFunction(() => typeof Laya !== 'undefined' && Laya.stage, { timeout: 30000 });
    await new Promise(r => setTimeout(r, 5000));

    for (let i = 0; i < 2; i++) {
        await page.evaluate(() => {
            const canvas = document.querySelector('canvas');
            if (!canvas) return;
            const rect = canvas.getBoundingClientRect();
            const clientX = rect.left + (340 / 800) * rect.width;
            const clientY = rect.top + (300 / 450) * rect.height;
            for (const type of ['mousedown', 'mouseup', 'click']) {
                canvas.dispatchEvent(new MouseEvent(type, {
                    clientX, clientY, bubbles: true, cancelable: true, button: 0
                }));
            }
        });
        await new Promise(r => setTimeout(r, 2000));
    }

    // ── 等待认证 ──
    console.log(`⏳ ${tag} 等待游戏认证...`);
    try {
        await page.waitForFunction(() => {
            const msgs = window.__cap?.msgs || [];
            return msgs.some(m =>
                (m.name === 'cmsg.CRespAuth' || m.name === 'cmsg.CRespLogin') &&
                (m.payload?.userID || m.payload?.userId)
            );
        }, { timeout: 60000 });
    } catch (_) {
        const diag = await page.evaluate(() => {
            const cap = window.__cap;
            return {
                msgCount: cap ? cap.msgs.length : -1,
                msgNames: cap ? [...new Set(cap.msgs.map(m => m.name))].slice(0, 20) : [],
            };
        });
        console.error(`❌ ${tag} 认证超时 — 诊断:`, JSON.stringify(diag));
        await browser.close();
        throw new Error(`auth timeout: ${account}`);
    }
    console.log(`✅ ${tag} 认证成功`);

    // ── 等待 PSC ──
    await page.evaluate(() => {
        return new Promise((resolve, reject) => {
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
            if (window.__psc) resolve();
            setTimeout(() => reject(new Error('PSC timeout')), 15000);
        });
    });
    console.log(`✅ ${tag} PSC 就绪`);

    return { browser, page, account };
}

// ═══════════════════════════════════════════════════
//  采集功能函数
// ═══════════════════════════════════════════════════

// ── 排行榜采集（含每日缓存）──────────────────────

async function collectBoards(page) {
    const todayStr = new Date().toISOString().slice(0, 10);
    const cachePath = path.join(CACHE_DIR, `boards_${todayStr}.json`);
    let boardCache = null;

    if (fs.existsSync(cachePath)) {
        boardCache = JSON.parse(fs.readFileSync(cachePath, 'utf8'));
        console.log(`\n📦 榜单缓存命中：${cachePath}`);
        const bc = boardCache;
        console.log(`   官阶 ${bc.officialRank?.count || 0} 人 | 省级排位 ${bc.provincial?.count || 0} 人 | 省级身份 ${bc.provincialIdentity?.count || 0} 人 | 斗地主 ${bc.doudizhuRank?.count || 0} 人`);
    }

    let officialRankIds, provinceRankedIds, provinceIdentityIds, doudizhuRankIds;
    let needCacheUpdate = !boardCache;

    // Step 1A：官阶榜 top 500
    if (boardCache?.officialRank) {
        officialRankIds = boardCache.officialRank.ids;
        console.log(`\n📦 Step 1A：使用缓存（${officialRankIds.length} 人）`);
    } else {
        console.log('\n📊 Step 1A：官阶榜 top 500...');
        needCacheUpdate = true;
        const officialResult = await page.evaluate(async (CMD, delayMs) => {
            const seen = new Set();
            const payload = new Uint8Array([
                ...encodeField(1, 20),
                ...encodeField(2, 1),
            ]);
            const resps = collectMsgs('cmsg.CRespRankList', 20000);
            window.__psc.Send(CMD, payload);
            const pages = await resps;
            for (const p of pages) {
                for (const u of (p.payload?.rankList || [])) {
                    if (u.userID) seen.add(String(u.userID));
                }
            }
            return { ids: [...seen], pages: pages.length };
        }, CMD_RANK_LIST, DELAY_MS);
        officialRankIds = officialResult.ids;
        console.log(`   ✅ 官阶榜 ${officialResult.pages} 页, ${officialRankIds.length} 人`);
    }

    // Step 1B：省级排位榜（2v2, modeID=8）
    if (boardCache?.provincial) {
        provinceRankedIds = boardCache.provincial.ids;
        console.log(`\n📦 Step 1B：使用缓存（${provinceRankedIds.length} 人）`);
    } else {
        console.log(`\n📊 Step 1B：省级排位榜（${PROVINCE_MAX + 1} 个省份, modeID=8）...`);
        needCacheUpdate = true;
        provinceRankedIds = await page.evaluate(async (CMD, provinceMax, delayMs) => {
            const seen = new Set();
            for (let pid = 0; pid <= provinceMax; pid++) {
                const payload = new Uint8Array([
                    ...encodeField(1, 5),
                    ...encodeField(2, 3),
                    ...encodeField(3, 8),
                    ...encodeField(4, pid),
                ]);
                const respPromise = waitForMsg('cmsg.CRespRankList', 6000);
                window.__psc.Send(CMD, payload);
                try {
                    const resp = await respPromise;
                    for (const u of (resp.payload?.rankList || [])) {
                        if (u.userID) seen.add(String(u.userID));
                    }
                } catch (_) {}
                if (pid < provinceMax) await delay(delayMs);
            }
            return [...seen];
        }, CMD_RANK_LIST, PROVINCE_MAX, DELAY_MS);
        console.log(`   ✅ 省级排位榜 ${provinceRankedIds.length} 人`);
    }

    // Step 1B2：省级身份排位榜（身份竞技, modeID=4）
    if (boardCache?.provincialIdentity) {
        provinceIdentityIds = boardCache.provincialIdentity.ids;
        console.log(`\n📦 Step 1B2：使用缓存（${provinceIdentityIds.length} 人）`);
    } else {
        console.log(`\n📊 Step 1B2：省级身份排位榜（${PROVINCE_MAX + 1} 个省份, modeID=4）...`);
        needCacheUpdate = true;
        provinceIdentityIds = await page.evaluate(async (CMD, provinceMax, delayMs) => {
            const seen = new Set();
            const globalPayload = new Uint8Array([
                ...encodeField(1, 5),
                ...encodeField(2, 1),
                ...encodeField(3, 4),
            ]);
            const globalResp = waitForMsg('cmsg.CRespRankList', 10000);
            window.__psc.Send(CMD, globalPayload);
            try {
                const resp = await globalResp;
                for (const u of (resp.payload?.rankList || [])) {
                    if (u.userID) seen.add(String(u.userID));
                }
            } catch (_) {}
            await delay(delayMs);

            for (let pid = 0; pid <= provinceMax; pid++) {
                const payload = new Uint8Array([
                    ...encodeField(1, 5),
                    ...encodeField(2, 3),
                    ...encodeField(3, 4),
                    ...encodeField(4, pid),
                ]);
                const respPromise = waitForMsg('cmsg.CRespRankList', 6000);
                window.__psc.Send(CMD, payload);
                try {
                    const resp = await respPromise;
                    for (const u of (resp.payload?.rankList || [])) {
                        if (u.userID) seen.add(String(u.userID));
                    }
                } catch (_) {}
                if (pid < provinceMax) await delay(delayMs);
            }
            return [...seen];
        }, CMD_RANK_LIST, PROVINCE_MAX, DELAY_MS);
        console.log(`   ✅ 省级身份排位榜 ${provinceIdentityIds.length} 人`);
    }

    // Step 1B3：斗地主排行榜（全服月榜 top 100）
    if (boardCache?.doudizhuRank) {
        doudizhuRankIds = boardCache.doudizhuRank.ids;
        console.log(`\n📦 Step 1B3：使用缓存（${doudizhuRankIds.length} 人）`);
    } else {
        console.log(`\n📊 Step 1B3：斗地主排行榜（全服月榜）...`);
        needCacheUpdate = true;
        doudizhuRankIds = await page.evaluate(async (CMD, delayMs) => {
            const seen = new Set();
            const payload = new Uint8Array([
                ...encodeField(1, 22),
                ...encodeField(2, 4),
            ]);
            const respPromise = waitForMsg('cmsg.CRespRankList', 10000);
            window.__psc.Send(CMD, payload);
            try {
                const resp = await respPromise;
                for (const u of (resp.payload?.rankList || [])) {
                    if (u.userID) seen.add(String(u.userID));
                }
            } catch (_) {}
            return [...seen];
        }, CMD_RANK_LIST, DELAY_MS);
        console.log(`   ✅ 斗地主排行榜 ${doudizhuRankIds.length} 人`);
    }

    // 写入/更新每日榜单缓存
    if (needCacheUpdate) {
        fs.mkdirSync(CACHE_DIR, { recursive: true });
        fs.writeFileSync(cachePath, JSON.stringify({
            date: todayStr,
            createdAt: new Date().toISOString(),
            officialRank:        { count: officialRankIds.length, ids: officialRankIds },
            provincial:          { count: provinceRankedIds.length, ids: provinceRankedIds },
            provincialIdentity:  { count: provinceIdentityIds.length, ids: provinceIdentityIds },
            doudizhuRank:        { count: doudizhuRankIds.length, ids: doudizhuRankIds },
        }, null, 2), 'utf8');
        console.log(`\n💾 榜单缓存已${boardCache ? '更新' : '保存'}：${cachePath}`);
    }

    return { officialRankIds, provinceRankedIds, provinceIdentityIds, doudizhuRankIds };
}

// ── 好友推荐采集 ──────────────────────────────────

async function collectFriends(page, account, rounds) {
    const tag = `[${account}]`;
    console.log(`📊 ${tag} 好友推荐（${rounds} 轮 × ~20 人）...`);
    const friendIds = await page.evaluate(async (CMD, rounds, delayMs) => {
        const seen = new Set();
        for (let i = 0; i < rounds; i++) {
            const respPromise = waitForMsg('cmsg.CRespFriendRecommend', 6000);
            window.__psc.Send(CMD, new Uint8Array(0));
            try {
                const resp = await respPromise;
                for (const u of (resp.payload?.users || [])) {
                    if (u.userID) seen.add(String(u.userID));
                }
            } catch (_) {}
            if ((i + 1) % 50 === 0) {
                console.log('[好友推荐] ' + (i + 1) + '/' + rounds + ' | UserID: ' + seen.size);
            }
            await delay(delayMs);
        }
        return [...seen];
    }, CMD_FRIEND_RECOMMEND, rounds, DELAY_MS);
    console.log(`   ✅ ${tag} 好友推荐 ${friendIds.length} 人`);
    return friendIds;
}

// ── GameID 批量查询 ──────────────────────────────

async function queryGameIds(page, account, userIds) {
    const tag = `[${account}]`;
    console.log(`🎮 ${tag} 查询 ${userIds.length} 个玩家的对局记录（${GAME_BATCH} 并发）...`);

    const result = await page.evaluate(async (userIds, CMD, keepModes, batchSize, sleepMin, sleepMax) => {
        const seen = new Set();
        const results = [];
        const totalBatches = Math.ceil(userIds.length / batchSize);

        for (let b = 0; b < totalBatches; b++) {
            const start = b * batchSize;
            const end = Math.min(start + batchSize, userIds.length);
            const batch = userIds.slice(start, end);

            const respPromise = collectNMsgs(
                'cmsg.CRespGetNewGameRecord',
                batch.length,
                batch.length * 500 + 15000,
            );
            for (const uid of batch) {
                window.__psc.Send(CMD, new Uint8Array(encodeField(1, uid)));
            }
            const responses = await respPromise;

            for (let j = 0; j < batch.length; j++) {
                const uid = batch[j];
                const resp = j < responses.length ? responses[j] : null;
                if (resp) {
                    const records = resp.payload?.recordData?.saveRecordList || [];
                    const gameIds = records
                        .filter(r => keepModes.length === 0 || keepModes.includes(modeToInt(r.modeID)))
                        .map(r => ({
                            gameId:      String(r.gameID),
                            modeId:      modeToInt(r.modeID),
                            gameTime:    r.gameStartTime || 0,
                            result:      r.gameResult || '',
                            isMvp:       !!r.isMvp,
                            isEscape:    !!r.isEscape,
                            figure:      r.figure || 0,
                            generalId:   (r.usingCharacters || [])[0] || 0,
                            scoreChange: r.scoreChange || 0,
                        }))
                        .filter(o => o.gameId && o.gameId !== '0' && !seen.has(o.gameId));
                    for (const o of gameIds) seen.add(o.gameId);
                    results.push({ userId: uid, gameIds });
                } else {
                    results.push({ userId: uid, gameIds: [] });
                }
            }

            const pct = (end / userIds.length * 100).toFixed(1);
            console.log('[进度] 批次 ' + (b+1) + '/' + totalBatches
                + ' | 已查 ' + end + '/' + userIds.length + ' (' + pct + '%)'
                + ' | GameID: ' + seen.size);

            if (b < totalBatches - 1) {
                const sleepMs = sleepMin + Math.random() * (sleepMax - sleepMin);
                await delay(sleepMs);
            }
        }
        return { results, totalGameIds: seen.size };
    }, userIds, CMD_GET_GAME_RECORD, KEEP_MODES, GAME_BATCH, BATCH_SLEEP_MIN, BATCH_SLEEP_MAX);

    console.log(`   ✅ ${tag} 共 ${result.totalGameIds} 个去重 GameID`);
    return result;
}

// ── 保存结果 ──────────────────────────────────────

function saveOutput(prefix, metadata, results, totalGameIds) {
    fs.mkdirSync(GAMEIDS_DIR, { recursive: true });
    const now     = new Date();
    const today   = now.toISOString().slice(0, 10);
    const timeTag = now.toISOString().slice(11, 16).replace(':', '');
    const batchId = `${prefix}_${today}_${timeTag}`;
    const outPath = path.join(GAMEIDS_DIR, `${batchId}.json`);
    const outData = {
        metadata: {
            ...metadata,
            date:         today,
            time:         now.toISOString(),
            batchId,
        },
        results,
    };
    fs.writeFileSync(outPath, JSON.stringify(outData, null, 2), 'utf8');
    console.log(`\n💾 已保存：${outPath}`);
    console.log(`   ${metadata.totalUserIds} 个 UserID → ${totalGameIds} 个 GameID`);
}

// ═══════════════════════════════════════════════════
//  模式入口
// ═══════════════════════════════════════════════════

// ── boards 模式：单账号，排行榜 + 查战绩 ─────────

async function runBoards() {
    const { accounts, passwords } = parseAccounts();
    const hour = new Date().getUTCHours();
    const idx = hour % accounts.length;
    console.log(`📋 [boards] 使用账号 ${idx + 1}/${accounts.length}: ${accounts[idx]}  (UTC hour=${hour})\n`);

    // 认证失败时自动重试（换下一个账号）
    let browser, page, account;
    for (let attempt = 0; attempt < accounts.length; attempt++) {
        const tryIdx = (idx + attempt) % accounts.length;
        try {
            ({ browser, page, account } = await setupSession(accounts[tryIdx], passwords[tryIdx]));
            break;
        } catch (err) {
            console.error(`❌ 账号 ${accounts[tryIdx]} 登录失败: ${err.message}`);
            if (attempt < accounts.length - 1) {
                console.log(`⏳ 等待 10 秒后尝试下一个账号...`);
                await new Promise(r => setTimeout(r, 10000));
            }
        }
    }
    if (!page) {
        console.error('❌ 所有账号登录失败');
        process.exit(1);
    }

    const { officialRankIds, provinceRankedIds, provinceIdentityIds, doudizhuRankIds } = await collectBoards(page);

    const allUserIdSet = new Set([...officialRankIds, ...provinceRankedIds, ...provinceIdentityIds, ...doudizhuRankIds]);
    const allUserIds = [...allUserIdSet];
    console.log(`\n📋 合计去重 UserID: ${allUserIds.length} (官阶${officialRankIds.length} + 省级排位${provinceRankedIds.length} + 省级身份${provinceIdentityIds.length} + 斗地主${doudizhuRankIds.length})`);

    const gameIdResult = await queryGameIds(page, account, allUserIds);

    await browser.close();

    saveOutput('boards', {
        mode:         'boards',
        account,
        sources: {
            officialRank:        officialRankIds.length,
            provincial:          provinceRankedIds.length,
            provincialIdentity:  provinceIdentityIds.length,
            doudizhuRank:        doudizhuRankIds.length,
        },
        totalUserIds: allUserIds.length,
        totalGameIds: gameIdResult.totalGameIds,
        keepModes:    KEEP_MODES,
    }, gameIdResult.results, gameIdResult.totalGameIds);
}

// ── friends 模式：全量账号并行，好友推荐 + 查战绩 ──

async function runFriends() {
    const { accounts, passwords } = parseAccounts();
    console.log(`📋 [friends] ${accounts.length} 个账号串行登录 → 并行采集, 每账号 ${FRIEND_ROUNDS} 轮\n`);

    // 串行登录（同 IP 并发 WebSocket 会被服务器限流，逐个等认证通过）
    // 认证失败时自动重试一次（服务器偶尔不回 CRespAuth）
    const MAX_RETRIES = 2;
    const sessions = [];
    for (let i = 0; i < accounts.length; i++) {
        let session = null;
        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                session = await setupSession(accounts[i], passwords[i]);
                break;
            } catch (err) {
                console.error(`❌ 账号 ${accounts[i]} 第 ${attempt}/${MAX_RETRIES} 次登录失败: ${err.message}`);
                if (attempt < MAX_RETRIES) {
                    console.log(`⏳ 等待 10 秒后重试...`);
                    await new Promise(r => setTimeout(r, 10000));
                }
            }
        }
        if (session) sessions.push(session);
    }

    if (sessions.length === 0) {
        console.error('❌ 所有账号登录失败');
        process.exit(1);
    }
    console.log(`\n✅ ${sessions.length}/${accounts.length} 个账号登录成功\n`);

    // 并行采集好友
    const friendResults = await Promise.allSettled(
        sessions.map(({ page, account }) => collectFriends(page, account, FRIEND_ROUNDS))
    );

    const allFriendIds = new Set();
    for (const r of friendResults) {
        if (r.status === 'fulfilled') {
            r.value.forEach(id => allFriendIds.add(id));
        }
    }
    const allUserIds = [...allFriendIds];
    console.log(`\n📋 合计去重好友 UserID: ${allUserIds.length}`);

    if (allUserIds.length === 0) {
        console.log('⚠️ 没有发现任何好友，跳过 GameID 查询');
        for (const { browser } of sessions) await browser.close().catch(() => {});
        saveOutput('friends', {
            mode: 'friends', accounts: sessions.map(s => s.account),
            sources: { friendRec: 0 }, totalUserIds: 0, totalGameIds: 0,
            keepModes: KEEP_MODES, friendRounds: FRIEND_ROUNDS,
        }, [], 0);
        return;
    }

    // 将 UserID 均分给各会话，并行查询 GameID
    const parts = partitionArray(allUserIds, sessions.length);
    const queryResults = await Promise.allSettled(
        sessions.map(async ({ page, account }, i) => {
            return queryGameIds(page, account, parts[i]);
        })
    );

    // 合并结果（跨会话 GameID 去重）
    const allResults = [];
    const seenGids = new Set();
    for (const r of queryResults) {
        if (r.status === 'fulfilled') {
            for (const entry of r.value.results) {
                entry.gameIds = entry.gameIds.filter(g => {
                    if (seenGids.has(g.gameId)) return false;
                    seenGids.add(g.gameId);
                    return true;
                });
                allResults.push(entry);
            }
        }
    }

    // 关闭所有浏览器
    for (const { browser } of sessions) {
        await browser.close().catch(() => {});
    }

    saveOutput('friends', {
        mode:         'friends',
        accounts:     sessions.map(s => s.account),
        sources:      { friendRec: allUserIds.length },
        totalUserIds: allUserIds.length,
        totalGameIds: seenGids.size,
        keepModes:    KEEP_MODES,
        friendRounds: FRIEND_ROUNDS,
    }, allResults, seenGids.size);
}

// ── full 模式：向后兼容，单账号跑全部 ────────────

async function runFull() {
    const { accounts, passwords } = parseAccounts();
    const hour = new Date().getUTCHours();
    const idx = hour % accounts.length;
    console.log(`📋 [full] 使用账号 ${idx + 1}/${accounts.length}: ${accounts[idx]}  (UTC hour=${hour})\n`);

    const { browser, page, account } = await setupSession(accounts[idx], passwords[idx]);

    // 排行榜
    const { officialRankIds, provinceRankedIds, provinceIdentityIds, doudizhuRankIds } = await collectBoards(page);

    // 好友推荐
    const friendIds = await collectFriends(page, account, FRIEND_ROUNDS);

    // 合并去重
    const allUserIdSet = new Set([...officialRankIds, ...provinceRankedIds, ...provinceIdentityIds, ...doudizhuRankIds, ...friendIds]);
    const allUserIds = [...allUserIdSet];
    console.log(`\n📋 合计去重 UserID: ${allUserIds.length} (官阶${officialRankIds.length} + 省级排位${provinceRankedIds.length} + 省级身份${provinceIdentityIds.length} + 斗地主${doudizhuRankIds.length} + 好友${friendIds.length})`);

    // 查询 GameID
    const gameIdResult = await queryGameIds(page, account, allUserIds);

    await browser.close();

    saveOutput('full', {
        mode:         'full',
        account,
        sources: {
            officialRank:        officialRankIds.length,
            provincial:          provinceRankedIds.length,
            provincialIdentity:  provinceIdentityIds.length,
            doudizhuRank:        doudizhuRankIds.length,
            friendRec:           friendIds.length,
        },
        totalUserIds: allUserIds.length,
        totalGameIds: gameIdResult.totalGameIds,
        keepModes:    KEEP_MODES,
        friendRounds: FRIEND_ROUNDS,
    }, gameIdResult.results, gameIdResult.totalGameIds);
}

// ═══════════════════════════════════════════════════
//  主入口
// ═══════════════════════════════════════════════════

async function main() {
    fs.mkdirSync(GAMEIDS_DIR, { recursive: true });
    console.log(`🔧 运行模式: ${MODE}\n`);

    if (MODE === 'boards')       await runBoards();
    else if (MODE === 'friends') await runFriends();
    else                         await runFull();
}

main().catch(err => {
    console.error('❌ 采集失败：', err.message);
    process.exit(1);
});
