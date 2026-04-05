#!/usr/bin/env node
'use strict';

/**
 * SGS 全自动采集脚本  collect/collect.js
 *
 * 采集源：
 *   A. 官阶榜 top 500（rankType=20, 自动分页 5×100）
 *   B. 省级排位榜 34 省 × top 100（rankType=5）
 *   C. 好友推荐"换一批"（CReqFriendRecommend, ~20 人/次, 刷 N 轮）
 *   → 所有去重 UserID 查战绩 → GameID
 *
 * 环境变量：
 *   SGS_ACCOUNTS    登录账号（逗号分隔，按小时轮替）
 *   SGS_PASSWORDS   登录密码（逗号分隔，与账号一一对应）
 *   KEEP_MODES      保留的模式 ID（默认 "8,36"）
 *   PROVINCE_MAX    最大省份 ID（默认 33）
 *   FRIEND_ROUNDS   好友推荐刷新轮数（默认 100，约 2000 人）
 *   REQUEST_DELAY   请求间隔 ms（默认 300）
 *
 * 用法：
 *   SGS_ACCOUNTS=a1,a2 SGS_PASSWORDS=p1,p2 node collect/collect.js
 */

const puppeteer = require('puppeteer');
const fs        = require('fs');
const path      = require('path');

// ─────────────────── 配置 ───────────────────

const LOGIN_URL      = 'https://web.sanguosha.com/login/index.html';
const GAME_URL       = 'https://web.sanguosha.com/10/';
const KEEP_MODES     = (process.env.KEEP_MODES || '8,36').split(',').map(Number);
const PROVINCE_MAX   = parseInt(process.env.PROVINCE_MAX || '33', 10);
const FRIEND_ROUNDS  = parseInt(process.env.FRIEND_ROUNDS || '100', 10);
const DELAY_MS       = parseInt(process.env.REQUEST_DELAY || '300', 10);

const ROOT        = path.resolve(__dirname, '..');
const GAMEIDS_DIR = path.join(ROOT, 'data', 'gameids');

// ─────────────────── cmdId 常量 ───────────────────

const CMD_RANK_LIST          = 3611896190;  // CReqRankList
const CMD_FRIEND_RECOMMEND   = 2818936274;  // CReqFriendRecommend（空 payload）
const CMD_GET_GAME_RECORD    = 1065628532;  // CReqGetNewGameRecord

// ─────────────────── 主流程 ───────────────────

async function main() {
    fs.mkdirSync(GAMEIDS_DIR, { recursive: true });

    // 兼容单数/复数命名，支持中英文逗号，自动去空格
    const rawAccounts  = process.env.SGS_ACCOUNTS  || process.env.SGS_ACCOUNT  || '';
    const rawPasswords = process.env.SGS_PASSWORDS || process.env.SGS_PASSWORD || '';
    const accounts  = rawAccounts.replace(/，/g, ',').split(',').map(s => s.trim()).filter(Boolean);
    const passwords = rawPasswords.replace(/，/g, ',').split(',').map(s => s.trim()).filter(Boolean);
    if (!accounts.length || accounts.length !== passwords.length) {
        console.error('❌ 缺少环境变量 SGS_ACCOUNTS / SGS_PASSWORDS（逗号分隔，数量需一致）');
        console.error(`   当前: accounts=${accounts.length}, passwords=${passwords.length}`);
        process.exit(1);
    }
    // 按当前小时轮替账号
    const hour = new Date().getUTCHours();
    const idx = hour % accounts.length;
    const SGS_ACCOUNT  = accounts[idx];
    const SGS_PASSWORD = passwords[idx];
    console.log(`📋 使用账号 ${idx + 1}/${accounts.length}: ${SGS_ACCOUNT}  (UTC hour=${hour})`);

    // ── 启动浏览器 ──────────────────────────────────────────────
    console.log('🚀 启动无头浏览器...');
    const browser = await puppeteer.launch({
        headless: 'new',
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

    // ── 反检测 ──────────────────────────────────────────────────
    await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    );
    await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en'] });
    });

    // ── 注入采集工具函数 ──────────────────────────────────────────
    await page.evaluateOnNewDocument(() => {
        window.__utils = true;  // 标记已注入

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

        // 收集多个同名响应（官阶榜自动分页 5×100）
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

        const MODE_STR = { MITHuanLeJingJi: 8, MITDouDiZhu: 36, MITBaRenJunZhengZiYou: 4 };
        window.modeToInt = function(m) { return typeof m === 'string' ? (MODE_STR[m] || 0) : (m || 0); };
        window.delay = function(ms) { return new Promise(r => setTimeout(r, ms)); };
    });

    // ── console.log 拦截 ────────────────────────────────────────
    // 游戏实际的 console.log 格式（经诊断确认）：
    //   发送: console.log('%o', '--------[  Sent  ] ID:xxx name:cmsg.XXX detail:', payload)
    //   接收: console.log('%o', '--------[Received] ID:xxx name:', 'cmsg.CRespXXX', 'detail:', payload)
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
                    // 接收: args = ['%o', '--------[Received] ID:xxx name:', 'cmsg.XXX', 'detail:', payload]
                    const name = typeof args[2] === 'string' ? args[2] : '';
                    const payload = args[4];
                    if (name) _dispatch(name, payload);
                } else if (header.includes('[  Sent  ]') || header.includes('[Cached]')) {
                    // 发送: args = ['%o', '--------[  Sent  ] ID:xxx name:cmsg.XXX detail:', payload]
                    const m = header.match(/name:(cmsg\.\w+)/);
                    if (m) _dispatch(m[1], args[2]);
                }
            } catch (_) {}
        };

        // 锁住 console.log，防止游戏覆盖
        Object.defineProperty(console, 'log', {
            get: () => _hook,
            set: () => {},
            configurable: true,
        });
    });

    // ── 登录 ────────────────────────────────────────────────────
    console.log('🌐 打开登录页...');
    await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });

    console.log('✏️  填写账号密码...');
    await page.waitForSelector('#SGS_login-account', { timeout: 10000 });
    await page.type('#SGS_login-account', SGS_ACCOUNT, { delay: 50 });
    await page.type('#SGS_login-password', SGS_PASSWORD, { delay: 50 });

    const agreed = await page.$eval('#SGS_userProto', el => el.checked);
    if (!agreed) await page.click('#SGS_userProto');

    console.log('🔑 点击登录...');
    await page.click('#SGS_login-btn');

    // ── 游戏选择 ────────────────────────────────────────────────
    console.log('🎮 等待游戏选择界面...');
    await page.waitForSelector('#selectGame', { visible: true, timeout: 60000 }).catch(async () => {
        console.error('❌ 登录失败');
        await browser.close();
        process.exit(1);
    });

    console.log('🎮 选择三国杀：一将成名...');
    await page.evaluate(() => {
        const items = document.querySelectorAll('#oL10th .game-item');
        if (items.length >= 3) items[2].click();
    });
    await new Promise(r => setTimeout(r, 500));

    console.log('⏳ 点击进入游戏...');
    await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
        page.click('#goInGameBtn'),
    ]).catch(async () => {
        console.log('   导航未触发，检查 URL...');
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
            console.error('❌ 无法进入游戏页面');
            await browser.close();
            process.exit(1);
        });
    }

    // ── 处理 Laya 弹窗（canvas 坐标点击"开启"） ─────────────────
    console.log('🔇 处理游戏内弹窗...');
    await page.waitForFunction(() => typeof Laya !== 'undefined' && Laya.stage, { timeout: 30000 });
    await new Promise(r => setTimeout(r, 5000));

    await page.evaluate(() => {
        const canvas = document.querySelector('canvas');
        if (!canvas) return;
        const rect = canvas.getBoundingClientRect();
        // 图层 800x450 左下角原点, "开启" at (340, 150) → 左上角 (340, 300)
        const clientX = rect.left + (340 / 800) * rect.width;
        const clientY = rect.top + (300 / 450) * rect.height;
        for (const type of ['mousedown', 'mouseup', 'click']) {
            canvas.dispatchEvent(new MouseEvent(type, {
                clientX, clientY, bubbles: true, cancelable: true, button: 0
            }));
        }
    });
    await new Promise(r => setTimeout(r, 2000));
    // 再点一次
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

    // ── 等待认证 ────────────────────────────────────────────────
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
        const diag = await page.evaluate(() => {
            const cap = window.__cap;
            return {
                msgCount: cap ? cap.msgs.length : -1,
                msgNames: cap ? [...new Set(cap.msgs.map(m => m.name))].slice(0, 20) : [],
            };
        });
        console.error('❌ 认证超时 — 诊断:', JSON.stringify(diag));
        await browser.close();
        process.exit(1);
    }
    console.log('✅ 认证成功');

    // ── 等待 PSC ────────────────────────────────────────────────
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
    console.log('✅ PSC 就绪');

    // ════════════════════════════════════════════════════════════
    //  采集阶段
    // ════════════════════════════════════════════════════════════

    // ── Step 1A：官阶榜 top 500 ─────────────────────────────────
    console.log('\n📊 Step 1A：官阶榜 top 500...');
    const officialIds = await page.evaluate(async (CMD, delayMs) => {
        const seen = new Set();
        // rankType=20(官阶), rangeType=1(全服), modeID=0, provinceID=-1
        // provinceID=-1 需要用有符号编码：用 zigzag? 不，protobuf3 varint 是无符号的
        // 但录像中 provinceID=-1 能工作，可能服务端忽略了这个字段
        // 安全起见不传 provinceID（protobuf3 零值省略）
        const payload = new Uint8Array([
            ...encodeField(1, 20),   // rankType = RLTOfficialRank
            ...encodeField(2, 1),    // rangeType = RLRTTotal
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
    console.log(`   ✅ 官阶榜 ${officialIds.pages} 页, ${officialIds.ids.length} 人`);

    // ── Step 1B：省级排位榜 ─────────────────────────────────────
    console.log(`\n📊 Step 1B：省级排位榜（${PROVINCE_MAX + 1} 个省份）...`);
    const provinceIds = await page.evaluate(async (CMD, provinceMax, delayMs) => {
        const seen = new Set();
        for (let pid = 0; pid <= provinceMax; pid++) {
            const payload = new Uint8Array([
                ...encodeField(1, 5),    // rankType = 排位
                ...encodeField(2, 3),    // rangeType = 省级
                ...encodeField(3, 8),    // modeID = 欢乐竞技
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
    console.log(`   ✅ 省级榜 ${provinceIds.length} 人`);

    // ── Step 1C：好友推荐"换一批" ───────────────────────────────
    console.log(`\n📊 Step 1C：好友推荐（${FRIEND_ROUNDS} 轮 × ~20 人）...`);
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
    }, CMD_FRIEND_RECOMMEND, FRIEND_ROUNDS, DELAY_MS);
    console.log(`   ✅ 好友推荐 ${friendIds.length} 人`);

    // ── 合并去重 UserID ─────────────────────────────────────────
    const allUserIdSet = new Set([...officialIds.ids, ...provinceIds, ...friendIds]);
    const allUserIds = [...allUserIdSet];
    console.log(`\n📋 合计去重 UserID: ${allUserIds.length} (官阶${officialIds.ids.length} + 省级${provinceIds.length} + 好友${friendIds.length})`);

    // ── Step 2：查询 GameID ─────────────────────────────────────
    console.log(`\n🎮 Step 2：查询 ${allUserIds.length} 个玩家的对局记录...`);
    console.log(`   保留模式: [${KEEP_MODES.join(', ')}]`);

    const allGameIds = await page.evaluate(async (userIds, CMD, keepModes, delayMs) => {
        const seen = new Set();
        const results = [];

        for (let i = 0; i < userIds.length; i++) {
            const uid = userIds[i];
            const payload = new Uint8Array(encodeField(1, uid));
            const respPromise = waitForMsg('cmsg.CRespGetNewGameRecord', 8000);
            window.__psc.Send(CMD, payload);

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

            if ((i + 1) % 100 === 0 || i === userIds.length - 1) {
                const pct = ((i + 1) / userIds.length * 100).toFixed(1);
                console.log('[进度] ' + (i+1) + '/' + userIds.length + ' (' + pct + '%) | GameID: ' + seen.size);
            }
            if (i < userIds.length - 1) await delay(delayMs);
        }
        return { results, totalGameIds: seen.size };
    }, allUserIds, CMD_GET_GAME_RECORD, KEEP_MODES, DELAY_MS);

    console.log(`   ✅ 共 ${allGameIds.totalGameIds} 个去重 GameID`);

    await browser.close();

    // ── 保存 ────────────────────────────────────────────────────
    const now     = new Date();
    const today   = now.toISOString().slice(0, 10);
    const timeTag = now.toISOString().slice(11, 16).replace(':', '');  // HHMM
    const outPath = path.join(GAMEIDS_DIR, `${today}_${timeTag}.json`);
    const outData = {
        metadata: {
            date:         today,
            time:         now.toISOString(),
            account:      SGS_ACCOUNT,
            sources: {
                officialRank: officialIds.ids.length,
                provincial:   provinceIds.length,
                friendRec:    friendIds.length,
            },
            totalUserIds: allUserIds.length,
            totalGameIds: allGameIds.totalGameIds,
            keepModes:    KEEP_MODES,
            friendRounds: FRIEND_ROUNDS,
        },
        results: allGameIds.results,
    };
    fs.writeFileSync(outPath, JSON.stringify(outData, null, 2), 'utf8');
    console.log(`\n💾 已保存：${outPath}`);
    console.log(`   ${allUserIds.length} 个 UserID → ${allGameIds.totalGameIds} 个 GameID`);
}

main().catch(err => {
    console.error('❌ 采集失败：', err.message);
    process.exit(1);
});
