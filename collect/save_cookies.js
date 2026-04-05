#!/usr/bin/env node
'use strict';

/**
 * Cookie 导出辅助脚本  collect/save_cookies.js
 *
 * 用途：手动登录游戏一次，把 Cookie 保存到本地文件，
 *       再把文件内容复制到 GitHub Secret GAME_COOKIES。
 *
 * 用法：
 *   node collect/save_cookies.js
 *   → 弹出浏览器窗口，手动完成登录
 *   → 按回车，自动保存 Cookie 到 cookies.json
 *   → 把 cookies.json 的内容粘贴到 GitHub Secret GAME_COOKIES
 *
 * 注意：cookies.json 已加入 .gitignore，不要手动提交它。
 */

const puppeteer = require('puppeteer');
const fs        = require('fs');
const path      = require('path');
const readline  = require('readline');

const GAME_URL   = 'https://web.sanguosha.com/10/';
const OUT_PATH   = path.resolve(__dirname, '..', 'cookies.json');

async function main() {
    console.log('🌐 打开浏览器，请手动完成登录...');
    const browser = await puppeteer.launch({
        headless: false,   // 有界面，供手动操作
        defaultViewport: null,
        args: ['--start-maximized'],
    });

    const page = await browser.newPage();
    await page.goto(GAME_URL, { waitUntil: 'domcontentloaded' });

    console.log('');
    console.log('👆 请在弹出的浏览器里完成登录，进入游戏主界面。');
    console.log('   登录完成后，回到这里按 Enter 保存 Cookie...');

    await new Promise(resolve => {
        const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
        rl.question('', () => { rl.close(); resolve(); });
    });

    const cookies = await page.cookies();
    await browser.close();

    if (!cookies.length) {
        console.error('❌ 未获取到任何 Cookie，请确认已成功登录');
        process.exit(1);
    }

    fs.writeFileSync(OUT_PATH, JSON.stringify(cookies, null, 2), 'utf8');
    console.log(`\n✅ 已保存 ${cookies.length} 个 Cookie 到：${OUT_PATH}`);
    console.log('');
    console.log('下一步：把以下内容复制到 GitHub → Settings → Secrets → GAME_COOKIES');
    console.log('─────────────────────────────────────────────');
    console.log(JSON.stringify(cookies));
    console.log('─────────────────────────────────────────────');
    console.log('');
    console.log('⚠️  cookies.json 已加入 .gitignore，不要提交到仓库');
}

main().catch(err => { console.error(err); process.exit(1); });
