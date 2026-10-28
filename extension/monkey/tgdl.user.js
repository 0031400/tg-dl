// ==UserScript==
// @name         tgdl 下载器（Telegram Web）
// @namespace    tgdl
// @version      1.0.3
// @description  在 Telegram Web 的视频/文件/图片消息下加「下载」按钮，交给本地 tgdl 服务用多账号并行下载
// @match        https://web.telegram.org/a/*
// @match        https://web.telegram.org/k/*
// @grant        GM_xmlhttpRequest
// @grant        GM_getValue
// @grant        GM_setValue
// @connect      127.0.0.1
// @connect      localhost
// @run-at       document-idle
// @noframes
// ==/UserScript==

(function () {
  'use strict';

  // ======================= 配置 =======================
  const DEBUG = false; // 打开后会在控制台打印 DOM 探测结果，便于适配新版本

  const store = {
    get: (k, d) => (typeof GM_getValue === 'function' ? GM_getValue(k, d) : JSON.parse(localStorage.getItem('tgdl_' + k) ?? 'null') ?? d),
    set: (k, v) => (typeof GM_setValue === 'function' ? GM_setValue(k, v) : localStorage.setItem('tgdl_' + k, JSON.stringify(v))),
  };

  const cfg = {
    server: store.get('server', 'http://127.0.0.1:8765'),
    token: store.get('token', ''),
    autoDownload: store.get('autoDownload', true),
  };

  // ======================= HTTP =======================
  function api(path, body) {
    const url = cfg.server.replace(/\/+$/, '') + path;
    return new Promise((resolve, reject) => {
      const req = {
        method: body ? 'POST' : 'GET',
        url,
        headers: { 'X-Tgdl-Token': cfg.token },
        timeout: 15000,
        onload: (res) => {
          let data = null;
          try { data = JSON.parse(res.responseText); } catch (e) { /* ignore */ }
          if (res.status >= 400) reject(new Error((data && data.error) || `HTTP ${res.status}`));
          else resolve(data);
        },
        onerror: () => reject(new Error('连不上本地服务，先运行 uv run tgdl serve')),
        ontimeout: () => reject(new Error('本地服务响应超时')),
      };
      if (body) {
        req.data = JSON.stringify(body);
        req.headers['Content-Type'] = 'application/json';
      }
      if (typeof GM_xmlhttpRequest === 'function') GM_xmlhttpRequest(req);
      else fetch(url, { method: req.method, headers: req.headers, body: req.data })
        .then((r) => r.json().then((j) => (r.ok ? resolve(j) : reject(new Error(j.error || r.status)))))
        .catch(reject);
    });
  }

  // ======================= 从页面提取 chat_id / message_id =======================
  function hashChatId() {
    const h = decodeURIComponent(location.hash.replace(/^#/, ''));
    if (!h) return null;
    if (/^-?\d+$/.test(h)) return Number(h);
    return null; // @username 形式，交给服务端解析
  }

  function hashUsername() {
    const h = decodeURIComponent(location.hash.replace(/^#/, ''));
    return h.startsWith('@') ? h.slice(1) : null;
  }

  const chatCache = new Map();
  async function currentChatId() {
    const direct = hashChatId();
    if (direct) return direct;
    const username = hashUsername();
    if (!username) return null;
    if (chatCache.has(username)) return chatCache.get(username);
    try {
      const res = await api('/api/resolve', { username });
      if (res && res.chat_id) {
        chatCache.set(username, res.chat_id);
        return res.chat_id;
      }
    } catch (e) {
      if (DEBUG) console.warn('[tgdl] 解析 username 失败', e);
    }
    return null;
  }

  // /a(webk) 实测：<div class="Message message-list-item" data-message-id="6733">
  function messageIdOf(el) {
    const v = el.getAttribute('data-message-id') || el.getAttribute('data-mid');
    return v && /^\d+$/.test(v) ? Number(v) : null;
  }

  // 媒体消息一定有 .media-inner；文件/音频另走一套容器
  function isMediaMessage(el) {
    return !!el.querySelector('.media-inner, .Document, .document, .Audio, .audio-player');
  }

  function messageElements() {
    return Array.from(document.querySelectorAll('.Message[data-message-id]'));
  }

  function mediaHost(el) {
    return el.querySelector('.media-inner')
      || el.querySelector('.Document, .document, .Audio')
      || el.querySelector('.message-content')
      || el;
  }

  // ======================= 按钮注入 =======================
  function makeButton(el, chatId, messageId) {
    const btn = document.createElement('button');
    btn.className = 'tgdl-chip';
    btn.textContent = '⬇ tgdl';
    btn.title = `下载 ${chatId}:${messageId}`;
    btn.addEventListener('click', async (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      btn.disabled = true;
      btn.textContent = '…';
      try {
        await api('/api/tasks', { target: `${chatId}:${messageId}` });
        btn.textContent = '✓ 已加入';
        setTimeout(() => { btn.textContent = '⬇ tgdl'; btn.disabled = false; }, 2000);
        refreshState();
      } catch (e) {
        btn.textContent = '✗ 失败';
        btn.title = String(e.message || e);
        setTimeout(() => { btn.textContent = '⬇ tgdl'; btn.disabled = false; }, 2500);
      }
    });
    return btn;
  }

  let scanning = false;
  async function ensureButtons() {
    if (scanning) return;
    scanning = true;
    try {
      const chatId = await currentChatId();
      if (!chatId) return;
      for (const el of messageElements()) {
        if (el.querySelector('.tgdl-chip')) continue;
        if (!isMediaMessage(el)) continue;
        const mid = messageIdOf(el);
        if (!mid) continue;

        const host = mediaHost(el);
        if (getComputedStyle(host).position === 'static') host.style.position = 'relative';
        const btn = makeButton(el, chatId, mid);
        host.appendChild(btn);
        if (DEBUG) console.log('[tgdl] 注入', chatId, mid, el.className);
      }
    } finally {
      scanning = false;
    }
  }

  // ======================= 面板 =======================
  const CSS = `
  :host { all: initial; }
  .panel {
    position: fixed; right: 16px; bottom: 16px; width: 340px; max-height: 70vh;
    display: flex; flex-direction: column; z-index: 2147483000;
    font: 13px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
    color: #111; background: #fff; border: 1px solid rgba(0,0,0,.12);
    border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,.18); overflow: hidden;
  }
  .head { display: flex; align-items: center; gap: 8px; padding: 8px 10px; background: #f6f6f4; border-bottom: 1px solid rgba(0,0,0,.08); cursor: move; }
  .head b { font-weight: 600; font-size: 13px; flex: 1; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: #bbb; }
  .dot.on { background: #1a9e5f; } .dot.off { background: #d24b4b; }
  .tabs { display: flex; border-bottom: 1px solid rgba(0,0,0,.08); }
  .tabs button { flex: 1; padding: 7px 0; background: none; border: 0; cursor: pointer; font: inherit; color: #666; }
  .tabs button.sel { color: #111; font-weight: 600; box-shadow: inset 0 -2px 0 #111; }
  .body { padding: 10px; overflow: auto; }
  .task { border: 1px solid rgba(0,0,0,.08); border-radius: 8px; padding: 8px; margin-bottom: 8px; }
  .task .n { font-weight: 500; word-break: break-all; }
  .task .m { color: #777; font-size: 12px; margin-top: 2px; }
  .bar { height: 4px; background: rgba(0,0,0,.08); border-radius: 2px; margin-top: 6px; overflow: hidden; }
  .bar i { display: block; height: 100%; background: #1a9e5f; width: 0; }
  .row { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
  .row label { width: 92px; color: #555; flex: none; }
  .row input[type=text], .row input[type=password], .row input[type=number] {
    flex: 1; min-width: 0; padding: 5px 7px; border: 1px solid rgba(0,0,0,.15); border-radius: 6px; font: inherit;
  }
  .row input[type=checkbox] { width: 16px; height: 16px; }
  button.pri { padding: 6px 12px; border-radius: 6px; border: 0; background: #111; color: #fff; font: inherit; cursor: pointer; }
  button.pri:disabled { opacity: .5; cursor: default; }
  button.ghost { padding: 4px 8px; border-radius: 6px; border: 1px solid rgba(0,0,0,.15); background: #fff; font: inherit; cursor: pointer; }
  .acts { display: flex; gap: 6px; margin-top: 8px; }
  .acts button { padding: 3px 9px; border-radius: 6px; border: 1px solid rgba(0,0,0,.15); background: #fff; font: inherit; font-size: 12px; cursor: pointer; }
  .acts button:hover { background: #f2f2f2; }
  .acts button:disabled { opacity: .5; cursor: default; }
  .hint { color: #777; font-size: 12px; }
  .err { color: #c0392b; font-size: 12px; }
  `;

  // 角标注入在页面 DOM 里（不是 shadow root），所以样式必须放到页面去
  const CHIP_CSS = `
  .tgdl-chip {
    position: absolute !important; top: 6px !important; right: 6px !important;
    z-index: 10 !important; display: inline-flex !important; align-items: center !important;
    padding: 3px 8px !important; border-radius: 10px !important; border: 0 !important;
    cursor: pointer !important; line-height: 1.3 !important;
    font: 11px/1.3 system-ui, -apple-system, "Segoe UI", sans-serif !important;
    color: #fff !important; background: rgba(0,0,0,.62) !important;
    opacity: .75; transition: opacity .15s, background .15s !important;
    box-shadow: 0 1px 4px rgba(0,0,0,.35) !important;
    backdrop-filter: blur(2px);
  }
  .Message:hover .tgdl-chip, .tgdl-chip:hover { opacity: 1 !important; background: rgba(0,0,0,.85) !important; }
  .tgdl-chip:disabled { cursor: default !important; }
  `;

  function injectChipStyle() {
    if (document.getElementById('tgdl-chip-style')) return;
    const el = document.createElement('style');
    el.id = 'tgdl-chip-style';
    el.textContent = CHIP_CSS;
    (document.head || document.documentElement).appendChild(el);
  }

  let shadow, panel, stateTimer = null, lastState = null;

  function buildPanel() {
    const host = document.createElement('div');
    host.id = 'tgdl-host';
    document.body.appendChild(host);
    shadow = host.attachShadow({ mode: 'open' });
    const style = document.createElement('style');
    style.textContent = CSS;
    shadow.appendChild(style);

    panel = document.createElement('div');
    panel.className = 'panel';
    panel.innerHTML = `
      <div class="head">
        <span class="dot" id="dot"></span>
        <b>tgdl</b>
        <button class="ghost" id="hide">收起</button>
      </div>
      <div class="tabs">
        <button data-tab="tasks" class="sel">下载</button>
        <button data-tab="settings">设置</button>
      </div>
      <div class="body" id="body"></div>
    `;
    shadow.appendChild(panel);

    panel.querySelectorAll('.tabs button').forEach((b) => {
      b.addEventListener('click', () => {
        panel.querySelectorAll('.tabs button').forEach((x) => x.classList.remove('sel'));
        b.classList.add('sel');
        render(b.dataset.tab);
      });
    });
    panel.querySelector('#hide').addEventListener('click', () => {
      panel.style.display = 'none';
      const fab = document.createElement('button');
      fab.className = 'ghost';
      fab.textContent = 'tgdl';
      fab.style.cssText = 'position:fixed;right:16px;bottom:16px;z-index:2147483000;border-radius:20px;padding:8px 14px;box-shadow:0 4px 16px rgba(0,0,0,.2)';
      fab.onclick = () => { panel.style.display = 'flex'; fab.remove(); };
      shadow.appendChild(fab);
    });

    // 拖动
    const head = panel.querySelector('.head');
    let drag = null;
    head.addEventListener('mousedown', (e) => {
      drag = { x: e.clientX, y: e.clientY, r: panel.getBoundingClientRect() };
      e.preventDefault();
    });
    window.addEventListener('mousemove', (e) => {
      if (!drag) return;
      panel.style.left = drag.r.left + (e.clientX - drag.x) + 'px';
      panel.style.top = drag.r.top + (e.clientY - drag.y) + 'px';
      panel.style.right = 'auto';
      panel.style.bottom = 'auto';
    });
    window.addEventListener('mouseup', () => { drag = null; });

    render('tasks');
  }

  function fmtSize(n) {
    if (!n) return '0 B';
    const u = ['B', 'KB', 'MB', 'GB'];
    let i = 0;
    while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
    return n.toFixed(i ? 1 : 0) + ' ' + u[i];
  }

  async function refreshState() {
    try {
      lastState = await api('/api/state');
      const dot = shadow.querySelector('#dot');
      dot.className = 'dot on';
      dot.title = '本地服务已连接';
      const active = panel.querySelector('.tabs button.sel')?.dataset.tab;
      if (active) render(active);
      return lastState;
    } catch (e) {
      const dot = shadow.querySelector('#dot');
      dot.className = 'dot off';
      dot.title = String(e.message || e);
      return null;
    }
  }

  function render(tab) {
    const body = shadow.querySelector('#body');
    if (!body) return;
    // 切换标签时必须清掉 sig，否则 renderTasks 会以为「任务没变」而跳过重建，
    // body 里留着上一个标签的内容（表现为切回下载页没反应）。
    if (body.dataset.tab !== tab) {
      body.dataset.tab = tab;
      body.dataset.sig = '';
      body.innerHTML = '';
    }
    if (tab === 'tasks') return renderTasks(body);
    if (tab === 'settings') return renderSettings(body);
  }

  const STATUS_TEXT = {
    queued: '排队中', running: '下载中', stopping: '正在停止',
    stopped: '已停止', done: '已完成', failed: '失败',
  };

  function esc(t) {
    return String(t == null ? '' : t).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  }

  function renderTasks(body) {
    const tasks = (lastState && lastState.tasks) || [];
    if (!tasks.length) {
      if (body.dataset.sig !== 'empty') {
        body.dataset.sig = 'empty';
        body.innerHTML = `<div class="hint">还没有任务。<br>在聊天里的图片/视频/文件上点「⬇ tgdl」。</div>`;
      }
      return;
    }
    // 只有任务集合变了才重建 DOM，否则只改文字和进度条（避免闪烁）
    const sig = tasks.map((t) => t.id).join(',');
    if (body.dataset.sig !== sig) {
      body.dataset.sig = sig;
      body.innerHTML = tasks.map((t) => `
        <div class="task" data-id="${t.id}">
          <div class="n">${esc(t.name || t.target)}</div>
          <div class="m"><span class="st"></span> · <span class="mt"></span></div>
          <div class="bar"><i></i></div>
          <div class="acts">
            <button class="ghost" data-act="cancel" data-id="${t.id}">停止</button>
            <button class="ghost" data-act="resume" data-id="${t.id}">继续</button>
            <button class="ghost" data-act="remove" data-id="${t.id}">删除</button>
          </div>
        </div>`).join('');
      body.querySelectorAll('button[data-act]').forEach((b) => {
        b.addEventListener('click', onTaskAction);
      });
    }
    for (const t of tasks) {
      const node = body.querySelector(`.task[data-id="${t.id}"]`);
      if (!node) continue;
      const pct = t.size ? Math.min(100, (t.done / t.size) * 100) : 0;
      node.querySelector('.st').textContent = STATUS_TEXT[t.status] || t.status;
      const speed = t.speed > 0 ? ` · ${fmtSize(t.speed)}/s` : '';
      const meta = t.status === 'done'
        ? fmtSize(t.size)
        : t.status === 'failed'
          ? esc(t.error || '失败')
          : `${fmtSize(t.done)} / ${fmtSize(t.size)}${speed}`;
      node.querySelector('.mt').textContent = meta;
      node.querySelector('.bar i').style.width = pct + '%';
      node.querySelector('.bar i').style.background = t.status === 'failed' ? '#d24b4b' : '#1a9e5f';
      node.querySelector('[data-act="cancel"]').style.display = (t.status === 'running' || t.status === 'queued') ? '' : 'none';
      node.querySelector('[data-act="resume"]').style.display = (t.status === 'stopped' || t.status === 'failed') ? '' : 'none';
    }
  }

  async function onTaskAction(ev) {
    const btn = ev.currentTarget;
    const { id, act } = btn.dataset;
    btn.disabled = true;
    try {
      await api('/api/tasks/' + act, { id });
      await refreshState();
    } catch (e) {
      btn.title = String(e.message || e);
    } finally {
      btn.disabled = false;
    }
  }

  function renderSettings(body) {
    // 轮询时不要重建：否则会把用户正在输入的内容和提示信息清掉
    if (body.dataset.sig === 'settings') return;
    body.dataset.sig = 'settings';
    const st = (lastState && lastState.settings) || {};
    body.innerHTML = `
      <div class="row"><label>服务地址</label><input type="text" id="srv" value="${cfg.server}"></div>
      <div class="row"><label>token</label><input type="password" id="tok" value="${cfg.token}"></div>
      <div class="row"><label>下载目录</label><input type="text" id="dir" value="${st.download_dir || ''}"></div>
      <div class="row"><label>并行请求</label><input type="number" id="conn" min="1" max="64" value="${st.connections || 16}"></div>
      <div class="row"><label>auth key 数</label><input type="number" id="keys" min="1" max="8" value="${st.auth_keys || 3}"></div>
      <div class="row"><label>固定并发</label><input type="checkbox" id="fixed" ${st.fixed ? 'checked' : ''}></div>
      <div class="row"><label></label>
        <button class="pri" id="save">保存</button>
        <button class="ghost" id="test">测试连接</button>
      </div>
      <div class="hint" id="msg"></div>
    `;
    body.querySelector('#test').addEventListener('click', async () => {
      const msg = body.querySelector('#msg');
      msg.textContent = '测试中…';
      cfg.server = body.querySelector('#srv').value.trim() || cfg.server;
      cfg.token = body.querySelector('#tok').value.trim();
      store.set('server', cfg.server);
      store.set('token', cfg.token);
      try {
        const r = await api('/api/ping');
        const acc = await api('/api/account/refresh', {});
        const who = acc.account && acc.account.fast;
        const err = acc.account && acc.account.error;
        msg.innerHTML = err
          ? `<span class="err">服务在跑，但连不上 Telegram：${err}</span>`
          : `<span style="color:#1a9e5f">连上了，服务版本 ${r.version}｜账号 ${who || '未登录'}</span>`;
        refreshState();
      } catch (e) {
        msg.innerHTML = `<span class="err">${e.message || e}</span>`;
      }
    });
    body.querySelector('#save').addEventListener('click', async () => {
      const msg = body.querySelector('#msg');
      cfg.server = body.querySelector('#srv').value.trim() || cfg.server;
      cfg.token = body.querySelector('#tok').value.trim();
      store.set('server', cfg.server);
      store.set('token', cfg.token);
      try {
        await api('/api/settings', {
          download_dir: body.querySelector('#dir').value.trim(),
          connections: Number(body.querySelector('#conn').value),
          auth_keys: Number(body.querySelector('#keys').value),
          fixed: body.querySelector('#fixed').checked,
        });
        msg.textContent = '已保存';
        refreshState();
      } catch (e) {
        msg.innerHTML = `<span class="err">${e.message || e}</span>`;
      }
    });
  }

  // ======================= 启动 =======================
  function boot() {
    injectChipStyle();
    buildPanel();
    if (typeof GM_xmlhttpRequest !== 'function') {
      const tip = document.createElement('div');
      tip.className = 'hint';
      tip.style.cssText = 'padding:6px 10px;background:#fff3cd;color:#7a5b00;border-bottom:1px solid rgba(0,0,0,.08)';
      tip.textContent = '未检测到油猴 API：请用 Tampermonkey 安装本脚本，否则连不上本地服务。';
      shadow.querySelector('.panel').insertBefore(tip, shadow.querySelector('.tabs'));
    }
    refreshState();
    const poll = async () => {
      if (panel.style.display !== 'none') await refreshState();
      const busy = lastState && lastState.tasks.some((t) => t.active);
      stateTimer = setTimeout(poll, busy ? 700 : 3000);
    };
    stateTimer = setTimeout(poll, 0);

    let scheduled = false;
    const schedule = () => {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(() => {
        scheduled = false;
        ensureButtons();
      });
    };
    new MutationObserver(schedule).observe(document.body, { childList: true, subtree: true });
    setInterval(ensureButtons, 2000);

    if (DEBUG) {
      console.log('[tgdl] 页面上下文', {
        hash: location.hash,
        chatId: hashChatId(),
        username: hashUsername(),
        messages: messageElements().slice(0, 5).map((el) => ({
          cls: el.className,
          mid: messageIdOf(el),
          media: isMediaMessage(el),
        })),
      });
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
