/* ============================================================
 * 星语 · 云端后端补丁（配置落盘 + LLM 转发 + 历史同步）
 * ------------------------------------------------------------
 * 依赖主 <script> 暴露的全局：config / saveConfig / saveChatSessions /
 * saveGroupChats / chatSessions / groupChats / xyState
 *
 * 做三件事：
 *   1. 启动时从后端 /api/config 拉取 AI 配置并回填；saveConfig 时写回云端
 *   2. 拦截 fetch 发往 /chat/completions 的请求，转发到后端 /api/chat，
 *      由后端用云端保存的 key 真正调用上游，响应原样返回（调用方零改动）
 *   3. chatSessions / groupChats 变化时防抖同步到后端 /api/sync 备份
 *
 * 后端地址优先取 window.XY_BACKEND_BASE，其次 xyState.feishu.baseUrl
 * ============================================================ */
(function () {
  function backendBase() {
    try {
      if (window.XY_BACKEND_BASE) return String(window.XY_BACKEND_BASE).replace(/\/+$/, '');
      if (typeof xyState !== 'undefined' && xyState.feishu &&
          xyState.feishu.enabled && xyState.feishu.baseUrl) {
        return String(xyState.feishu.baseUrl).replace(/\/+$/, '');
      }
    } catch (e) {}
    return null;
  }

  /* ---------- 1. 配置落盘 ---------- */
  async function pullConfig() {
    const base = backendBase();
    if (!base) return;
    try {
      const r = await fetch(base + '/api/config', { cache: 'no-store' });
      const j = await r.json();
      if (j && j.ok && j.config && Object.keys(j.config).length) {
        if (typeof config !== 'undefined' && config) {
          Object.assign(config, j.config);
          try { localStorage.setItem('echo_api_config', JSON.stringify(config)); } catch (e) {}
          try {
            const map = { apiBaseUrl: config.baseUrl, apiKey: config.apiKey, apiModel: config.model };
            Object.keys(map).forEach(function (id) {
              const el = document.getElementById(id);
              if (el && map[id]) el.value = map[id];
            });
          } catch (e) {}
          console.log('[xy-backend] 已从云端加载 AI 配置');
        }
      }
    } catch (e) {}
  }

  function pushConfig(cfg) {
    const base = backendBase();
    if (!base) return;
    try {
      fetch(base + '/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ config: cfg || {} })
      }).catch(function () {});
    } catch (e) {}
  }

  if (typeof saveConfig === 'function') {
    const _saveConfig = saveConfig;
    saveConfig = function (cfg) {
      _saveConfig(cfg);
      pushConfig(cfg);
    };
  }

  /* ---------- 2. LLM 转发拦截 ---------- */
  const _fetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    try {
      const url = (typeof input === 'string') ? input : (input && input.url);
      if (url && /\/chat\/completions\/?$/.test(url)) {
        const base = backendBase();
        if (base) {
          let payload = {};
          if (init && init.body) { try { payload = JSON.parse(init.body); } catch (e) {} }
          let apiKey = '';
          try {
            const h = (init && init.headers) || {};
            const auth = h.Authorization || h.authorization;
            if (auth) apiKey = String(auth).replace(/^Bearer\s+/i, '');
          } catch (e) {}
          const ctx = window.xyConvContext || {};
          const body = {
            apiConfig: {
              baseUrl: url.replace(/\/chat\/completions\/?$/, ''),
              apiKey: apiKey,
              model: payload.model || ''
            },
            payload: payload,
            path: '/chat/completions',
            conversationId: ctx.conversationId || 'default',
            type: ctx.type || 'private',
            name: ctx.name || '',
            persist: true
          };
          return _fetch(base + '/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
          });
        }
      }
    } catch (e) {}
    return _fetch(input, init);
  };

  /* ---------- 3. 历史同步（已关闭） ----------
   * 架构决策：完整聊天上下文只留本地 localStorage，不上云，省空间。
   * 云端只存配置/密钥、UID 映射、定时任务与静态资源。
   * 需要时把下面 scheduleHistorySync 接到保存函数即可恢复。
   */
  function scheduleHistorySync() { /* 预留：暂不同步上下文到云端 */ }

  if (typeof saveChatSessions === 'function') {
    const _scs = saveChatSessions;
    saveChatSessions = function () { _scs(); scheduleHistorySync(); };
  }
  if (typeof saveGroupChats === 'function') {
    const _sgc = saveGroupChats;
    saveGroupChats = function () { _sgc(); scheduleHistorySync(); };
  }

  /* ---------- 4. 会话上下文：让转发带上 conversationId ---------- */
  if (typeof generateChatReply === 'function') {
    const _gcr = generateChatReply;
    generateChatReply = function (charId, images, userText, rc) {
      let nm = '';
      try { const c = getCharacter(charId); if (c) nm = c.name || ''; } catch (e) {}
      window.xyConvContext = { conversationId: 'private_' + charId, type: 'private', name: nm };
      return _gcr(charId, images, userText, rc);
    };
  }
  if (typeof generateGroupChatReply === 'function') {
    const _ggr = generateGroupChatReply;
    generateGroupChatReply = function (gid, images, userText) {
      let nm = '';
      try { const g = getGroupChat(gid); if (g) nm = g.name || ''; } catch (e) {}
      window.xyConvContext = { conversationId: 'group_' + gid, type: 'group', name: nm };
      return _ggr(gid, images, userText);
    };
  }

  /* ---------- 启动 ---------- */
  function start() { setTimeout(pullConfig, 800); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();

  window.xyBackendBase = backendBase;
})();
