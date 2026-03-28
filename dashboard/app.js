const summaryCards = document.getElementById("summaryCards");
const agentGrid = document.getElementById("agentGrid");
const contentRows = document.getElementById("contentRows");
const metricRows = document.getElementById("metricRows");
const trendStats = document.getElementById("trendStats");
const errorRows = document.getElementById("errorRows");
const accountRows = document.getElementById("accountRows");

const refreshBtn = document.getElementById("refreshBtn");
const pullFeishuBtn = document.getElementById("pullFeishuBtn");
const queryErrorsBtn = document.getElementById("queryErrorsBtn");
const saveIpConfigBtn = document.getElementById("saveIpConfigBtn");
const saveSoulBtn = document.getElementById("saveSoulBtn");
const saveRuntimeConfigBtn = document.getElementById("saveRuntimeConfigBtn");
const startLoginBtn = document.getElementById("startLoginBtn");

const runIdInput = document.getElementById("runIdInput");
const ipConfigEditor = document.getElementById("ipConfigEditor");
const soulEditor = document.getElementById("soulEditor");
const runtimeConfigEditor = document.getElementById("runtimeConfigEditor");
const loginProfileInput = document.getElementById("loginProfileInput");
const loginStatusText = document.getElementById("loginStatusText");
const loginQrImage = document.getElementById("loginQrImage");
const lastUpdated = document.getElementById("lastUpdated");

let currentLoginSessionId = "";
let loginPollTimer = null;

function card(label, value) {
  return `<div class="card"><div class="k">${label}</div><div class="v">${value}</div></div>`;
}

function safe(v) {
  return v === null || v === undefined ? "" : String(v);
}

function statusDot(state) {
  if (state === "running") return "state-running";
  if (state === "error") return "state-error";
  return "state-idle";
}

async function fetchJson(url, options) {
  const res = await fetch(url, options);
  const data = await res.json();
  return data;
}

async function loadSummary() {
  const data = await fetchJson("/api/dashboard/summary");
  if (!data.ok) return;
  const s = data.summary;
  summaryCards.innerHTML = [
    card("内容总数", s.total_content),
    card("待审核", s.pending_review),
    card("已发布", s.published),
    card("失败", s.failed),
    card("死信任务", s.dead_jobs),
    card("通过率", `${Math.round(Number(s.pass_rate || 0) * 100)}%`),
    card("Token", s.token_usage_total || 0),
  ].join("");
}

async function loadAgents() {
  const data = await fetchJson("/api/dashboard/agents");
  if (!data.ok) return;
  agentGrid.innerHTML = data.items.map((a) => `
    <article class="agent-item">
      <div><span class="state-dot ${statusDot(a.state)}"></span>${safe(a.agent)}</div>
      <div>状态: ${safe(a.last_status)}</div>
      <div>运行中: ${safe(a.running_count)}</div>
      <div>更新时间: ${safe(a.last_updated_at)}</div>
    </article>
  `).join("");
}

async function loadAccounts() {
  const data = await fetchJson("/api/publish/accounts");
  if (!data.ok) return;
  const items = data.items || [];
  if (accountRows) {
    accountRows.innerHTML = items.map((x) => `
      <tr>
        <td>${safe(x.profile)}</td>
        <td>${safe(x.status)}</td>
        <td>${safe(x.updated_at)}</td>
        <td><button class="inline-btn" data-action="remove-account" data-profile="${safe(x.profile)}">删除</button></td>
      </tr>
    `).join("");
  }
}

async function loadContent() {
  const data = await fetchJson("/api/content");
  if (!data.ok) return;
  contentRows.innerHTML = data.items.map((x) => `
    <tr>
      <td>${safe(x.id)}</td>
      <td>${safe(x.platform)}</td>
      <td>${safe(x.author)}</td>
      <td>${safe(x.status)}</td>
      <td>${safe(x.source)}</td>
      <td>${safe(x.updated_at)}</td>
    </tr>
  `).join("");
}

async function loadMetrics() {
  const data = await fetchJson("/api/dashboard/metrics");
  if (!data.ok) return;
  const items = data.items || [];
  const sum = data.summary || {};

  trendStats.innerHTML = [
    card("累计播放", sum.plays || 0),
    card("累计点赞", sum.likes || 0),
    card("累计评论", sum.comments || 0),
    card("采样条数", sum.samples || items.length),
    card("通过率", `${Math.round(Number(sum.pass_rate || 0) * 100)}%`),
    card("Token", sum.token_usage_total || 0),
  ].join("");

  metricRows.innerHTML = items.slice(-50).reverse().map((x) => `
    <tr>
      <td>${safe(x.content_id)}</td>
      <td>${safe(x.platform)}</td>
      <td>${safe(x.plays)}</td>
      <td>${safe(x.likes)}</td>
      <td>${safe(x.comments)}</td>
      <td>${safe(x.captured_at)}</td>
    </tr>
  `).join("");
}

async function loadErrors() {
  const runId = runIdInput ? runIdInput.value.trim() : "";
  const q = runId ? `?run_id=${encodeURIComponent(runId)}` : "";
  const data = await fetchJson(`/api/dashboard/errors${q}`);
  if (!data.ok) return;
  errorRows.innerHTML = data.items.map((x) => `
    <tr>
      <td>${safe(x.run_id)}</td>
      <td>${safe(x.agent)}</td>
      <td>${safe(x.status)}</td>
      <td>${safe(x.error_code)}</td>
      <td>${safe(x.updated_at)}</td>
    </tr>
  `).join("");
}

async function loadConfig() {
  const ipData = await fetchJson("/api/config/ip");
  if (ipData.ok && ipConfigEditor) {
    ipConfigEditor.value = JSON.stringify(ipData.config, null, 2);
  }
  const soulData = await fetchJson("/api/config/soul");
  if (soulData.ok && soulEditor) {
    soulEditor.value = soulData.content || "";
  }
  const runtimeData = await fetchJson("/api/config/runtime");
  if (runtimeData.ok && runtimeConfigEditor) {
    runtimeConfigEditor.value = JSON.stringify(runtimeData.config, null, 2);
  }
}

async function saveIpConfig() {
  if (!ipConfigEditor) return;
  let parsed;
  try {
    parsed = JSON.parse(ipConfigEditor.value);
  } catch (e) {
    alert("ip-config JSON 格式错误");
    return;
  }
  const data = await fetchJson("/api/config/ip", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ config: parsed }),
  });
  alert(data.ok ? "ip-config 保存成功" : `保存失败: ${safe(data.error)}`);
}

async function saveSoul() {
  if (!soulEditor) return;
  const data = await fetchJson("/api/config/soul", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content: soulEditor.value }),
  });
  alert(data.ok ? "SOUL 保存成功" : `保存失败: ${safe(data.error)}`);
}

async function saveRuntimeConfig() {
  if (!runtimeConfigEditor) return;
  let parsed;
  try {
    parsed = JSON.parse(runtimeConfigEditor.value);
  } catch (e) {
    alert("runtime-config JSON 格式错误");
    return;
  }
  const data = await fetchJson("/api/config/runtime", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ config: parsed }),
  });
  alert(data.ok ? "runtime-config 保存成功" : `保存失败: ${safe(data.error)}`);
}

async function pullFeishu() {
  const data = await fetchJson("/api/collab/feishu/pull", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ limit: 100 }),
  });
  if (!data.ok) {
    alert(`飞书回流失败: ${safe(data.error)}`);
    return;
  }
  alert(`飞书回流完成: 匹配 ${safe(data.matched)} 条，更新脚本 ${safe(data.script_updates)} 条，处理审核 ${safe(data.review_updates)} 条`);
  await refreshAll();
}

async function startPublishLogin() {
  const profile = (loginProfileInput && loginProfileInput.value.trim()) || "default";
  const data = await fetchJson("/api/publish/accounts/login/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ profile, wait_seconds: 300 }),
  });
  if (!data.ok) {
    alert(`扫码登录启动失败: ${safe(data.error)}`);
    return;
  }

  const result = data.result || {};
  const status = result.status || "pending";
  if (loginStatusText) loginStatusText.textContent = `状态: ${status}`;

  if (result.qr_image_base64 && loginQrImage) {
    loginQrImage.src = `data:image/png;base64,${result.qr_image_base64}`;
    loginQrImage.style.display = "block";
  } else if (loginQrImage) {
    loginQrImage.style.display = "none";
  }

  currentLoginSessionId = result.session_id || "";
  if (status === "pending" && currentLoginSessionId) {
    if (loginPollTimer) clearInterval(loginPollTimer);
    loginPollTimer = setInterval(pollPublishLoginStatus, 2000);
  } else {
    await loadAccounts();
  }
}

async function pollPublishLoginStatus() {
  if (!currentLoginSessionId) return;
  const data = await fetchJson(`/api/publish/accounts/login/status?session_id=${encodeURIComponent(currentLoginSessionId)}`);
  if (!data.ok) {
    if (loginPollTimer) {
      clearInterval(loginPollTimer);
      loginPollTimer = null;
    }
    return;
  }
  const result = data.result || {};
  if (loginStatusText) {
    const suffix = result.error ? ` (${result.error})` : "";
    loginStatusText.textContent = `状态: ${safe(result.status)}${suffix}`;
  }
  if (result.status !== "pending") {
    if (loginPollTimer) {
      clearInterval(loginPollTimer);
      loginPollTimer = null;
    }
    if (loginQrImage) loginQrImage.style.display = "none";
    await loadAccounts();
  }
}

async function removePublishAccount(profile) {
  const ok = window.confirm(`确认删除账号 profile=${profile} 的登录态？`);
  if (!ok) return;
  const data = await fetchJson("/api/publish/accounts/remove", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ profile }),
  });
  if (!data.ok) {
    alert(`删除失败: ${safe(data.error)}`);
    return;
  }
  await loadAccounts();
}

async function refreshAll() {
  await Promise.all([loadSummary(), loadAgents(), loadAccounts(), loadContent(), loadMetrics(), loadErrors()]);
  lastUpdated.textContent = `更新: ${new Date().toLocaleTimeString()}`;
}

if (refreshBtn) refreshBtn.addEventListener("click", refreshAll);
if (queryErrorsBtn) queryErrorsBtn.addEventListener("click", loadErrors);
if (saveIpConfigBtn) saveIpConfigBtn.addEventListener("click", saveIpConfig);
if (saveSoulBtn) saveSoulBtn.addEventListener("click", saveSoul);
if (saveRuntimeConfigBtn) saveRuntimeConfigBtn.addEventListener("click", saveRuntimeConfig);
if (pullFeishuBtn) pullFeishuBtn.addEventListener("click", pullFeishu);
if (startLoginBtn) startLoginBtn.addEventListener("click", startPublishLogin);
if (accountRows) {
  accountRows.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.action === "remove-account") {
      await removePublishAccount(target.dataset.profile || "");
    }
  });
}

setInterval(refreshAll, 10000);
loadConfig();
refreshAll();
