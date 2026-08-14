const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const state = {
  token: localStorage.getItem("ruyi.gatewayToken") || "",
  rootId: localStorage.getItem("ruyi.teamRootId") || "",
  tasks: new Map(),
  selectedTaskId: null,
  poller: null,
};

const tokenInput = $("#token");
const rootInput = $("#root-id");
tokenInput.value = state.token;
rootInput.value = state.rootId;

function now() { return new Date().toLocaleTimeString([], {hour12: false}); }
function logEvent(kind, label, payload) {
  const li = document.createElement("li");
  const body = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
  li.innerHTML = `<time>${now()}</time><strong>${kind}</strong><code></code>`;
  $("code", li).textContent = `${label}\n${body}`;
  $("#event-log").prepend(li);
}
function toast(message) {
  const el = $("#toast"); el.textContent = message; el.classList.add("visible");
  clearTimeout(toast.timer); toast.timer = setTimeout(() => el.classList.remove("visible"), 2800);
}
function setConnection(label, type = "neutral") {
  const el = $("#connection-state"); el.textContent = label; el.className = `status ${type}`;
}

async function api(path, options = {}) {
  const token = tokenInput.value.trim();
  const headers = {"Content-Type": "application/json", ...(options.headers || {})};
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch(path, {...options, headers});
  let payload;
  try { payload = await response.json(); } catch { payload = await response.text(); }
  logEvent(response.ok ? "HTTP" : "ERROR", `${options.method || "GET"} ${path} → ${response.status}`, payload);
  if (!response.ok) {
    const message = payload?.error?.message || payload?.detail || `HTTP ${response.status}`;
    throw new Error(typeof message === "string" ? message : JSON.stringify(message));
  }
  return payload;
}

function statusClass(status) {
  return ["running", "completed", "failed", "cancelled", "interrupted"].includes(status) ? status : "neutral";
}
function fillStatus(el, status) { el.textContent = status || "unknown"; el.className = `status ${statusClass(status)}`; }
function formatDate(value) { return value ? new Date(value).toLocaleString() : "—"; }

function renderRoot(root) {
  const values = [root?.task_id || "尚未创建", root?.run_count ?? "—", root?.status || "—", formatDate(root?.updated_at)];
  $$("#root-meta dd").forEach((el, index) => { el.textContent = values[index]; });
  $("#cancel-root").disabled = !root || root.status !== "running";
  const moderator = $("#moderator-result");
  moderator.textContent = root?.last_result || (root?.error ? `运行失败：${root.error}` : "主持人正在组织独立分析与分歧质询…");
  moderator.classList.toggle("empty", !root?.last_result && !root?.error);
}

function renderArchitect(agentName, task) {
  const sheet = $(`.architect-sheet[data-agent="${agentName}"]`);
  fillStatus($(".status", sheet), task?.status || "未创建");
  $(".task-id", sheet).textContent = task?.task_id || "等待主持人委派";
  $(".run-count", sheet).textContent = task ? `Run ${task.run_count}` : "Run —";
  const result = $(".result", sheet);
  result.textContent = task?.last_result || (task?.error ? `运行失败：${task.error}` : task ? "Agent 正在撰写或回应质询…" : "独立方案将在这里出现。");
  result.classList.toggle("empty", !task?.last_result && !task?.error);
  const button = $(".open-session", sheet); button.disabled = !task; button.dataset.taskId = task?.task_id || "";
}

function render(tasks) {
  state.tasks = new Map(tasks.map(task => [task.task_id, task]));
  const root = state.tasks.get(state.rootId);
  renderRoot(root);
  for (const agent of ["architect_codex", "architect_deepseek"]) {
    const matches = tasks.filter(task => task.agent_name === agent).sort((a,b) => new Date(b.updated_at) - new Date(a.updated_at));
    renderArchitect(agent, matches[0]);
  }
  const children = tasks.filter(task => task.parent_task_id === state.rootId);
  const round = Math.max(0, ...children.map(task => task.run_count || 0));
  $("#round-state").textContent = children.length ? `已委派 ${children.length}/2 · 讨论轮次 ${round}` : root ? "主持人准备委派" : "等待选题";
}

async function refresh() {
  if (!state.rootId) return;
  try {
    const payload = await api(`/tasks?root_task_id=${encodeURIComponent(state.rootId)}&limit=100`);
    render(payload.items || []);
    setConnection("已连接", "completed");
  } catch (error) { setConnection("连接失败", "failed"); toast(error.message); }
}
function startPolling() { clearInterval(state.poller); refresh(); state.poller = setInterval(refresh, 2500); }

$("#connect").addEventListener("click", async () => {
  state.token = tokenInput.value.trim(); localStorage.setItem("ruyi.gatewayToken", state.token);
  try { const agents = await api("/agents"); setConnection(`在线 · ${agents.items.length} agents`, "completed"); if (state.rootId) startPolling(); }
  catch (error) { setConnection("认证失败", "failed"); toast(error.message); }
});

$("#start-form").addEventListener("submit", async event => {
  event.preventDefault();
  const requirement = $("#requirement").value.trim(); if (!requirement) return;
  const button = $("#start"); button.disabled = true; button.textContent = "送审中…";
  try {
    const task = await api("/agents/main/tasks", {method: "POST", body: JSON.stringify({input: {content: requirement}, metadata: {surface: "team_console"}})});
    state.rootId = task.task_id; rootInput.value = task.task_id; localStorage.setItem("ruyi.teamRootId", task.task_id);
    render([task]); startPolling(); toast("主持 Task 已创建");
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; button.textContent = "送审"; }
});

$("#load-root").addEventListener("click", () => {
  const id = rootInput.value.trim(); if (!id) return;
  state.rootId = id; localStorage.setItem("ruyi.teamRootId", id); startPolling(); toast("正在恢复 Task Tree");
});
$("#refresh").addEventListener("click", refresh);
$("#cancel-root").addEventListener("click", async () => {
  if (!state.rootId) return;
  try { await api(`/tasks/${state.rootId}/cancel`, {method: "POST"}); await refresh(); toast("取消请求已提交"); }
  catch (error) { toast(error.message); }
});

$$('.open-session').forEach(button => button.addEventListener("click", () => {
  const task = state.tasks.get(button.dataset.taskId); if (!task) return;
  state.selectedTaskId = task.task_id;
  $("#session-agent").textContent = task.agent_name;
  $("#session-title").textContent = task.agent_name === "architect_codex" ? "Codex 独立 Session" : "DeepSeek 独立 Session";
  fillStatus($("#session-status"), task.status);
  $("#session-result").textContent = task.last_result || task.error || "当前 Run 尚未产生结果。";
  $("#session-dialog").showModal();
}));

$("#session-form").addEventListener("submit", async event => {
  event.preventDefault(); if (!state.selectedTaskId) return;
  const textarea = $("#session-message"); const content = textarea.value.trim(); if (!content) return;
  const button = $("#session-form .button"); button.disabled = true;
  try { await api(`/tasks/${state.selectedTaskId}/input`, {method: "POST", body: JSON.stringify({input: {content}})}); textarea.value = ""; await refresh(); toast("已发送到同一 Task"); }
  catch (error) { toast(error.message); }
  finally { button.disabled = false; }
});

$("#clear-events").addEventListener("click", () => $("#event-log").replaceChildren());
if (state.token) $("#connect").click();
