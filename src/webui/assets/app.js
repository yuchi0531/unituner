// unituner management UI (SPEC §10).
// Vanilla JS, no build step, no framework. The page is one file: tabs switch
// visibility, the status tab polls /webui/api/status, and the editors PUT the
// whole document back to /webui/api/{tuners,channels,pipelines}.
'use strict';

const STATUS_INTERVAL_MS = 2500;

/** Per-document ETag from the last GET, sent back as If-Match on save. */
const etags = { channels: null, tuners: null, pipelines: null };

/** Editable models, the single source of truth for each editor. */
const models = {
  channels: [],
  tuners: [],
  pipelines: {}, // name -> [command]
  bindings: {}, // type -> pipeline
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

/** Calls `path` and returns the parsed JSON body. Throws an Error on !2xx. */
async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { Accept: 'application/json', ...(options.headers || {}) },
    ...options,
  });
  const text = await response.text();
  let body = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { code: response.status, reason: text, errors: [] };
    }
  }
  if (!response.ok) {
    const error = new Error((body && body.reason) || response.statusText);
    error.body = body;
    error.status = response.status;
    throw error;
  }
  return { body, etag: response.headers.get('ETag') };
}

async function loadDocument(name) {
  const { body, etag } = await api('/webui/api/' + name);
  etags[name] = etag;
  return body;
}

async function saveDocument(name, payload) {
  const { etag } = await api('/webui/api/' + name, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      ...(etags[name] ? { 'If-Match': etags[name] } : {}),
    },
    body: JSON.stringify(payload),
  });
  // The save response carries the ETag of what was just written, so a second
  // save from the same page is not rejected as a conflict.
  etags[name] = etag;
}

/** Formats an API error, including the validation issue list. */
function describe(error) {
  const issues = error.body && Array.isArray(error.body.errors) ? error.body.errors : [];
  return issues.length ? `${error.message}\n${issues.join('\n')}` : error.message;
}

// ---------------------------------------------------------------------------
// Small DOM helpers
// ---------------------------------------------------------------------------

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === 'class') node.className = value;
    else if (key === 'text') node.textContent = value;
    else if (key.startsWith('data-') || key.startsWith('aria-')) node.setAttribute(key, value);
    else if (key in node) node[key] = value;
    else node.setAttribute(key, value);
  }
  for (const child of [].concat(children)) {
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

function field(label, input) {
  return el('label', {}, [label, input]);
}

function textInput(value, onInput, placeholder = '') {
  const input = el('input', { type: 'text', value: value ?? '', placeholder });
  input.addEventListener('input', () => onInput(input.value));
  return input;
}

function checkbox(checked, onChange) {
  const input = el('input', { type: 'checkbox', checked: Boolean(checked) });
  input.addEventListener('change', () => onChange(input.checked));
  return input;
}

function badge(text, kind) {
  return el('span', { class: kind ? 'badge ' + kind : 'badge', text });
}

function message(text, kind) {
  const node = $('#message');
  node.textContent = text || '';
  node.className = 'message' + (kind ? ' ' + kind : '');
}

function formatTime(millis) {
  if (!millis) return '—';
  const delta = Math.round((Date.now() - millis) / 1000);
  if (delta < 60) return `${delta}s ago`;
  if (delta < 3600) return `${Math.round(delta / 60)}m ago`;
  return new Date(millis).toLocaleString();
}

/** The channel label used across the status tab. */
function channelLabel(channel) {
  return channel ? `${channel.type}/${channel.channel}` : '—';
}

// ---------------------------------------------------------------------------
// Tabs
// ---------------------------------------------------------------------------

const loaders = {
  status: () => refreshStatus(),
  services: () => loadServices(),
  channels: () => loadChannels(),
  tuners: () => loadTuners(),
  pipelines: () => loadPipelines(),
  server: () => loadServerConfig(),
};

function showTab(name) {
  $$('#tabs button').forEach((button) => {
    button.classList.toggle('active', button.dataset.tab === name);
  });
  $$('main .tab').forEach((section) => {
    section.classList.toggle('active', section.id === 'tab-' + name);
  });
  message('');
  const loader = loaders[name];
  if (loader) {
    Promise.resolve(loader()).catch((error) => message(describe(error), 'error'));
  }
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

let statusTimer = null;

async function refreshStatus() {
  const { body } = await api('/webui/api/status');
  $('#version').textContent = 'v' + body.version + ' · pid ' + body.pid;
  const counts = $('#counts');
  counts.replaceChildren(
    ...Object.entries({
      Tuners: body.counts.tuners,
      'In use': body.tuners.filter((tuner) => tuner.isUsing).length,
      Sessions: body.counts.sessions,
      Users: body.counts.users,
      Services: body.counts.services,
      Channels: body.counts.channels,
    }).map(([label, value]) => el('div', { class: 'count' }, [el('b', { text: value }), el('span', { text: label })])),
  );

  const tuners = $('#status-tuners');
  tuners.replaceChildren(
    ...body.tuners.map((tuner) => {
      const kind = !tuner.isAvailable ? 'disabled' : tuner.isUsing ? 'using' : 'free';
      const state = !tuner.isAvailable ? 'disabled' : tuner.isFree ? 'free' : tuner.isUsing ? 'busy' : 'available';
      const details = el('dl', {}, [
        el('dt', { text: 'types' }),
        el('dd', { text: tuner.types.join(', ') || '—' }),
        el('dt', { text: 'channel' }),
        el('dd', { text: channelLabel(tuner.channel) }),
        el('dt', { text: 'users' }),
        el('dd', { text: String(tuner.users.length) }),
      ]);
      const users = el('div', {});
      if (tuner.users.length) {
        users.append(
          el('table', {}, [
            el('thead', {}, [
              el('tr', {}, ['id', 'priority', 'provider', 'started'].map((head) => el('th', { text: head }))),
            ]),
            el('tbody', {}, tuner.users.map((user) =>
              el('tr', {}, [
                el('td', { text: user.id }),
                el('td', { text: String(user.priority) }),
                el('td', { text: user.provider || '—' }),
                el('td', { text: formatTime(user.startedAt) }),
              ]),
            )),
          ]),
        );
      }
      return el('div', { class: 'card ' + kind }, [
        el('h4', {}, [tuner.name, badge(state, tuner.isAvailable ? (tuner.isUsing ? 'warn' : 'ok') : 'bad')]),
        details,
        users,
      ]);
    }),
  );

  const sessions = $('#status-sessions');
  if (!body.sessions.length) {
    sessions.replaceChildren(el('p', { class: 'hint', text: 'No active sessions.' }));
  } else {
    sessions.replaceChildren(
      ...body.sessions.map((session) =>
        el('div', { class: 'card ' + (session.phase === 'Ready' ? 'free' : 'using') }, [
          el('h4', {}, ['#' + session.id, badge(session.phase, session.phase === 'Ready' ? 'ok' : 'warn')]),
          el('dl', {}, [
            el('dt', { text: 'tuner' }),
            el('dd', { text: session.tuner }),
            el('dt', { text: 'channel' }),
            el('dd', { text: `${session.type}/${session.channel}` }),
            el('dt', { text: 'users' }),
            el('dd', { text: String(session.users) }),
            el('dt', { text: 'pids' }),
            el('dd', { text: session.pids.join(' ') || '—' }),
            el('dt', { text: 'started' }),
            el('dd', { text: formatTime(session.startedAt) }),
          ]),
        ]),
      ),
    );
  }
  $('#status-meta').textContent = 'auto-refresh every ' + STATUS_INTERVAL_MS / 1000 + 's';
}

function startStatusPolling() {
  if (statusTimer) return;
  statusTimer = setInterval(() => {
    if (document.hidden || !$('#tab-status').classList.contains('active')) return;
    refreshStatus().catch((error) => message(describe(error), 'error'));
  }, STATUS_INTERVAL_MS);
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

async function loadServices() {
  const { body } = await api('/api/services');
  const rows = body.map((service) =>
    el('tr', {}, [
      el('td', { text: String(service.id) }),
      el('td', { text: String(service.serviceId) }),
      el('td', { text: String(service.networkId) }),
      el('td', { text: service.name }),
      el('td', { text: String(service.type) }),
      el('td', { text: channelLabel(service.channel) }),
    ]),
  );
  $('#services-table tbody').replaceChildren(...rows);
  if (!rows.length) {
    $('#services-table tbody').append(
      el('tr', {}, [el('td', { colspan: '6', class: 'hint', text: 'No services yet.' })]),
    );
  }
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

async function loadChannels() {
  const body = await loadDocument('channels');
  models.channels = body.channels || [];
  renderChannels();
}

function renderChannels() {
  const rows = models.channels.map((channel, index) => {
    const remove = el('button', { class: 'icon danger', text: '×', title: 'Delete' });
    remove.addEventListener('click', () => {
      models.channels.splice(index, 1);
      renderChannels();
    });
    return el('tr', {}, [
      el('td', {}, [textInput(channel.name, (value) => (channel.name = value), 'ＮＨＫ総合')]),
      el('td', {}, [textInput(channel.type, (value) => (channel.type = value), 'GR')]),
      el('td', {}, [textInput(channel.channel, (value) => (channel.channel = value), '27')]),
      el('td', {}, [checkbox(channel.disabled, (value) => (channel.disabled = value))]),
      el('td', { class: 'row-actions' }, [remove]),
    ]);
  });
  $('#channels-table tbody').replaceChildren(...rows);
}

function addChannel() {
  models.channels.push({ name: '', type: '', channel: '', disabled: false });
  renderChannels();
}

async function saveChannels() {
  await saveDocument('channels', { channels: models.channels });
  message('channels.yml saved (previous version in channels.yml.bak). Restart to apply.', 'ok');
  await loadChannels();
}

// ---------------------------------------------------------------------------
// Tuners
// ---------------------------------------------------------------------------

async function loadTuners() {
  const body = await loadDocument('tuners');
  models.tuners = body.tuners || [];
  renderTuners();
}

function renderTuners() {
  const cards = models.tuners.map((tuner, index) => {
    const remove = el('button', { class: 'icon danger', text: 'Delete' });
    remove.addEventListener('click', () => {
      models.tuners.splice(index, 1);
      renderTuners();
    });
    return el('div', { class: 'card editor' }, [
      el('h4', {}, [tuner.name || '(unnamed)', remove]),
      field('name', textInput(tuner.name, (value) => (tuner.name = value), 'pt3-gr0')),
      field(
        'types (comma separated)',
        textInput((tuner.types || []).join(', '), (value) => {
          tuner.types = value.split(',').map((part) => part.trim()).filter(Boolean);
        }, 'GR, BS'),
      ),
      field(
        'command',
        textInput(tuner.command, (value) => (tuner.command = value), 'recpt1 --device /dev/pt3video0 {channel} -'),
      ),
      el('label', {}, [checkbox(tuner.disabled, (value) => (tuner.disabled = value)), ' disabled']),
    ]);
  });
  $('#tuners-list').replaceChildren(...cards);
}

function addTuner() {
  models.tuners.push({ name: '', types: [], command: '', disabled: false });
  renderTuners();
}

async function saveTuners() {
  await saveDocument('tuners', { tuners: models.tuners });
  message('tuners.yml saved (previous version in tuners.yml.bak). Restart to apply.', 'ok');
  await loadTuners();
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

async function loadPipelines() {
  const body = await loadDocument('pipelines');
  models.pipelines = body.pipelines || {};
  models.bindings = {};
  for (const [type, binding] of Object.entries(body.types || {})) {
    models.bindings[type] = binding.pipeline;
  }
  renderPipelines();
}

function pipelineNames() {
  return Object.keys(models.pipelines);
}

function renderPipelines() {
  const cards = Object.entries(models.pipelines).map(([name, commands]) => {
    const remove = el('button', { class: 'icon danger', text: 'Delete pipeline' });
    remove.addEventListener('click', () => {
      delete models.pipelines[name];
      // Drop bindings that would reference the deleted pipeline.
      for (const [type, pipeline] of Object.entries(models.bindings)) {
        if (pipeline === name) delete models.bindings[type];
      }
      renderPipelines();
    });

    // Renaming on `change` (blur/Enter) rather than on every keystroke: a
    // re-render per character would steal the focus.
    const nameInput = el('input', { type: 'text', value: name, placeholder: 'pipeline name' });
    nameInput.addEventListener('change', () => {
      const next = nameInput.value.trim();
      if (!next || next === name || next in models.pipelines) {
        nameInput.value = name;
        return;
      }
      const renamed = {};
      for (const [key, value] of Object.entries(models.pipelines)) {
        renamed[key === name ? next : key] = value;
      }
      models.pipelines = renamed;
      for (const [type, pipeline] of Object.entries(models.bindings)) {
        if (pipeline === name) models.bindings[type] = next;
      }
      renderPipelines();
    });

    const stages = commands.map((command, stageIndex) => {
      const up = el('button', { class: 'icon', text: '↑', title: 'Move up' });
      const down = el('button', { class: 'icon', text: '↓', title: 'Move down' });
      const drop = el('button', { class: 'icon danger', text: '×', title: 'Remove stage' });
      up.disabled = stageIndex === 0;
      down.disabled = stageIndex === commands.length - 1;
      up.addEventListener('click', () => {
        [commands[stageIndex - 1], commands[stageIndex]] = [commands[stageIndex], commands[stageIndex - 1]];
        renderPipelines();
      });
      down.addEventListener('click', () => {
        [commands[stageIndex + 1], commands[stageIndex]] = [commands[stageIndex], commands[stageIndex + 1]];
        renderPipelines();
      });
      drop.addEventListener('click', () => {
        commands.splice(stageIndex, 1);
        renderPipelines();
      });
      return el('div', { class: 'stage' }, [
        textInput(command, (value) => (commands[stageIndex] = value), 'arib-b25-stream-test'),
        up,
        down,
        drop,
      ]);
    });

    const addStage = el('button', { text: 'Add stage' });
    addStage.addEventListener('click', () => {
      commands.push('');
      renderPipelines();
    });

    return el('div', { class: 'card editor' }, [
      el('div', { class: 'card-head' }, [nameInput, remove]),
      ...stages,
      addStage,
    ]);
  });
  $('#pipelines-list').replaceChildren(...cards);

  const rows = Object.entries(models.bindings).map(([type, pipeline]) => {
    const select = el('select', {}, pipelineNames().map((name) =>
      el('option', { value: name, text: name, selected: name === pipeline }),
    ));
    select.addEventListener('change', () => (models.bindings[type] = select.value));
    const remove = el('button', { class: 'icon danger', text: '×', title: 'Remove binding' });
    remove.addEventListener('click', () => {
      delete models.bindings[type];
      renderPipelines();
    });
    // `change` (not `input`): renaming on every keystroke would rebuild the row
    // and steal focus after the first character.
    const typeInput = el('input', { type: 'text', value: type, placeholder: 'GR' });
    typeInput.addEventListener('change', () => {
      const next = typeInput.value.trim();
      if (!next || next === type || next in models.bindings) {
        typeInput.value = type;
        return;
      }
      const bindings = {};
      for (const [key, value] of Object.entries(models.bindings)) {
        bindings[key === type ? next : key] = value;
      }
      models.bindings = bindings;
      renderPipelines();
    });
    return el('tr', {}, [
      el('td', {}, [typeInput]),
      el('td', {}, [select]),
      el('td', { class: 'row-actions' }, [remove]),
    ]);
  });
  $('#bindings-table tbody').replaceChildren(...rows);
}

function addPipeline() {
  const taken = new Set(pipelineNames());
  let name = 'pipeline';
  for (let index = 2; taken.has(name); index += 1) name = 'pipeline' + index;
  models.pipelines[name] = [''];
  renderPipelines();
}

function addBinding() {
  const taken = new Set(Object.keys(models.bindings));
  let type = 'TYPE';
  for (let index = 2; taken.has(type); index += 1) type = 'TYPE' + index;
  models.bindings[type] = pipelineNames()[0] || '';
  renderPipelines();
}

async function savePipelines() {
  const bindings = {};
  for (const [type, pipeline] of Object.entries(models.bindings)) {
    bindings[type] = { pipeline };
  }
  await saveDocument('pipelines', { pipelines: models.pipelines, types: bindings });
  message('pipelines.yml saved (previous version in pipelines.yml.bak). Restart to apply.', 'ok');
  await loadPipelines();
}

// ---------------------------------------------------------------------------
// Server config (read-only)
// ---------------------------------------------------------------------------

async function loadServerConfig() {
  const { body } = await api('/webui/api/server-config');
  const rows = [
    ['addr', body.addr],
    ['allowed_cidrs', body.allowed_cidrs.join(', ')],
    ['idle_timeout_secs', body.idle_timeout_secs],
    ['subscriber_buffer_chunks', body.subscriber_buffer_chunks],
    ['chunk_size_bytes', body.chunk_size_bytes],
  ].map(([label, value]) => [el('dt', { text: label }), el('dd', { text: String(value) })]);
  $('#server-config').replaceChildren(...rows.flat());
}

// ---------------------------------------------------------------------------
// Restart
// ---------------------------------------------------------------------------

async function restart() {
  const confirmed = window.confirm(
    'Restart unituner?\n\nAll active streams are stopped and the process re-executes with the configuration on disk.',
  );
  if (!confirmed) return;
  $('#restart').disabled = true;
  message('Restarting…');
  try {
    await api('/webui/api/restart', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
  } catch (error) {
    // A self-exec closes this connection before the body is read, so a network
    // error is expected; only a JSON error means the restart was refused.
    if (error.body && error.status) {
      message('Restart refused:\n' + describe(error), 'error');
      $('#restart').disabled = false;
      return;
    }
  }
  message('Restarting… waiting for the server to come back.', 'ok');
  await waitForServer();
  $('#restart').disabled = false;
  refreshStatus().catch(() => {});
}

/** Polls the status endpoint until the re-executed process answers. */
async function waitForServer() {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 500));
    try {
      await api('/webui/api/status');
      message('Server is back.', 'ok');
      return;
    } catch {
      // Not listening yet.
    }
  }
  message('The server did not come back within 30s. Check its log.', 'error');
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

function init() {
  $$('#tabs button').forEach((button) => {
    button.addEventListener('click', () => showTab(button.dataset.tab));
  });
  $$('[data-add]').forEach((button) => {
    button.addEventListener('click', () => {
      const actions = { channel: addChannel, tuner: addTuner, pipeline: addPipeline, binding: addBinding };
      const action = actions[button.dataset.add];
      if (action) action();
    });
  });
  $$('[data-save]').forEach((button) => {
    button.addEventListener('click', () => {
      const actions = { channels: saveChannels, tuners: saveTuners, pipelines: savePipelines };
      const action = actions[button.dataset.save];
      if (!action) return;
      message('');
      action().catch((error) => message(describe(error), 'error'));
    });
  });
  $('#restart').addEventListener('click', restart);
  startStatusPolling();
  showTab('status');
}

document.addEventListener('DOMContentLoaded', init);
