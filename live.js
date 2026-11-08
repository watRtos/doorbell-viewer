// Live doorbell panel for the recordings page: status, Answer/End, live camera,
// call audio and push-to-talk, over Supabase Realtime private broadcast channels
// doorbell:<device>:ctl|audio|video (access: supabase/realtime.sql).
// Plain script (no build step) so the ESP32-CAM can serve it next to index.html.
// The inline script in index.html calls CG2271Live.start(...) after sign-in and
// CG2271Live.stop() on sign-out. Tested by supabase/tests/test_live.mjs.
(function (global) {
  'use strict';
  const RATE = 16000, CHUNK = 1600;            // 100 ms of 16 kHz mono per message
  const HEARTBEAT_MS = 25000, WATCH_MS = 5000, STATE_TIMEOUT_MS = 8000, TOKEN_CHECK_MS = 60000;
  const LABELS = {
    IDLE: 'Nobody at the door', VERIFYING_VISITOR: 'Checking a visitor…', VISITOR_PRESENT: 'Visitor at the door',
    WAITING_FOR_RESPONSE: '🔔 Someone pressed the doorbell', LIVE_SESSION: 'In a call with the door',
    RECORDING: 'Recording the visitor’s message',
  };

  // ---------- pure helpers (exported for tests) ----------
  function bytesToB64(bytes) {
    let s = '';
    for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
    return btoa(s);
  }
  function b64ToBytes(b64) {
    const s = atob(b64), out = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
    return out;
  }
  function int16ToB64(samples) {
    const bytes = new Uint8Array(samples.length * 2), view = new DataView(bytes.buffer);
    samples.forEach((v, i) => view.setInt16(i * 2, v, true));
    return bytesToB64(bytes);
  }
  function b64ToFloat(b64) {
    const bytes = b64ToBytes(b64), view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const out = new Float32Array(bytes.length >> 1);
    for (let i = 0; i < out.length; i++) out[i] = view.getInt16(i * 2, true) / 32768;
    return out;
  }
  // Linear-interpolation resampler, any input rate -> 16 kHz int16, keeping position across blocks.
  // Positions are in input samples; index -1 is the previous block's last sample.
  class Resampler {
    constructor(fromRate, toRate = RATE) { this.step = fromRate / toRate; this.pos = 0; this.prev = 0; }
    push(input) {
      const out = [], n = input.length;
      while (this.pos < n - 1) {
        const i = Math.floor(this.pos), t = this.pos - i;
        const a = i < 0 ? this.prev : input[i], b = input[i + 1];
        const v = a + (b - a) * t;
        out.push(Math.max(-32768, Math.min(32767, Math.round(v * 32767))));
        this.pos += this.step;
      }
      if (n) { this.pos -= n; this.prev = input[n - 1]; }
      return Int16Array.from(out);
    }
  }
  function topic(deviceId, suffix) { return `realtime:doorbell:${deviceId}:${suffix}`; }
  function joinFrame(t, token, ref) {
    return JSON.stringify({topic: t, event: 'phx_join', ref: String(ref), join_ref: String(ref), payload: {
      config: {broadcast: {ack: false, self: false}, presence: {key: ''}, postgres_changes: [], private: true},
      access_token: token}});
  }
  function broadcastFrame(t, event, payload, ref) {
    return JSON.stringify({topic: t, event: 'broadcast', ref: String(ref), payload: {type: 'broadcast', event, payload}});
  }
  function parseFrame(text) {
    let m;
    try { m = JSON.parse(text); } catch { return null; }
    if (!m || typeof m.topic !== 'string' || typeof m.event !== 'string') return null;
    const p = m.payload || {};
    if (m.event === 'broadcast') return {topic: m.topic, event: 'broadcast', inner: p.event, payload: p.payload || {}, ref: m.ref};
    return {topic: m.topic, event: m.event, status: p.status, payload: p, ref: m.ref};
  }
  // What the panel shows and allows for a doorbell state message.
  function stateView(s) {
    if (!s) return {label: 'Waiting for the doorbell…', answer: false, end: false, talk: false, verify: false, ring: false, testRing: false};
    const live = s.state === 'LIVE_SESSION';
    return {
      label: LABELS[s.state] || s.state,
      answer: s.state === 'WAITING_FOR_RESPONSE' || (!s.link && !live),
      end: live,
      talk: live && s.speaker !== 'locked',
      verify: s.state === 'VERIFYING_VISITOR',
      ring: s.state === 'WAITING_FOR_RESPONSE',
      testRing: !s.link && s.state === 'IDLE',
      locked: s.speaker === 'locked',
      bench: !s.link,
    };
  }

  // ---------- Realtime client ----------
  class Realtime {
    constructor({url, key, deviceId, token, onMessage, onStatus, WebSocketImpl}) {
      Object.assign(this, {url, key, deviceId, token, onMessage, onStatus});
      this.WS = WebSocketImpl || global.WebSocket;
      this.channels = new Map(); this.ref = 1; this.stopped = false; this.backoff = 1000; this.currentToken = null;
    }
    add(suffix) { this.channels.set(topic(this.deviceId, suffix), {suffix, joined: false, joinRef: null}); }
    joinedCount() { let n = 0; for (const c of this.channels.values()) if (c.joined) n++; return n; }
    async connect() {
      if (this.stopped) return;
      this.currentToken = await this.token();
      if (this.stopped) return;
      const ws = new this.WS(`${this.url.replace(/^http/, 'ws')}/realtime/v1/websocket?apikey=${encodeURIComponent(this.key)}&vsn=1.0.0`);
      this.ws = ws;
      ws.onopen = () => {
        this.backoff = 1000;
        for (const [t, c] of this.channels) { c.joined = false; c.joinRef = String(this.ref); ws.send(joinFrame(t, this.currentToken, this.ref++)); }
        this.heartbeat = setInterval(() => this.raw({topic: 'phoenix', event: 'heartbeat', payload: {}, ref: String(this.ref++)}), HEARTBEAT_MS);
        this.tokenTimer = setInterval(() => this.checkToken(), TOKEN_CHECK_MS);
        this.onStatus('connecting');
      };
      ws.onmessage = e => this.receive(typeof e.data === 'string' ? e.data : '');
      ws.onclose = () => {
        clearInterval(this.heartbeat); clearInterval(this.tokenTimer);
        for (const c of this.channels.values()) c.joined = false;
        if (this.stopped) return;
        this.onStatus('offline');
        this.retry = setTimeout(() => this.connect().catch(() => this.onStatus('offline')), this.backoff);
        this.backoff = Math.min(this.backoff * 2, 15000);
      };
      ws.onerror = () => { try { ws.close(); } catch {} };
    }
    receive(text) {
      const m = parseFrame(text);
      if (!m) return;
      const c = this.channels.get(m.topic);
      if (!c) return;
      if (m.event === 'phx_reply' && m.ref === c.joinRef) {
        c.joined = m.status === 'ok'; c.joinRef = null;
        this.onStatus(c.joined ? (this.joinedCount() === this.channels.size ? 'live' : 'connecting') : 'refused', c.suffix);
      } else if (m.event === 'phx_close' || m.event === 'phx_error') {
        c.joined = false; this.onStatus('connecting');
      } else if (m.event === 'broadcast' && c.joined) {
        this.onMessage(c.suffix, m.inner, m.payload);
      }
    }
    raw(obj) { if (this.ws && this.ws.readyState === 1) this.ws.send(JSON.stringify(obj)); }
    send(suffix, event, payload) {
      const t = topic(this.deviceId, suffix), c = this.channels.get(t);
      if (!c || !c.joined || !this.ws || this.ws.readyState !== 1) return false;
      this.ws.send(broadcastFrame(t, event, payload, this.ref++));
      return true;
    }
    async checkToken() {
      try {
        const next = await this.token();
        if (next && next !== this.currentToken) {
          this.currentToken = next;
          for (const [t, c] of this.channels) if (c.joined) this.raw({topic: t, event: 'access_token', payload: {access_token: next}, ref: String(this.ref++)});
        }
      } catch {}
    }
    close() {
      this.stopped = true; clearTimeout(this.retry); clearInterval(this.heartbeat); clearInterval(this.tokenTimer);
      if (this.ws) { this.ws.onclose = null; try { this.ws.close(); } catch {} }
    }
  }

  // ---------- panel ----------
  let rt = null, el = null, state = null, stateTimer = null, watchTimer = null, frameUrl = null;
  let audio = null, playAt = 0, talk = null, talkSeq = 0, secure = false, doc = null;

  const $ = id => doc.getElementById(id);
  function show(node, on) { if (node) node.classList[on ? 'remove' : 'add']('hidden'); }
  function setText(node, text) { if (node) node.textContent = text; }

  function render() {
    const v = stateView(state);
    setText(el.state, v.label + (state && state.error ? ` — ${state.error}` : ''));
    el.state.className = v.ring ? 'warn' : 'muted';
    el.answer.disabled = !v.answer;
    el.answer.textContent = v.ring ? 'Answer' : 'Talk to the door';
    el.hangup.disabled = !v.end;
    show(el.verify, v.verify);
    show(el.testRing, !!v.testRing);
    el.talk.disabled = !(v.talk && secure);
    setText(el.talkNote, !state ? '' : v.locked ? 'The doorbell speaker is locked until its bench safety check; you can still listen.'
      : !secure ? 'Talking needs the HTTPS page (GitHub Pages), not the camera’s local page.'
      : v.end ? 'Hold the button to talk; release to listen.' : v.bench ? 'Bench mode: no MCXC444 link.' : '');
    if (!v.end) stopTalk();
  }

  function onStatus(status, suffix) {
    const text = {connecting: 'Connecting…', live: 'Live', offline: 'Offline — retrying', refused: `Access refused (${suffix})`}[status];
    setText(el.link, text || status);
    el.link.className = status === 'live' ? 'ok' : status === 'refused' || status === 'offline' ? 'bad' : 'muted';
    if (status === 'live') {
      rt.send('ctl', 'hello', {});
      clearTimeout(stateTimer);
      stateTimer = setTimeout(() => { if (!state) setText(el.state, 'The doorbell (ESP32-S2) is not answering.'); }, STATE_TIMEOUT_MS);
    }
  }

  function chime() {
    try { if (global.navigator && global.navigator.vibrate) global.navigator.vibrate([200, 100, 200]); } catch {}
    if (!audio) return; // no sound without an earlier tap; the banner and Telegram still alert
    const o = audio.createOscillator(), g = audio.createGain();
    o.frequency.value = 880; g.gain.value = 0.15; o.connect(g); g.connect(audio.destination);
    o.start(); o.stop(audio.currentTime + 0.4);
  }

  function onMessage(channel, event, payload) {
    if (channel === 'ctl' && event === 'state' && payload && typeof payload.state === 'string') {
      const ringing = payload.state === 'WAITING_FOR_RESPONSE' && (!state || state.state !== 'WAITING_FOR_RESPONSE');
      state = payload;
      clearTimeout(stateTimer);
      render();
      if (ringing) { chime(); setWatching(true); }
    } else if (channel === 'video' && event === 'frame' && payload && typeof payload.jpeg === 'string' && el.watchOn) {
      const blob = new Blob([b64ToBytes(payload.jpeg)], {type: 'image/jpeg'}), next = URL.createObjectURL(blob);
      el.frame.src = next; show(el.frame, true);
      if (frameUrl) URL.revokeObjectURL(frameUrl);
      frameUrl = next;
    } else if (channel === 'audio' && event === 'up' && payload && typeof payload.pcm === 'string') {
      play(b64ToFloat(payload.pcm), payload.rate || RATE);
    }
  }

  function ensureAudio() {
    const Ctx = global.AudioContext || global.webkitAudioContext;
    if (!audio && Ctx) audio = new Ctx();
    if (audio && audio.state === 'suspended') audio.resume();
    return audio;
  }
  function play(samples, rate) {
    if (!audio || audio.state !== 'running' || talk || !state || state.state !== 'LIVE_SESSION') return;
    const now = audio.currentTime;
    if (playAt > now + 0.4) playAt = 0;          // bound latency; never build a backlog
    playAt = Math.max(playAt, now + 0.08);
    const buffer = audio.createBuffer(1, samples.length, rate);
    buffer.copyToChannel(samples, 0);
    const src = audio.createBufferSource();
    src.buffer = buffer; src.connect(audio.destination); src.start(playAt);
    playAt += buffer.duration;
  }

  function setWatching(on) {
    el.watchOn = on;
    clearInterval(watchTimer);
    el.watch.textContent = on ? 'Hide camera' : 'Show camera';
    if (on) {
      rt.send('ctl', 'watch', {});
      watchTimer = setInterval(() => rt.send('ctl', 'watch', {}), WATCH_MS);
    } else {
      show(el.frame, false);
      if (frameUrl) { URL.revokeObjectURL(frameUrl); frameUrl = null; }
    }
  }

  async function startTalk() {
    if (talk || el.talk.disabled) return;
    const media = global.navigator && global.navigator.mediaDevices;
    if (!secure || !media || !media.getUserMedia) { setText(el.talkNote, 'This browser cannot use the microphone here.'); return; }
    const ctx = ensureAudio();
    talk = {pending: true};
    let stream;
    try { stream = await media.getUserMedia({audio: {echoCancellation: true, noiseSuppression: true}}); }
    catch { talk = null; setText(el.talkNote, 'Microphone permission was refused.'); return; }
    if (!talk) { stream.getTracks().forEach(t => t.stop()); return; } // released before permission came back
    const source = ctx.createMediaStreamSource(stream), proc = ctx.createScriptProcessor(4096, 1, 1);
    const resample = new Resampler(ctx.sampleRate);
    let pending = [];
    proc.onaudioprocess = e => {
      const pcm = resample.push(e.inputBuffer.getChannelData(0));
      for (const v of pcm) pending.push(v);
      while (pending.length >= CHUNK) {
        rt.send('audio', 'down', {seq: talkSeq++, rate: RATE, pcm: int16ToB64(Int16Array.from(pending.slice(0, CHUNK)))});
        pending = pending.slice(CHUNK);
      }
    };
    const mute = ctx.createGain(); mute.gain.value = 0;   // keep the processor running without echoing
    source.connect(proc); proc.connect(mute); mute.connect(ctx.destination);
    talk = {stream, source, proc, mute};
    rt.send('ctl', 'talk', {on: true});
    el.talk.textContent = 'Talking… release to listen';
  }
  function stopTalk() {
    if (!talk) return;
    const t = talk; talk = null;
    if (t.proc) { t.proc.onaudioprocess = null; try { t.source.disconnect(); t.proc.disconnect(); t.mute.disconnect(); } catch {} }
    if (t.stream) t.stream.getTracks().forEach(tr => tr.stop());
    if (rt) rt.send('ctl', 'talk', {on: false});
    if (el) el.talk.textContent = 'Hold to talk';
  }

  function command(cmd) { if (!rt.send('ctl', 'cmd', {cmd})) setText(el.state, 'Not connected to the doorbell yet.'); }

  function start({cfg, deviceId, token, documentRef, WebSocketImpl, isSecure}) {
    stop();
    doc = documentRef || global.document;
    secure = isSecure != null ? isSecure : !!global.isSecureContext;
    el = {panel: $('live'), link: $('liveLink'), state: $('liveState'), frame: $('liveFrame'), answer: $('answer'),
          hangup: $('hangup'), watch: $('watch'), testRing: $('testRing'), verify: $('verify'), talk: $('talk'),
          talkNote: $('talkNote'), confirm: $('confirm'), reject: $('reject'), watchOn: false};
    if (!el.panel) return;
    show(el.panel, true);
    state = null; render();
    rt = new Realtime({url: cfg.url, key: cfg.key, deviceId, token, onMessage, onStatus, WebSocketImpl});
    ['ctl', 'audio', 'video'].forEach(s => rt.add(s));
    el.answer.onclick = () => { ensureAudio(); command('answer'); };
    el.hangup.onclick = () => command('end');
    el.testRing.onclick = () => { ensureAudio(); command('test_ring'); };
    el.confirm.onclick = () => command('confirm');
    el.reject.onclick = () => command('reject');
    el.watch.onclick = () => setWatching(!el.watchOn);
    const down = e => { if (e && e.preventDefault) e.preventDefault(); startTalk(); };
    el.talk.onpointerdown = down;
    el.talk.onpointerup = el.talk.onpointerleave = el.talk.onpointercancel = () => stopTalk();
    rt.connect().catch(() => onStatus('offline'));
  }

  function stop() {
    stopTalk();
    clearTimeout(stateTimer); clearInterval(watchTimer);
    if (rt) { rt.close(); rt = null; }
    if (frameUrl) { URL.revokeObjectURL(frameUrl); frameUrl = null; }
    if (audio) { try { audio.close(); } catch {} audio = null; }
    playAt = 0; state = null;
    if (el && el.panel) { show(el.panel, false); show(el.frame, false); el.frame.removeAttribute && el.frame.removeAttribute('src'); }
    el = null;
  }

  global.CG2271Live = {start, stop, _test: {bytesToB64, b64ToBytes, int16ToB64, b64ToFloat, Resampler, topic,
    joinFrame, broadcastFrame, parseFrame, stateView, Realtime, RATE, CHUNK}};
})(typeof window !== 'undefined' ? window : globalThis);
