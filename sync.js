/* Cortex Sync — WebRTC peer-to-peer note sync.

   Two transports:
     1. PeerJS — short 8-char codes, persistent device IDs, auto-reconnect
     2. Manual — SDP copy-paste, works without third-party signaling

   Merge policy: last-write-wins per note by `updated` timestamp. */

(function(global){
  'use strict';

  const ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
  ];

  const CHUNK_SIZE = 8000;
  const RECV_TIMEOUT_MS = 60000;
  const ICE_WAIT_MS = 3500;
  const CONNECT_TIMEOUT_MS = 15000;

  const PEERJS_CDN = 'https://unpkg.com/peerjs@1.5.4/dist/peerjs.min.js';
  const PEER_PREFIX = 'cortexsync-';
  const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';   // no 0/O/1/l/I
  const CODE_LENGTH = 8;

  const STORE_DEVICE_CODE = 'cortex.device.code.v1';
  const STORE_PAIRED_PEER = 'cortex.paired.peer.v1';   // JSON: { code, name, lastSync }
  const STORE_AUTO_SYNC = 'cortex.autosync.v1';        // '1' or '0'

  // ---- state ----
  let channel = null;          // active Channel (transport-agnostic)
  let role = null;             // 'host' | 'guest'
  let state = 'idle';

  let onProgress = null;
  let onDone = null;
  let onError = null;
  let notesProvider = null;
  let notesReceiver = null;

  let pjsPeer = null;          // PeerJS Peer instance
  let pjsConn = null;          // PeerJS DataConnection
  let peerjsLoaded = false;
  let peerjsLoading = null;

  /* ============================================================
     Device code (persistent)
     ============================================================ */

  function generateCode(){
    let s = '';
    for (let i = 0; i < CODE_LENGTH; i++){
      s += CODE_ALPHABET[Math.floor(Math.random() * CODE_ALPHABET.length)];
    }
    return s;
  }

  function getDeviceCode(){
    let code = null;
    try { code = localStorage.getItem(STORE_DEVICE_CODE); } catch(e){}
    if (code && code.length === CODE_LENGTH &&
        code.split('').every(c => CODE_ALPHABET.indexOf(c) >= 0)){
      return code;
    }
    code = generateCode();
    try { localStorage.setItem(STORE_DEVICE_CODE, code); } catch(e){}
    return code;
  }

  function formatCode(code){
    if (!code) return '';
    if (code.length === CODE_LENGTH) return code.slice(0, 4) + '-' + code.slice(4);
    return code;
  }

  function normalizeCode(input){
    return String(input || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  }

  /* ============================================================
     Paired peer memory
     ============================================================ */

  function getPairedPeer(){
    try {
      const raw = localStorage.getItem(STORE_PAIRED_PEER);
      if (!raw) return null;
      const p = JSON.parse(raw);
      if (p && typeof p.code === 'string' && p.code.length === CODE_LENGTH) return p;
    } catch(e){}
    return null;
  }

  function savePairedPeer(code, name){
    const norm = normalizeCode(code);
    if (!norm || norm.length !== CODE_LENGTH) return;
    const existing = getPairedPeer() || {};
    try {
      localStorage.setItem(STORE_PAIRED_PEER, JSON.stringify({
        code: norm,
        name: name || existing.name || '',
        lastSync: Date.now(),
      }));
    } catch(e){}
  }

  function forgetPairedPeer(){
    try { localStorage.removeItem(STORE_PAIRED_PEER); } catch(e){}
  }

  function getAutoSync(){
    try { return localStorage.getItem(STORE_AUTO_SYNC) !== '0'; } catch(e){ return true; }
  }
  function setAutoSync(on){
    try { localStorage.setItem(STORE_AUTO_SYNC, on ? '1' : '0'); } catch(e){}
  }

  /* ============================================================
     PeerJS loader
     ============================================================ */

  function loadPeerJS(){
    if (peerjsLoaded && typeof global.Peer === 'function') return Promise.resolve();
    if (peerjsLoading) return peerjsLoading;

    peerjsLoading = new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = PEERJS_CDN;
      s.onload = () => {
        if (typeof global.Peer === 'function'){
          peerjsLoaded = true;
          resolve();
        } else {
          peerjsLoading = null;
          reject(new Error('PeerJS loaded but window.Peer is missing.'));
        }
      };
      s.onerror = () => {
        peerjsLoading = null;
        reject(new Error('Could not load PeerJS from unpkg.com. Check your connection.'));
      };
      document.head.appendChild(s);
    });
    return peerjsLoading;
  }

  /* ============================================================
     Channel — transport-agnostic send/receive
     ============================================================ */

  function makeRTCChannel(dc){
    const queue = [];
    const waiters = [];
    const chunks = new Map();
    let closed = false;

    function dispatch(msg){
      for (let i = 0; i < waiters.length; i++){
        if (waiters[i].type === msg.type){
          const w = waiters.splice(i, 1)[0];
          w.resolve(msg);
          return;
        }
      }
      queue.push(msg);
    }

    function receive(type){
      return new Promise((resolve, reject) => {
        for (let i = 0; i < queue.length; i++){
          if (queue[i].type === type){
            resolve(queue.splice(i, 1)[0]);
            return;
          }
        }
        const w = { type, resolve, reject };
        waiters.push(w);
        setTimeout(() => {
          const i = waiters.indexOf(w);
          if (i >= 0){
            waiters.splice(i, 1);
            reject(new Error('Timeout waiting for "' + type + '"'));
          }
        }, RECV_TIMEOUT_MS);
      });
    }

    function send(obj){
      const json = JSON.stringify(obj);
      if (json.length <= CHUNK_SIZE){
        dc.send('M' + json);
        return;
      }
      const id = Math.random().toString(36).slice(2, 10);
      const total = Math.ceil(json.length / CHUNK_SIZE);
      for (let i = 0; i < total; i++){
        const part = json.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
        dc.send('C' + id + ':' + i + ':' + total + ':' + part);
      }
    }

    dc.addEventListener('message', (e) => {
      const data = e.data;
      if (typeof data !== 'string') return;
      if (data[0] === 'M'){
        try { dispatch(JSON.parse(data.slice(1))); } catch(err){}
      } else if (data[0] === 'C'){
        const rest = data.slice(1);
        const c1 = rest.indexOf(':');
        const c2 = rest.indexOf(':', c1 + 1);
        const c3 = rest.indexOf(':', c2 + 1);
        if (c1 < 0 || c2 < 0 || c3 < 0) return;
        const id = rest.slice(0, c1);
        const idx = parseInt(rest.slice(c1 + 1, c2), 10);
        const total = parseInt(rest.slice(c2 + 1, c3), 10);
        const part = rest.slice(c3 + 1);
        let entry = chunks.get(id);
        if (!entry){ entry = { total, parts: new Array(total) }; chunks.set(id, entry); }
        entry.parts[idx] = part;
        let complete = true;
        for (let i = 0; i < entry.total; i++){
          if (entry.parts[i] === undefined){ complete = false; break; }
        }
        if (complete){
          chunks.delete(id);
          try { dispatch(JSON.parse(entry.parts.join(''))); } catch(err){}
        }
      }
    });

    dc.addEventListener('close', () => { closed = true; });

    return {
      send,
      receive,
      close: () => { try { dc.close(); } catch(e){} closed = true; },
      isClosed: () => closed,
    };
  }

  function makePeerJSChannel(conn){
    const queue = [];
    const waiters = [];
    let closed = false;

    function dispatch(msg){
      for (let i = 0; i < waiters.length; i++){
        if (waiters[i].type === msg.type){
          const w = waiters.splice(i, 1)[0];
          w.resolve(msg);
          return;
        }
      }
      queue.push(msg);
    }

    function receive(type){
      return new Promise((resolve, reject) => {
        for (let i = 0; i < queue.length; i++){
          if (queue[i].type === type){
            resolve(queue.splice(i, 1)[0]);
            return;
          }
        }
        const w = { type, resolve, reject };
        waiters.push(w);
        setTimeout(() => {
          const i = waiters.indexOf(w);
          if (i >= 0){
            waiters.splice(i, 1);
            reject(new Error('Timeout waiting for "' + type + '"'));
          }
        }, RECV_TIMEOUT_MS);
      });
    }

    conn.on('data', (obj) => {
      if (obj && typeof obj === 'object' && typeof obj.type === 'string'){
        dispatch(obj);
      }
    });

    conn.on('close', () => { closed = true; });
    conn.on('error', () => { closed = true; });

    return {
      send: (obj) => { try { conn.send(obj); } catch(e){} },
      receive,
      close: () => { try { conn.close(); } catch(e){} closed = true; },
      isClosed: () => closed,
    };
  }

  /* ============================================================
     SDP codec (manual transport)
     ============================================================ */

  function encodeCode(sdp){
    return btoa(unescape(encodeURIComponent(sdp)))
      .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }
  function decodeCode(code){
    let c = String(code || '').trim().replace(/\s+/g, '');
    c = c.replace(/-/g, '+').replace(/_/g, '/');
    while (c.length % 4) c += '=';
    return decodeURIComponent(escape(atob(c)));
  }

  /* ============================================================
     Peer connection helpers (manual transport)
     ============================================================ */

  function waitForIce(conn){
    return new Promise(resolve => {
      if (conn.iceGatheringState === 'complete') return resolve();
      const done = () => {
        if (conn.iceGatheringState === 'complete'){
          conn.removeEventListener('icegatheringstatechange', done);
          resolve();
        }
      };
      conn.addEventListener('icegatheringstatechange', done);
      setTimeout(resolve, ICE_WAIT_MS);
    });
  }

  /* ============================================================
     Sync protocol — shared by both transports
     ============================================================ */

  async function runSync(ch){
    try {
      const mine = await notesProvider();
      const deviceName = describeDevice();
      const myCode = getDeviceCode();

      ch.send({ type: 'hello', deviceName, deviceCode: myCode, count: mine.length });
      const theirHello = await ch.receive('hello');

      setState('syncing', 'Connected to ' + (theirHello.deviceName || 'peer') + '. Sending notes...');

      ch.send({ type: 'notes', notes: mine });
      const theirsMsg = await ch.receive('notes');
      const theirs = Array.isArray(theirsMsg.notes) ? theirsMsg.notes : [];

      setState('syncing', 'Merging ' + theirs.length + ' notes from peer...');
      const result = await notesReceiver(theirs);

      // Remember the peer for auto-reconnect
      if (theirHello.deviceCode){
        savePairedPeer(theirHello.deviceCode, theirHello.deviceName);
      }

      ch.send({ type: 'done', result });
      const theirDone = await ch.receive('done');

      setState('done', 'Sync complete.');
      onDone && onDone({
        local: result,
        remote: theirDone.result || {},
        peerDevice: theirHello.deviceName || 'peer',
      });

      setTimeout(() => { try { ch.close(); } catch(e){} }, 400);
    } catch (err){
      fail(err);
    }
  }

  /* ============================================================
     Public — PeerJS quick flows
     ============================================================ */

  async function startQuickHost(){
    await loadPeerJS();
    reset();
    role = 'host';

    const myCode = getDeviceCode();
    const peerId = PEER_PREFIX + myCode;

    setState('waiting-connect', 'Waiting for the other device to connect...');

    return new Promise((resolve, reject) => {
      pjsPeer = new global.Peer(peerId, { debug: 0 });
      let settled = false;

      pjsPeer.on('open', () => {
        if (settled) return;
        settled = true;
        resolve({ code: myCode });
      });

      pjsPeer.on('connection', (conn) => {
        pjsConn = conn;
        channel = makePeerJSChannel(conn);
        setState('syncing', 'Connected. Starting sync...');
        runSync(channel).catch(err => fail(err));
      });

      pjsPeer.on('error', (err) => {
        if (settled){
          fail(err);
        } else {
          settled = true;
          reject(err);
        }
      });
    });
  }

  async function joinQuick(code){
    await loadPeerJS();
    reset();
    role = 'guest';

    const target = PEER_PREFIX + normalizeCode(code);
    setState('waiting-connect', 'Connecting to ' + formatCode(normalizeCode(code)) + '...');

    return new Promise((resolve, reject) => {
      pjsPeer = new global.Peer({ debug: 0 });
      let settled = false;

      pjsPeer.on('open', () => {
        const conn = pjsPeer.connect(target, { reliable: true });
        pjsConn = conn;
        channel = makePeerJSChannel(conn);

        conn.on('open', () => {
          if (settled) return;
          settled = true;
          setState('syncing', 'Connected. Starting sync...');
          resolve();
          runSync(channel).catch(err => fail(err));
        });

        setTimeout(() => {
          if (settled) return;
          settled = true;
          try { conn.close(); } catch(e){}
          reject(new Error('Connection timed out. Check the code and that both devices are online.'));
        }, CONNECT_TIMEOUT_MS);
      });

      pjsPeer.on('error', (err) => {
        if (settled) return;
        settled = true;
        if (err.type === 'peer-unavailable'){
          reject(new Error('No device is online with that code.'));
        } else {
          reject(err);
        }
      });
    });
  }

  /* ============================================================
     Public — Auto reconnect (background)
     ============================================================ */

  async function tryAutoReconnect(){
    if (!getAutoSync()) return { ok: false, reason: 'auto-sync disabled' };
    if (state === 'syncing' || state === 'waiting-connect') return { ok: false, reason: 'busy' };

    const paired = getPairedPeer();
    if (!paired) return { ok: false, reason: 'no paired peer' };

    // We don't know if the other device is online. PeerJS will tell us.
    try {
      await loadPeerJS();
    } catch(e){
      return { ok: false, reason: 'PeerJS unavailable' };
    }

    reset();
    role = 'guest';

    const target = PEER_PREFIX + paired.code;

    return new Promise((resolve) => {
      let settled = false;
      const done = (val) => { if (!settled){ settled = true; resolve(val); } };

      pjsPeer = new global.Peer({ debug: 0 });

      pjsPeer.on('open', () => {
        const conn = pjsPeer.connect(target, { reliable: true });
        pjsConn = conn;
        channel = makePeerJSChannel(conn);

        conn.on('open', () => {
          setState('syncing', 'Auto-syncing with ' + (paired.name || 'paired device') + '...');
          onProgress && onProgress({ state: 'auto-syncing', message: 'Auto-syncing with ' + (paired.name || 'peer') + '...' });
          runSync(channel).catch(() => {});
          done({ ok: true });
        });

        setTimeout(() => {
          if (settled) return;
          try { conn.close(); } catch(e){}
          try { pjsPeer.destroy(); } catch(e){}
          reset();
          setState('idle', '');
          done({ ok: false, reason: 'timeout' });
        }, 6000);
      });

      pjsPeer.on('error', () => {
        try { pjsPeer.destroy(); } catch(e){}
        reset();
        setState('idle', '');
        done({ ok: false, reason: 'peer unavailable' });
      });
    });
  }

  /* ============================================================
     Public — Manual flows (existing)
     ============================================================ */

  async function startManualHost(){
    reset();
    role = 'host';
    setState('creating-offer', 'Creating connection code...');

    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === 'failed') fail(new Error('ICE failed. Same Wi-Fi?'));
    };

    const dc = pc.createDataChannel('sync', { ordered: true });
    dc.addEventListener('open', () => {
      channel = makeRTCChannel(dc);
      setState('syncing', 'Connected. Starting sync...');
      runSync(channel).catch(err => fail(err));
    });

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await waitForIce(pc);

    manualPC = pc;
    setState('waiting-answer', 'Waiting for the other device to answer...');
    return encodeCode(pc.localDescription.sdp);
  }

  let manualPC = null;

  async function acceptManualAnswer(code){
    if (!manualPC) throw new Error('Not hosting');
    let sdp;
    try { sdp = decodeCode(code); }
    catch(e){ throw new Error('That answer code is malformed.'); }
    setState('waiting-connect', 'Connecting...');
    await manualPC.setRemoteDescription({ type: 'answer', sdp });
  }

  async function joinManualOffer(code){
    reset();
    role = 'guest';
    setState('creating-answer', 'Reading host code...');
    let sdp;
    try { sdp = decodeCode(code); }
    catch(e){ throw new Error('That host code is malformed.'); }

    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === 'failed') fail(new Error('ICE failed. Same Wi-Fi?'));
    };
    pc.ondatachannel = (e) => {
      const dc = e.channel;
      dc.addEventListener('open', () => {
        channel = makeRTCChannel(dc);
        setState('syncing', 'Connected. Starting sync...');
        runSync(channel).catch(err => fail(err));
      });
    };

    await pc.setRemoteDescription({ type: 'offer', sdp });
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    await waitForIce(pc);

    manualPC = pc;
    setState('waiting-connect', 'Send the answer code back, then wait...');
    return encodeCode(pc.localDescription.sdp);
  }

  /* ============================================================
     Utility
     ============================================================ */

  function fail(err){
    setState('error', err && err.message ? err.message : String(err));
    onError && onError(err);
    try { channel && channel.close(); } catch(e){}
    try { pjsConn && pjsConn.close(); } catch(e){}
    try { pjsPeer && pjsPeer.destroy(); } catch(e){}
    try { manualPC && manualPC.close(); } catch(e){}
    pjsConn = null; pjsPeer = null; manualPC = null; channel = null;
  }

  function reset(){
    try { channel && channel.close(); } catch(e){}
    try { pjsConn && pjsConn.close(); } catch(e){}
    try { pjsPeer && pjsPeer.destroy(); } catch(e){}
    try { manualPC && manualPC.close(); } catch(e){}
    channel = null; pjsConn = null; pjsPeer = null; manualPC = null;
    state = 'idle';
  }

  function setState(s, msg){
    state = s;
    onProgress && onProgress({ state: s, message: msg || describeState(s) });
  }

  function describeState(s){
    return ({
      'idle': 'Ready',
      'creating-offer': 'Creating connection code...',
      'waiting-answer': 'Waiting for answer code...',
      'creating-answer': 'Creating answer code...',
      'waiting-connect': 'Connecting...',
      'syncing': 'Syncing...',
      'done': 'Done',
      'error': 'Error',
    })[s] || s;
  }

  function describeDevice(){
    const ua = navigator.userAgent;
    let browser = 'Browser';
    if (/Edg\//.test(ua)) browser = 'Edge';
    else if (/Chrome\//.test(ua) && !/Edg\//.test(ua)) browser = 'Chrome';
    else if (/Firefox\//.test(ua)) browser = 'Firefox';
    else if (/Safari\//.test(ua) && !/Chrome\//.test(ua)) browser = 'Safari';
    let os = '';
    if (/Android/i.test(ua)) os = 'Android';
    else if (/iPhone|iPad|iPod/i.test(ua)) os = 'iOS';
    else if (/Windows/i.test(ua)) os = 'Windows';
    else if (/Macintosh|Mac OS/i.test(ua)) os = 'macOS';
    else if (/Linux/i.test(ua)) os = 'Linux';
    return os ? browser + ' on ' + os : browser;
  }

  /* ============================================================
     Public API
     ============================================================ */

  global.CortexSync = {
    // PeerJS quick flows
    startQuickHost,
    joinQuick,
    tryAutoReconnect,

    // Manual flows (fallback)
    startManualHost,
    acceptManualAnswer,
    joinManualOffer,

    // Shared
    reset,
    getDeviceCode,
    formatCode,
    normalizeCode,
    getPairedPeer,
    savePairedPeer,
    forgetPairedPeer,
    getAutoSync,
    setAutoSync,

    // Callbacks
    setProgressCallback: (fn) => { onProgress = fn; },
    setDoneCallback: (fn) => { onDone = fn; },
    setErrorCallback: (fn) => { onError = fn; },
    setNotesProvider: (fn) => { notesProvider = fn; },
    setNotesReceiver: (fn) => { notesReceiver = fn; },
    getState: () => state,
    getRole: () => role,
  };

})(window);
