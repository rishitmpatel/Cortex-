/* Cortex Sync v18 — Local QR pairing via compressed SDP. */

(function(global){
  'use strict';

  const ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
  ];

  const CHUNK_SIZE = 8000;
  const RECV_TIMEOUT_MS = 60000;
  const ICE_WAIT_MS = 3500;
  const CONNECT_TIMEOUT_MS = 30000;

  const PEERJS_CDN = 'https://unpkg.com/peerjs@1.5.4/dist/peerjs.min.js';
  const QRCODE_CDN = 'https://cdn.jsdelivr.net/npm/qrcode@1.5.3/build/qrcode.min.js';
  const JSQR_CDN   = 'https://cdn.jsdelivr.net/npm/jsqr@1.4.0/dist/jsQR.js';

  const PEER_PREFIX = 'cortexsync-';
  const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
  const CODE_LENGTH = 8;

  const STORE_DEVICE_CODE = 'cortex.device.code.v1';
  const STORE_PAIRED_PEER = 'cortex.paired.peer.v1';
  const STORE_AUTO_SYNC = 'cortex.autosync.v1';

  let channel = null;
  let role = null;
  let state = 'idle';

  let onProgress = null;
  let onDone = null;
  let onError = null;
  let notesProvider = null;
  let notesReceiver = null;

  let pjsPeer = null;
  let pjsConn = null;
  let peerjsLoaded = false;
  let peerjsLoading = null;

  let qrGenLoaded = false;
  let qrGenLoading = null;
  let qrScanLoaded = false;
  let qrScanLoading = null;

  let activeScanStop = null;
  let localPC = null;

  const logLines = [];
  function log(msg){
    const t = new Date().toLocaleTimeString('en-GB', { hour12: false });
    const line = '[' + t + '] ' + msg;
    logLines.push(line);
    if (logLines.length > 40) logLines.shift();
    try { console.log('[CortexSync]', msg); } catch(e){}
    onProgress && onProgress({ state: 'log', message: line, log: logLines.slice() });
  }
  function clearLog(){ logLines.length = 0; }

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

  function getPairedPeer(){
    try {
      const raw = localStorage.getItem(STORE_PAIRED_PEER);
      if (!raw) return null;
      const p = JSON.parse(raw);
      if (p && typeof p.code === 'string') return p;
    } catch(e){}
    return null;
  }

  function savePairedPeer(code, name){
    const existing = getPairedPeer() || {};
    try {
      localStorage.setItem(STORE_PAIRED_PEER, JSON.stringify({
        code: code || existing.code || '',
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

  function loadScript(src){
    return new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = src;
      s.onload = resolve;
      s.onerror = () => reject(new Error('Failed to load ' + src));
      document.head.appendChild(s);
    });
  }

  function loadPeerJS(){
    if (peerjsLoaded && typeof global.Peer === 'function') return Promise.resolve();
    if (peerjsLoading) return peerjsLoading;
    log('Loading PeerJS...');
    peerjsLoading = loadScript(PEERJS_CDN).then(() => {
      if (typeof global.Peer === 'function'){ peerjsLoaded = true; log('PeerJS ready'); return; }
      peerjsLoading = null;
      throw new Error('PeerJS loaded but window.Peer is missing.');
    });
    return peerjsLoading;
  }

  function loadQRGen(){
    if (qrGenLoaded && global.QRCode && typeof global.QRCode.toDataURL === 'function') return Promise.resolve();
    if (qrGenLoading) return qrGenLoading;
    qrGenLoading = loadScript(QRCODE_CDN).then(() => {
      if (global.QRCode && typeof global.QRCode.toDataURL === 'function'){ qrGenLoaded = true; return; }
      qrGenLoading = null;
      throw new Error('QRCode library loaded but API missing.');
    });
    return qrGenLoading;
  }

  function loadQRScan(){
    if (qrScanLoaded && typeof global.jsQR === 'function') return Promise.resolve();
    if (qrScanLoading) return qrScanLoading;
    qrScanLoading = loadScript(JSQR_CDN).then(() => {
      if (typeof global.jsQR === 'function'){ qrScanLoaded = true; return; }
      qrScanLoading = null;
      throw new Error('jsQR loaded but function is missing.');
    });
    return qrScanLoading;
  }

  const hasCompression = (typeof CompressionStream === 'function') && (typeof DecompressionStream === 'function');

  async function compressString(str){
    if (!hasCompression) return new TextEncoder().encode(str);
    const bytes = new TextEncoder().encode(str);
    const cs = new CompressionStream('gzip');
    const writer = cs.writable.getWriter();
    writer.write(bytes);
    writer.close();
    const buf = await new Response(cs.readable).arrayBuffer();
    return new Uint8Array(buf);
  }

  async function decompressBytes(bytes){
    if (!hasCompression) return new TextDecoder().decode(bytes);
    const ds = new DecompressionStream('gzip');
    const writer = ds.writable.getWriter();
    writer.write(bytes);
    writer.close();
    const buf = await new Response(ds.readable).arrayBuffer();
    return new TextDecoder().decode(buf);
  }

  function bytesToBase64Url(bytes){
    let bin = '';
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }

  function base64UrlToBytes(b64){
    let s = String(b64 || '').replace(/-/g, '+').replace(/_/g, '/');
    while (s.length % 4) s += '=';
    const bin = atob(s);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

/* SYNC PART 2 */
   async function encodeSDPForQR(sdp, tag){
    const payload = tag + ':' + sdp;
    const bytes = await compressString(payload);
    const b64 = bytesToBase64Url(bytes);
    return 'cortex://sdp?' + b64;
  }

  async function decodeSDPFromQR(text){
    const s = String(text || '');
    const m = s.match(/cortex:\/\/sdp\?([A-Za-z0-9\-_]+)/);
    if (!m) return null;
    try {
      const bytes = base64UrlToBytes(m[1]);
      const payload = await decompressBytes(bytes);
      const idx = payload.indexOf(':');
      if (idx < 0) return null;
      const tag = payload.slice(0, idx);
      const sdp = payload.slice(idx + 1);
      if (!sdp.startsWith('v=')) return null;
      return { tag, sdp };
    } catch(e){ return null; }
  }

  async function drawQR(text, imgEl, options){
    await loadQRGen();
    const opts = Object.assign({
      errorCorrectionLevel: 'L', width: 480, margin: 1,
      color: { dark: '#000000', light: '#ffffff' },
    }, options || {});
    return new Promise((resolve, reject) => {
      global.QRCode.toDataURL(text, opts, (err, url) => {
        if (err) return reject(err);
        if (imgEl){ imgEl.src = url; imgEl.alt = 'Pairing QR'; }
        resolve(url);
      });
    });
  }

  async function startScan(videoEl, onDetected, onError){
    stopScan();
    try { await loadQRScan(); }
    catch(err){ onError && onError(err); return null; }

    let stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: { ideal: 'environment' } },
        audio: false,
      });
    } catch(err){ onError && onError(err); return null; }

    videoEl.srcObject = stream;
    videoEl.setAttribute('playsinline', '');
    videoEl.setAttribute('autoplay', '');
    videoEl.setAttribute('muted', '');
    try { await videoEl.play(); } catch(e){}

    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d', { willReadFrequently: true });
    let running = true;

    function tick(){
      if (!running) return;
      if (videoEl.readyState === videoEl.HAVE_ENOUGH_DATA){
        const w = videoEl.videoWidth || 640;
        const h = videoEl.videoHeight || 480;
        const targetW = Math.min(720, w);
        const targetH = Math.round(h * (targetW / w));
        canvas.width = targetW;
        canvas.height = targetH;
        try {
          ctx.drawImage(videoEl, 0, 0, targetW, targetH);
          const imageData = ctx.getImageData(0, 0, targetW, targetH);
          const result = global.jsQR(imageData.data, targetW, targetH, {
            inversionAttempts: 'attemptBoth',
          });
          if (result && result.data){
            running = false;
            stop();
            onDetected(result.data);
            return;
          }
        } catch(e){}
      }
      requestAnimationFrame(tick);
    }

    function stop(){
      running = false;
      try { stream.getTracks().forEach(t => t.stop()); } catch(e){}
      try { videoEl.srcObject = null; } catch(e){}
      if (activeScanStop === stop) activeScanStop = null;
    }

    activeScanStop = stop;
    requestAnimationFrame(tick);
    return stop;
  }

  function stopScan(){
    if (activeScanStop){
      try { activeScanStop(); } catch(e){}
      activeScanStop = null;
    }
  }

  function makeRTCChannel(dc){
    const queue = []; const waiters = []; const chunks = new Map();
    let closed = false;

    function dispatch(msg){
      for (let i = 0; i < waiters.length; i++){
        if (waiters[i].type === msg.type){
          const w = waiters.splice(i, 1)[0];
          w.resolve(msg); return;
        }
      }
      queue.push(msg);
    }
    function receive(type){
      return new Promise((resolve, reject) => {
        for (let i = 0; i < queue.length; i++){
          if (queue[i].type === type){ resolve(queue.splice(i, 1)[0]); return; }
        }
        const w = { type, resolve, reject };
        waiters.push(w);
        setTimeout(() => {
          const i = waiters.indexOf(w);
          if (i >= 0){ waiters.splice(i, 1); reject(new Error('Timeout waiting for "' + type + '"')); }
        }, RECV_TIMEOUT_MS);
      });
    }
    function send(obj){
      const json = JSON.stringify(obj);
      if (json.length <= CHUNK_SIZE){ dc.send('M' + json); return; }
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
        const c1 = rest.indexOf(':'); const c2 = rest.indexOf(':', c1 + 1); const c3 = rest.indexOf(':', c2 + 1);
        if (c1 < 0 || c2 < 0 || c3 < 0) return;
        const id = rest.slice(0, c1);
        const idx = parseInt(rest.slice(c1 + 1, c2), 10);
        const total = parseInt(rest.slice(c2 + 1, c3), 10);
        const part = rest.slice(c3 + 1);
        let entry = chunks.get(id);
        if (!entry){ entry = { total, parts: new Array(total) }; chunks.set(id, entry); }
        entry.parts[idx] = part;
        let complete = true;
        for (let i = 0; i < entry.total; i++){ if (entry.parts[i] === undefined){ complete = false; break; } }
        if (complete){ chunks.delete(id); try { dispatch(JSON.parse(entry.parts.join(''))); } catch(err){} }
      }
    });
    dc.addEventListener('close', () => { closed = true; });
    return {
      send, receive,
      close: () => { try { dc.close(); } catch(e){} closed = true; },
      isClosed: () => closed,
    };
  }

  function makePeerJSChannel(conn){
    const queue = []; const waiters = [];
    let closed = false;
    function dispatch(msg){
      for (let i = 0; i < waiters.length; i++){
        if (waiters[i].type === msg.type){
          const w = waiters.splice(i, 1)[0];
          w.resolve(msg); return;
        }
      }
      queue.push(msg);
    }
    function receive(type){
      return new Promise((resolve, reject) => {
        for (let i = 0; i < queue.length; i++){
          if (queue[i].type === type){ resolve(queue.splice(i, 1)[0]); return; }
        }
        const w = { type, resolve, reject };
        waiters.push(w);
        setTimeout(() => {
          const i = waiters.indexOf(w);
          if (i >= 0){ waiters.splice(i, 1); reject(new Error('Timeout waiting for "' + type + '"')); }
        }, RECV_TIMEOUT_MS);
      });
    }
    conn.on('data', (obj) => {
      if (obj && typeof obj === 'object' && typeof obj.type === 'string'){ dispatch(obj); }
    });
    conn.on('close', () => { closed = true; });
    conn.on('error', (e) => { closed = true; log('conn error: ' + (e && e.message || e)); });
    return {
      send: (obj) => { try { conn.send(obj); } catch(e){} },
      receive,
      close: () => { try { conn.close(); } catch(e){} closed = true; },
      isClosed: () => closed,
    };
  }

  function waitForIce(conn){
    return new Promise(resolve => {
      if (conn.iceGatheringState === 'complete') return resolve();
      const done = () => {
        if (conn.iceGatheringState === 'complete'){
          conn.removeEventListener('icegatheringstatechange', done); resolve();
        }
      };
      conn.addEventListener('icegatheringstatechange', done);
      setTimeout(resolve, ICE_WAIT_MS);
    });
  }

  async function runSync(ch){
    try {
      log('Starting sync protocol...');
      const mine = await notesProvider();
      const deviceName = describeDevice();
      const myCode = getDeviceCode();
      log('Sending hello (' + mine.length + ' notes)...');

      ch.send({ type: 'hello', deviceName, deviceCode: myCode, count: mine.length });
      const theirHello = await ch.receive('hello');
      log('Peer identified as ' + (theirHello.deviceName || 'unknown'));

      setState('syncing', 'Sending ' + mine.length + ' notes...');
      ch.send({ type: 'notes', notes: mine });

      const theirsMsg = await ch.receive('notes');
      const theirs = Array.isArray(theirsMsg.notes) ? theirsMsg.notes : [];
      log('Received ' + theirs.length + ' notes. Merging...');

      const result = await notesReceiver(theirs);
      log('Merged: +' + result.added + ' ~' + result.updated + ' -' + result.deleted);

      if (theirHello.deviceCode){ savePairedPeer(theirHello.deviceCode, theirHello.deviceName); }

      ch.send({ type: 'done', result });
      await ch.receive('done');
      log('Sync complete.');

      setState('done', 'Sync complete.');
      onDone && onDone({
        local: result, remote: {},
        peerDevice: theirHello.deviceName || 'peer',
      });
      setTimeout(() => { try { ch.close(); } catch(e){} }, 400);
    } catch (err){
      log('Sync protocol failed: ' + (err && err.message ? err.message : err));
      fail(err);
    }
  }

  async function startLocalHost(){
    reset();
    clearLog();
    role = 'host';
    setState('creating-offer', 'Creating offer...');
    log('Local QR mode: creating RTCPeerConnection');

    localPC = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    localPC.oniceconnectionstatechange = () => {
      log('ICE: ' + localPC.iceConnectionState);
      if (localPC.iceConnectionState === 'failed'){
        fail(new Error('ICE failed. Are both devices on the same Wi-Fi?'));
      }
    };

    const dc = localPC.createDataChannel('sync', { ordered: true });
    dc.addEventListener('open', () => {
      log('Data channel open');
      channel = makeRTCChannel(dc);
      setState('syncing', 'Connected. Starting sync...');
      runSync(channel).catch(err => fail(err));
    });

    const offer = await localPC.createOffer();
    await localPC.setLocalDescription(offer);
    await waitForIce(localPC);

    const sdp = localPC.localDescription.sdp;
    log('Offer SDP: ' + sdp.length + ' chars');
    const qrText = await encodeSDPForQR(sdp, 'offer');
    log('QR payload: ' + qrText.length + ' chars');
    return qrText;
  }

  async function consumeLocalAnswer(qrText){
    const decoded = await decodeSDPFromQR(qrText);
    if (!decoded) throw new Error('That QR is not a Cortex answer.');
    if (decoded.tag !== 'answer') throw new Error('That QR is a "' + decoded.tag + '", not an answer.');
    log('Answer decoded: ' + decoded.sdp.length + ' chars');
    setState('waiting-connect', 'Connecting...');
    await localPC.setRemoteDescription({ type: 'answer', sdp: decoded.sdp });
  }

  async function startLocalGuest(){
    reset();
    clearLog();
    role = 'guest';
    setState('creating-answer', 'Waiting for offer...');
    log('Local QR mode: guest waiting for host QR');

    localPC = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    localPC.oniceconnectionstatechange = () => {
      log('ICE: ' + localPC.iceConnectionState);
      if (localPC.iceConnectionState === 'failed'){
        fail(new Error('ICE failed. Are both devices on the same Wi-Fi?'));
      }
    };
    localPC.ondatachannel = (e) => {
      const dc = e.channel;
      dc.addEventListener('open', () => {
        log('Data channel open');
        channel = makeRTCChannel(dc);
        setState('syncing', 'Connected. Starting sync...');
        runSync(channel).catch(err => fail(err));
      });
    };
  }

/* SYNC PART 3 */
   async function consumeLocalOffer(qrText){
    const decoded = await decodeSDPFromQR(qrText);
    if (!decoded) throw new Error('That QR is not a Cortex offer.');
    if (decoded.tag !== 'offer') throw new Error('That QR is a "' + decoded.tag + '", not an offer.');
    log('Offer decoded: ' + decoded.sdp.length + ' chars');

    await localPC.setRemoteDescription({ type: 'offer', sdp: decoded.sdp });
    const answer = await localPC.createAnswer();
    await localPC.setLocalDescription(answer);
    await waitForIce(localPC);

    const sdp = localPC.localDescription.sdp;
    log('Answer SDP: ' + sdp.length + ' chars');
    const qrText = await encodeSDPForQR(sdp, 'answer');
    log('QR payload: ' + qrText.length + ' chars');
    setState('waiting-connect', 'Show this QR to the host, then wait...');
    return qrText;
  }

  async function startQuickHost(){
    await loadPeerJS();
    reset();
    clearLog();
    role = 'host';

    const myCode = getDeviceCode();
    const peerId = PEER_PREFIX + myCode;
    log('PeerJS host: ' + formatCode(myCode));
    setState('waiting-connect', 'Waiting for the other device...');

    return new Promise((resolve, reject) => {
      let settled = false;
      pjsPeer = new global.Peer(peerId, { debug: 0 });
      pjsPeer.on('open', () => {
        log('Signaling server: connected');
        if (settled) return;
        settled = true;
        resolve({ code: myCode });
      });
      pjsPeer.on('connection', (conn) => {
        log('Incoming connection');
        pjsConn = conn;
        channel = makePeerJSChannel(conn);
        conn.on('open', () => {
          log('Data channel open');
          setState('syncing', 'Connected. Starting sync...');
          runSync(channel).catch(err => fail(err));
        });
      });
      pjsPeer.on('error', (err) => {
        const msg = err && err.type ? err.type : (err && err.message ? err.message : String(err));
        log('Peer error: ' + msg);
        if (!settled){ settled = true; reject(err); }
        else { fail(err); }
      });
    });
  }

  async function joinQuick(code){
    await loadPeerJS();
    reset();
    clearLog();
    role = 'guest';
    const norm = normalizeCode(code);
    const target = PEER_PREFIX + norm;
    log('PeerJS guest: connecting to ' + formatCode(norm));
    setState('waiting-connect', 'Connecting...');

    return await new Promise((resolve, reject) => {
      let settled = false;
      pjsPeer = new global.Peer({ debug: 0 });
      pjsPeer.on('open', () => {
        log('Signaling server: connected');
        const conn = pjsPeer.connect(target, { reliable: true });
        pjsConn = conn;
        channel = makePeerJSChannel(conn);
        conn.on('open', () => {
          log('Data channel open');
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
          try { pjsPeer.destroy(); } catch(e){}
          reject(new Error('Timed out. Try Local QR if both devices are on the same Wi-Fi.'));
        }, CONNECT_TIMEOUT_MS);
      });
      pjsPeer.on('error', (err) => {
        const msg = err && err.type ? err.type : (err && err.message ? err.message : String(err));
        log('Peer error: ' + msg);
        if (settled) return;
        settled = true;
        if (err && err.type === 'peer-unavailable'){
          reject(new Error('No device is online with code ' + formatCode(norm) + '.'));
        } else {
          reject(new Error('Signaling failed: ' + msg));
        }
      });
    });
  }

  async function tryAutoReconnect(){
    if (!getAutoSync()) return { ok: false, reason: 'auto-sync disabled' };
    if (state === 'syncing' || state === 'waiting-connect') return { ok: false, reason: 'busy' };
    const paired = getPairedPeer();
    if (!paired || !paired.code) return { ok: false, reason: 'no paired peer' };

    try { await loadPeerJS(); }
    catch(e){ return { ok: false, reason: 'PeerJS unavailable' }; }

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
          log('Auto-sync: connected');
          setState('syncing', 'Auto-syncing...');
          runSync(channel).catch(() => {});
          done({ ok: true });
        });
        setTimeout(() => {
          if (settled) return;
          try { conn.close(); } catch(e){}
          try { pjsPeer.destroy(); } catch(e){}
          reset(); setState('idle', '');
          done({ ok: false, reason: 'timeout' });
        }, 8000);
      });
      pjsPeer.on('error', () => {
        try { pjsPeer.destroy(); } catch(e){}
        reset(); setState('idle', '');
        done({ ok: false, reason: 'peer unavailable' });
      });
    });
  }

  function fail(err){
    setState('error', err && err.message ? err.message : String(err));
    onError && onError(err);
    try { channel && channel.close(); } catch(e){}
    try { pjsConn && pjsConn.close(); } catch(e){}
    try { pjsPeer && pjsPeer.destroy(); } catch(e){}
    try { localPC && localPC.close(); } catch(e){}
    pjsConn = null; pjsPeer = null; localPC = null; channel = null;
  }

  function reset(){
    stopScan();
    try { channel && channel.close(); } catch(e){}
    try { pjsConn && pjsConn.close(); } catch(e){}
    try { pjsPeer && pjsPeer.destroy(); } catch(e){}
    try { localPC && localPC.close(); } catch(e){}
    channel = null; pjsConn = null; pjsPeer = null; localPC = null;
    state = 'idle';
  }

  function setState(s, msg){
    state = s;
    onProgress && onProgress({ state: s, message: msg || describeState(s), log: logLines.slice() });
  }

  function describeState(s){
    return ({
      'idle': 'Ready',
      'creating-offer': 'Creating offer...',
      'creating-answer': 'Creating answer...',
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

  global.CortexSync = {
    startLocalHost, consumeLocalAnswer, startLocalGuest, consumeLocalOffer,
    startQuickHost, joinQuick, tryAutoReconnect,
    drawQR, startScan, stopScan, encodeSDPForQR, decodeSDPFromQR,
    reset, getDeviceCode, formatCode, normalizeCode,
    getPairedPeer, savePairedPeer, forgetPairedPeer,
    getAutoSync, setAutoSync,
    getLog: () => logLines.slice(),
    clearLog,
    setProgressCallback: (fn) => { onProgress = fn; },
    setDoneCallback: (fn) => { onDone = fn; },
    setErrorCallback: (fn) => { onError = fn; },
    setNotesProvider: (fn) => { notesProvider = fn; },
    setNotesReceiver: (fn) => { notesReceiver = fn; },
    getState: () => state,
    getRole: () => role,
  };

})(window);
