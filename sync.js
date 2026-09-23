/* Cortex Sync — WebRTC LAN peer-to-peer note sync.
   No signaling server. Manual copy-paste of two SDP codes.
   Merge policy: last-write-wins by `updated` timestamp. */

(function(global){
  'use strict';

  const ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
  ];

  const CHUNK_SIZE = 8000;   // keep well under the smallest browser message cap
  const RECV_TIMEOUT_MS = 60000;
  const ICE_WAIT_MS = 3500;

  let pc = null;
  let dc = null;
  let role = null;                     // 'host' | 'guest'
  let state = 'idle';

  let onProgress = null;
  let onDone = null;
  let onError = null;
  let notesProvider = null;            // async () => notes[]  (including tombstones)
  let notesReceiver = null;            // async (theirs[]) => { added, updated, deleted, total }

  // ---- chunked message transport ----
  const incomingChunks = new Map();
  const incomingQueue = [];
  const incomingWaiters = [];

  function sendChunked(obj){
    if (!dc || dc.readyState !== 'open') throw new Error('Data channel not open');
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

  function dispatchIncoming(msg){
    for (let i = 0; i < incomingWaiters.length; i++){
      if (incomingWaiters[i].type === msg.type){
        const w = incomingWaiters.splice(i, 1)[0];
        w.resolve(msg);
        return;
      }
    }
    incomingQueue.push(msg);
  }

  function handleChannelData(e){
    const data = e.data;
    if (typeof data !== 'string') return;

    if (data[0] === 'M'){
      try { dispatchIncoming(JSON.parse(data.slice(1))); } catch(err){}
      return;
    }
    if (data[0] === 'C'){
      const rest = data.slice(1);
      const c1 = rest.indexOf(':');
      const c2 = rest.indexOf(':', c1 + 1);
      const c3 = rest.indexOf(':', c2 + 1);
      if (c1 < 0 || c2 < 0 || c3 < 0) return;
      const id = rest.slice(0, c1);
      const idx = parseInt(rest.slice(c1 + 1, c2), 10);
      const total = parseInt(rest.slice(c2 + 1, c3), 10);
      const part = rest.slice(c3 + 1);
      let entry = incomingChunks.get(id);
      if (!entry){ entry = { total, parts: new Array(total) }; incomingChunks.set(id, entry); }
      entry.parts[idx] = part;
      for (let i = 0; i < entry.total; i++){
        if (entry.parts[i] === undefined) return;
      }
      incomingChunks.delete(id);
      try { dispatchIncoming(JSON.parse(entry.parts.join(''))); } catch(err){}
    }
  }

  function receiveMessage(expectedType){
    return new Promise((resolve, reject) => {
      for (let i = 0; i < incomingQueue.length; i++){
        if (incomingQueue[i].type === expectedType){
          resolve(incomingQueue.splice(i, 1)[0]);
          return;
        }
      }
      const waiter = { type: expectedType, resolve, reject };
      incomingWaiters.push(waiter);
      setTimeout(() => {
        const i = incomingWaiters.indexOf(waiter);
        if (i >= 0){
          incomingWaiters.splice(i, 1);
          reject(new Error('Timeout waiting for "' + expectedType + '"'));
        }
      }, RECV_TIMEOUT_MS);
    });
  }

  // ---- base64 url-safe codec for SDP ----
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

  // ---- peer connection ----
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

  function attachDataChannel(channel){
    dc = channel;
    dc.binaryType = 'arraybuffer';
    dc.onopen = () => {
      setState('syncing', 'Connected. Starting sync...');
      runSyncProtocol().catch(err => fail(err));
    };
    dc.onerror = () => { /* surfaced via timeout or icestatechange */ };
    dc.addEventListener('message', handleChannelData);
  }

  function createPeerConnection(){
    const conn = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    conn.oniceconnectionstatechange = () => {
      const st = conn.iceConnectionState;
      if (st === 'failed'){
        fail(new Error('ICE connection failed. Are both devices on the same Wi-Fi?'));
      }
    };
    conn.ondatachannel = (e) => attachDataChannel(e.channel);
    return conn;
  }

  // ---- public flows ----

  async function startHost(){
    reset();
    role = 'host';
    setState('creating-offer', 'Creating connection code...');
    pc = createPeerConnection();
    const channel = pc.createDataChannel('sync', { ordered: true });
    attachDataChannel(channel);

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await waitForIce(pc);

    setState('waiting-answer', 'Waiting for the other device to answer...');
    return encodeCode(pc.localDescription.sdp);
  }

  async function acceptAnswer(code){
    if (!pc || role !== 'host') throw new Error('Not hosting');
    let sdp;
    try { sdp = decodeCode(code); }
    catch(e){ throw new Error('That answer code is malformed.'); }
    if (sdp.indexOf('v=0') !== 0 && sdp.indexOf('v=0') === -1){
      throw new Error('That does not look like a valid code.');
    }
    setState('waiting-connect', 'Connecting...');
    await pc.setRemoteDescription({ type: 'answer', sdp });
  }

  async function joinWithOffer(code){
    reset();
    role = 'guest';
    setState('creating-answer', 'Reading host code...');
    let sdp;
    try { sdp = decodeCode(code); }
    catch(e){ throw new Error('That host code is malformed.'); }
    pc = createPeerConnection();
    await pc.setRemoteDescription({ type: 'offer', sdp });
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    await waitForIce(pc);
    setState('waiting-connect', 'Send the answer code back, then wait...');
    return encodeCode(pc.localDescription.sdp);
  }

  // ---- sync protocol ----
  // 1. Both sides send { type:'hello', deviceName, count }
  // 2. Both sides send { type:'notes', notes:[...] }   (in chunks if needed)
  // 3. Both sides merge, both send { type:'done', result:{...} }
  // 4. Both sides wait for the other's 'done'
  // 5. Both close

  async function runSyncProtocol(){
    const mine = await notesProvider();
    const deviceName = describeDevice();

    sendChunked({ type: 'hello', deviceName, count: mine.length });
    const theirHello = await receiveMessage('hello');
    setState('syncing', 'Connected to ' + (theirHello.deviceName || 'peer') + '. Sending notes...');

    // Send our notes
    sendChunked({ type: 'notes', notes: mine });

    // Receive theirs
    const theirsMsg = await receiveMessage('notes');
    const theirs = Array.isArray(theirsMsg.notes) ? theirsMsg.notes : [];

    setState('syncing', 'Merging ' + theirs.length + ' notes from peer...');
    const result = await notesReceiver(theirs);

    sendChunked({ type: 'done', result });
    const theirDone = await receiveMessage('done');

    setState('done', 'Sync complete.');
    onDone && onDone({
      local: result,
      remote: theirDone.result || {},
      peerDevice: theirHello.deviceName || 'peer',
    });

    setTimeout(() => {
      try { dc && dc.close(); } catch(e){}
      try { pc && pc.close(); } catch(e){}
    }, 400);
  }

  function fail(err){
    setState('error', err && err.message ? err.message : String(err));
    onError && onError(err);
    try { dc && dc.close(); } catch(e){}
    try { pc && pc.close(); } catch(e){}
  }

  function reset(){
    try { dc && dc.close(); } catch(e){}
    try { pc && pc.close(); } catch(e){}
    pc = null; dc = null;
    incomingChunks.clear();
    incomingQueue.length = 0;
    incomingWaiters.length = 0;
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

  global.CortexSync = {
    startHost,
    acceptAnswer,
    joinWithOffer,
    reset,
    setProgressCallback: (fn) => { onProgress = fn; },
    setDoneCallback: (fn) => { onDone = fn; },
    setErrorCallback: (fn) => { onError = fn; },
    setNotesProvider: (fn) => { notesProvider = fn; },
    setNotesReceiver: (fn) => { notesReceiver = fn; },
    getState: () => state,
    getRole: () => role,
  };

})(window);
