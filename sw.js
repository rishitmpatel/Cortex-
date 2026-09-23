/* Cortex service worker */

const CACHE_NAME = 'cortex-v3';

const APP_SHELL = [
  './',
  './index.html',
  './manifest.json',
  './icon.svg',
  './icon-maskable.svg',
];

const CDN_HOSTS = ['cdn.jsdelivr.net', 'unpkg.com'];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  if (CDN_HOSTS.includes(url.hostname)){
    event.respondWith(cacheFirst(req));
    return;
  }
  if (url.origin === self.location.origin){
    event.respondWith(staleWhileRevalidate(req));
    return;
  }
});

async function cacheFirst(req){
  const cache = await caches.open(CACHE_NAME);
  const hit = await cache.match(req, { ignoreVary: true });
  if (hit) return hit;
  const resp = await fetch(req);
  if (resp && resp.ok) cache.put(req, resp.clone());
  return resp;
}

async function staleWhileRevalidate(req){
  const cache = await caches.open(CACHE_NAME);
  const hit = await cache.match(req);
  const network = fetch(req)
    .then((resp) => {
      if (resp && resp.ok) cache.put(req, resp.clone());
      return resp;
    })
    .catch(() => hit);
  return hit || network;
}
