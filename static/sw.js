/* ═══════════════════════════════════════════════════════════════════
   Elshan & Anil Messenger — Service Worker
   - نمایش نوتیفیکیشن وقتی سایت بازه ولی فوکوس نداره
   - کش کردن فایل‌های اصلی برای آفلاین
   ═══════════════════════════════════════════════════════════════════ */

const CACHE_NAME = 'messenger-v1';
const PRECACHE_URLS = [
  '/',
  '/login',
  'https://cdn.socket.io/4.7.5/socket.io.min.js',
  'https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;500;600;700;800;900&display=swap',
];

/* ── نصب: پیش‌کش ── */
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(PRECACHE_URLS).catch((err) => {
        console.warn('[SW] precache failed partially:', err);
      });
    }).then(() => self.skipWaiting())
  );
});

/* ── فعال‌سازی: پاک کردن کش قدیمی ── */
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

/* ── Fetch: استراتژی network-first برای API، cache-first برای استاتیک ── */
self.addEventListener('fetch', (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // درخواست‌های SocketIO / API رو کش نکن
  if (
    url.pathname.startsWith('/socket.io') ||
    url.pathname.startsWith('/api/') ||
    req.method !== 'GET'
  ) {
    return;
  }

  // برای فایل‌های استاتیک: cache-first
  if (
    url.pathname.startsWith('/static/') ||
    url.hostname.includes('fonts.g') ||
    url.hostname.includes('cdn.socket.io') ||
    url.hostname.includes('cdn.imgurl.ir')
  ) {
    event.respondWith(
      caches.match(req).then((cached) => {
        if (cached) return cached;
        return fetch(req).then((res) => {
          const copy = res.clone();
          caches.open(CACHE_NAME).then((c) => c.put(req, copy)).catch(() => {});
          return res;
        }).catch(() => cached);
      })
    );
    return;
  }

  // بقیه: network-first با fallback به کش
  event.respondWith(
    fetch(req).then((res) => {
      const copy = res.clone();
      caches.open(CACHE_NAME).then((c) => c.put(req, copy)).catch(() => {});
      return res;
    }).catch(() => caches.match(req).then((cached) => cached || caches.match('/')))
  );
});

/* ── دریافت پیام از کلاینت برای نمایش نوتیفیکیشن ── */
self.addEventListener('message', (event) => {
  const data = event.data || {};

  if (data.type === 'SHOW_NOTIFICATION') {
    const { title, body, icon, tag, data: notifData } = data.payload || {};

    event.waitUntil(
      self.registration.showNotification(title || 'پیام جدید', {
        body: body || '',
        icon: icon || '/favicon.ico',
        badge: '/favicon.ico',
        tag: tag || 'messenger-message',
        renotify: true,
        requireInteraction: false,
        dir: 'rtl',
        lang: 'fa',
        vibrate: [80, 40, 80],
        data: notifData || {},
      })
    );
  }

  if (data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

/* ── کلیک روی نوتیفیکیشن: باز کردن / فوکوس کردن تب ── */
self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const notifData = event.notification.data || {};
  const targetUrl = notifData.url || '/';

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      // اگه تبی از سایت بازه، فوکوسش کن
      for (const client of clients) {
        try {
          const clientUrl = new URL(client.url);
          if (clientUrl.origin === self.location.origin) {
            if ('focus' in client) {
              client.postMessage({
                type: 'NOTIFICATION_CLICKED',
                data: notifData,
              });
              return client.focus();
            }
          }
        } catch (e) {}
      }
      // وگرنه تب جدید باز کن
      if (self.clients.openWindow) {
        return self.clients.openWindow(targetUrl);
      }
    })
  );
});

/* ── بستن نوتیفیکیشن ── */
self.addEventListener('notificationclose', (event) => {
  // اختیاری: می‌تونی رویداد رو به کلاینت بفرستی
});
