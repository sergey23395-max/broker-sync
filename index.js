// Broker Sync — постоянное подключение к Telegram (личный аккаунт Сергея, официальный API через GramJS).
// Живёт на Render (бесплатный веб-сервис); Cloudflare будит его каждые 10 минут.
// Новые сообщения → в базу за секунды; при перезапуске догоняет пропущенное; раз в сутки — фото.
// Также: чтение присланных ссылок на объявления и поиск Property Finder (по одной странице, без обхода каталога).
import http from 'node:http';
import crypto from 'node:crypto';
import { TelegramClient, Api } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { NewMessage } from 'telegram/events/index.js';
import { Jimp } from 'jimp';

const PORT = Number(process.env.PORT || 10000);
const WORKER = (process.env.WORKER_URL || '').replace(/\/$/, '');
// Ключ связи: из SYNC_SECRET или выводится из SESSION_KEY (Render генерирует его сам) — вводить ничего не нужно.
const KEY = process.env.SYNC_SECRET || (process.env.SESSION_KEY ? crypto.createHash('sha256').update('sync:' + process.env.SESSION_KEY).digest('hex') : '');
let API_ID = Number(process.env.TG_API_ID || 0);
let API_HASH = process.env.TG_API_HASH || '';
const SESSION_KEY = crypto.createHash('sha256').update(process.env.SESSION_KEY || KEY).digest(); // ключ шифрования сессии
const SELF_URL = (process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36';

let client = null;
let authorized = false;
let lastMessageAt = null;
let login = null; // { phone, phoneCodeHash, client }
const startedAt = new Date().toISOString();
const log = (...a) => console.log(new Date().toISOString(), ...a);

// ——— Обмен с Cloudflare ———
async function w(path, opts = {}) {
  const r = await fetch(WORKER + path, {
    method: opts.method || (opts.body ? 'POST' : 'GET'),
    headers: { 'content-type': 'application/json', 'x-sync-key': KEY },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`${path}: ${r.status} ${j.error || ''}`);
  return j;
}

// Сессия Telegram хранится в KV Cloudflare только зашифрованной (AES-256-GCM, ключ — на Render).
function seal(s) {
  const iv = crypto.randomBytes(12);
  const c = crypto.createCipheriv('aes-256-gcm', SESSION_KEY, iv);
  const ct = Buffer.concat([c.update(s, 'utf8'), c.final()]);
  return Buffer.concat([iv, c.getAuthTag(), ct]).toString('base64');
}
function open(b64) {
  const b = Buffer.from(b64, 'base64');
  const d = crypto.createDecipheriv('aes-256-gcm', SESSION_KEY, b.subarray(0, 12));
  d.setAuthTag(b.subarray(12, 28));
  return Buffer.concat([d.update(b.subarray(28)), d.final()]).toString('utf8');
}

// ——— Telegram ———
function peerInfo(u) {
  return {
    tg_id: String(u.id), username: u.username || undefined, phone: u.phone || undefined,
    first_name: u.firstName || undefined, last_name: u.lastName || undefined,
    contact_name: u.contact ? [u.firstName, u.lastName].filter(Boolean).join(' ') : undefined,
  };
}
function msgText(m) {
  if (m.message) return m.message;
  if (m.media) {
    const k = m.media.className;
    if (k === 'MessageMediaPhoto') return '[фото]';
    if (k === 'MessageMediaDocument') return m.media.document?.mimeType?.startsWith('audio') ? '[голосовое]' : '[файл]';
    if (k === 'MessageMediaContact') return '[контакт]';
    if (k === 'MessageMediaGeo') return '[геолокация]';
    return '[медиа]';
  }
  return '';
}
const toMsg = (m, peerId) => ({ tg_id: String(peerId), out: !!m.out, text: msgText(m), at: new Date(m.date * 1000).toISOString(), id: m.id });

const queue = new Map(); // peerId → {peer, messages[]}
let flushTimer = null;
function enqueue(peer, msg) {
  const q = queue.get(peer.tg_id) || { peer, messages: [] };
  q.messages.push(msg);
  queue.set(peer.tg_id, q);
  clearTimeout(flushTimer);
  flushTimer = setTimeout(flush, 1500); // группируем пачки, задержка — секунды
}
async function flush() {
  if (!queue.size) return;
  const dialogs = [...queue.values()];
  queue.clear();
  for (let i = 0; i < dialogs.length; i += 20) {
    try { await w('/api/sync/messages', { body: { dialogs: dialogs.slice(i, i + 20) } }); }
    catch (e) { log('flush', e.message); for (const d of dialogs.slice(i, i + 20)) queue.set(d.peer.tg_id, d); setTimeout(flush, 15000); return; }
  }
  const last = dialogs.flatMap(d => d.messages).map(m => m.at).sort().pop();
  if (last) { lastMessageAt = last; await w('/api/sync/cursor', { method: 'PUT', body: { last } }).catch(() => {}); }
}

async function startClient(sessionStr) {
  client = new TelegramClient(new StringSession(sessionStr), API_ID, API_HASH, { connectionRetries: 10, autoReconnect: true });
  client.setLogLevel('error');
  await client.connect();
  authorized = await client.checkAuthorization();
  if (!authorized) { log('сессия недействительна'); return; }
  log('Telegram подключён');
  client.addEventHandler(async ev => {
    const m = ev.message;
    if (!ev.isPrivate) return;
    try {
      const u = await m.getChat();
      if (!u || u.bot || u.self) return;
      enqueue(peerInfo(u), toMsg(m, u.id));
    } catch (e) { log('event', e.message); }
  }, new NewMessage({}));
  catchUp().catch(e => log('catchUp', e.message));
  photosDaily();
}

/** Догнать пропущенное после перезапуска: диалоги, изменившиеся после последней отметки. */
async function catchUp() {
  const { cursor } = await w('/api/sync/cursor');
  const since = cursor?.last ? Date.parse(cursor.last) - 60_000 : Date.now() - 2 * 86400_000;
  let n = 0;
  for await (const d of client.iterDialogs({ limit: 300 })) {
    if (!d.isUser || !d.entity || d.entity.bot || d.entity.self) continue;
    const top = d.message?.date ? d.message.date * 1000 : 0;
    if (top < since) break; // диалоги отсортированы по свежести
    const peer = peerInfo(d.entity);
    const msgs = [];
    for await (const m of client.iterMessages(d.entity, { limit: 200 })) {
      if (m.date * 1000 < since) break;
      msgs.push(toMsg(m, d.entity.id));
    }
    for (const m of msgs.reverse()) enqueue(peer, m);
    n += msgs.length;
  }
  await flush();
  log('догнал сообщений:', n);
}

/** Раз в сутки — маленькие фото (64×64) тем, у кого их нет. */
async function photosDaily() {
  const run = async () => {
    if (!authorized) return;
    try {
      const { peers } = await w('/api/sync/peers');
      let ok = 0;
      for (const id of peers.slice(0, 300)) {
        try {
          const ent = await client.getEntity(BigInt(id));
          if (!ent.photo || ent.photo.className === 'UserProfilePhotoEmpty') continue;
          const buf = await client.downloadProfilePhoto(ent, { isBig: false });
          if (!buf || !buf.length) continue;
          const img = await Jimp.read(Buffer.from(buf));
          img.cover({ w: 64, h: 64 });
          const jpeg = await img.getBuffer('image/jpeg', { quality: 80 });
          await w('/api/sync/avatar', { body: { peer: peerInfo(ent), jpeg: jpeg.toString('base64') } });
          ok++;
          await new Promise(r => setTimeout(r, 400)); // бережно к лимитам Telegram
        } catch { /* следующий */ }
      }
      log('фото обновлено:', ok);
    } catch (e) { log('photos', e.message); }
  };
  setTimeout(run, 60_000);
  setInterval(run, 24 * 3600_000);
}

async function loadApiKeys() {
  if (process.env.TG_API_ID && process.env.TG_API_HASH) return;
  const c = await w('/api/sync/config').catch(() => ({}));
  if (c.api_id && c.api_hash) { API_ID = Number(c.api_id); API_HASH = c.api_hash; }
}

async function boot() {
  if (!WORKER || !KEY) { log('нет WORKER_URL / ключа связи'); return; }
  // Первый запуск: регистрируем свой адрес и ключ в Cloudflare (один раз).
  if (SELF_URL) {
    const r = await fetch(WORKER + '/api/sync/claim', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ url: SELF_URL, key: KEY }) }).catch(e => ({ status: 0, e }));
    log('регистрация в Cloudflare:', r.status);
  }
  await loadApiKeys();
  if (!API_ID || !API_HASH) { log('ключи Telegram ещё не введены в приложении — ждём'); return; }
  try {
    const { session } = await w('/api/sync/session');
    if (session) await startClient(open(session));
    else log('Telegram ещё не подключён — вход из приложения: Ещё → Синхронизация Telegram');
  } catch (e) { log('boot', e.message); }
}

// ——— Вход в Telegram (код вводит Сергей один раз из приложения) ———
async function doLogin(b) {
  if (!API_ID || !API_HASH) await loadApiKeys();
  if (!API_ID || !API_HASH) throw new Error('Сначала введите api_id и api_hash в приложении');
  if (b.phone) {
    const c = new TelegramClient(new StringSession(''), API_ID, API_HASH, { connectionRetries: 5 });
    c.setLogLevel('error');
    await c.connect();
    const r = await c.invoke(new Api.auth.SendCode({ phoneNumber: b.phone, apiId: API_ID, apiHash: API_HASH, settings: new Api.CodeSettings({}) }));
    login = { phone: b.phone, phoneCodeHash: r.phoneCodeHash, client: c };
    return { next: 'code' };
  }
  if (!login) throw new Error('Сначала номер телефона');
  const c = login.client;
  try {
    if (b.code) {
      await c.invoke(new Api.auth.SignIn({ phoneNumber: login.phone, phoneCodeHash: login.phoneCodeHash, phoneCode: String(b.code) }));
    } else if (b.password) {
      const { computeCheck } = await import('telegram/Password.js');
      const pwd = await c.invoke(new Api.account.GetPassword());
      await c.invoke(new Api.auth.CheckPassword({ password: await computeCheck(pwd, b.password) }));
    }
  } catch (e) {
    if (String(e.errorMessage || e.message).includes('SESSION_PASSWORD_NEEDED')) return { next: 'password' };
    throw new Error(e.errorMessage || e.message);
  }
  const s = c.session.save();
  await w('/api/sync/session', { method: 'PUT', body: { session: seal(s) } });
  await c.disconnect().catch(() => {});
  login = null;
  if (client) await client.disconnect().catch(() => {});
  await startClient(s);
  return { next: 'done' };
}

async function doLogout() {
  if (client) { await client.invoke(new Api.auth.LogOut()).catch(() => {}); await client.disconnect().catch(() => {}); }
  client = null; authorized = false;
  await w('/api/sync/session', { method: 'PUT', body: { session: '' } });
  return { ok: true };
}

// ——— Объявления по ссылке ———
function textOf(html) {
  return html.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n').replace(/<\/(p|div|li|h\d|tr)>/gi, '\n').replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/[ \t]+/g, ' ').replace(/\n\s*\n+/g, '\n').trim();
}
const SQFT = 0.092903;
function pfListing(p) {
  const area = p.size?.unit === 'sqft' ? Math.round(p.size.value * SQFT * 10) / 10 : p.size?.value;
  return {
    url: p.share_url || ('https://www.propertyfinder.ae' + (p.details_path || '')), external_id: String(p.id), title: p.title,
    addr: p.location?.full_name || '', priceNum: p.price?.value, currency: p.price?.currency || 'AED',
    price: p.price?.value ? `${String(p.price.value).replace(/\B(?=(\d{3})+(?!\d))/g, ' ')} ${p.price.currency}${p.price.period && p.price.period !== 'sell' ? ' / ' + p.price.period : ''}` : '',
    areaNum: area, area: area ? `${area} м²` : '', rooms: p.bedrooms === 'studio' ? 'Студия' : p.bedrooms ? `${p.bedrooms} спальни` : '',
    floor: '', features: (p.amenity_names || []).join(', '), facts: [p.completion_status === 'off_plan' ? 'Строится' : 'Готовый', p.furnished === 'YES' ? 'С мебелью' : ''].filter(Boolean).join('\n'),
    desc: p.description || '', photos: (Array.isArray(p.images) ? p.images : (p.images?.property || [])).slice(0, 12).map(i => i.full || i.medium || i.small), completion: p.completion_status,
  };
}
function nextData(html) {
  const m = /<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/.exec(html);
  try { return m ? JSON.parse(m[1]) : null; } catch { return null; }
}
async function get(url) {
  const r = await fetch(url, { headers: { 'user-agent': UA, 'accept-language': 'ru,en;q=0.8', accept: 'text/html,application/json' }, redirect: 'follow' });
  if (!r.ok) throw new Error(`Страница не отдаётся (${r.status}) — вставьте текст вручную`);
  return r.text();
}

async function fetchPage({ url }) {
  const html = await get(url);
  const nd = nextData(html);
  // Выдача Property Finder → список объектов страницы
  const list = nd?.props?.pageProps?.searchResult?.listings;
  if (Array.isArray(list) && list.length) return { listings: list.filter(x => x.property).map(x => pfListing(x.property)), total: nd.props.pageProps.searchResult.meta?.total_count };
  const pfOne = nd?.props?.pageProps?.propertyResult?.property;
  if (pfOne) { const l = pfListing(pfOne); return { fields: l, photos: l.photos, text: `${l.title}\n${l.addr}\n${l.desc}` }; }
  // Циан и прочие: og-теги + JSON-LD + текст
  const og = (p) => (new RegExp(`<meta[^>]+property=["']og:${p}["'][^>]+content=["']([^"']+)`, 'i').exec(html) || [])[1] || '';
  const flat = html.replace(/\\u002F/gi, '/').replace(/\\\//g, '/');
  const photos = [...new Set([...flat.matchAll(/https:\/\/[^"'\s]+?\.(?:jpe?g|webp)(?:\?[^"'\s]*)?/gi)].map(m => m[0])
    .filter(u => /cdn-cian|images\.cdn|static\.shared\.propertyfinder|bayut|avito|img/i.test(u) && !/avatar|logo|icon|thumb|promo|dummy|no_image/i.test(u)))].slice(0, 15);
  if (og('image') && !/dummy|no_image|logo/i.test(og('image'))) photos.unshift(og('image'));
  const text = [og('title'), og('description'), textOf(html).slice(0, 8000)].filter(Boolean).join('\n');
  const fields = /cian\.ru/.test(url) ? cianFields(flat) : { title: og('title') };
  const allPhotos = fields.photos?.length ? fields.photos : [...new Set(photos)].slice(0, 15);
  delete fields.photos;
  return { text, photos: allPhotos, fields, videos: findVideos(flat) };
}

/** Циан: данные объявления из JSON страницы → поля как в презентации Сергея (цена, этаж, площадь, ремонт, планировка). */
function cianFields(flat) {
  const num = k => { const m = new RegExp('"' + k + '":"?([0-9.]+)"?').exec(flat); return m ? Number(m[1]) : null; };
  const str = k => (new RegExp('"' + k + '":"([^"]{0,80})"').exec(flat) || [])[1] || '';
  const sp = n => String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
  const jk = (/"jk":\{[^{}]{0,300}?"name":"([^"]+)"/.exec(flat) || /"newbuilding":\{[^{}]{0,300}?"name":"([^"]+)"/.exec(flat) || [])[1] || '';
  const titleCase = t => t.replace(/\s*\(.*\)$/, '').toLowerCase().replace(/(^|[\s-])(\p{L})/gu, (_, a, b) => a + b.toUpperCase());
  const price = num('price'), floor = num('floorNumber'), floors = num('floorsCount'), area = num('totalArea'), rooms = num('roomsCount');
  const repair = { design: 'Дизайнерский ремонт', euro: 'Евроремонт', cosmetic: 'Косметический ремонт', no: 'Без ремонта' }[str('repairType')] || '';
  // Описание объявления (а не блоки банка): самый длинный "description" со словами про жильё
  const descs = [...flat.matchAll(/"description":"((?:[^"\\]|\\.){20,4000})"/g)].map(m => m[1].replace(/\\n/g, '\n'));
  const jkWord = (jk.split(/\s|\(/)[0] || '').toLowerCase();
  const own = descs.filter(d => /квартир|апартамент|комнат|спальн|сда[её]т|прода[её]т/i.test(d) && !/в народе|обычно в таких|кредит|рассрочк|ипотек/i.test(d));
  const desc = own.find(d => jkWord && d.toLowerCase().includes(jkWord)) || own.find(d => /^(сда|прода|предлага)/i.test(d.trim())) || own[0] || '';
  const isStudio = str('flatType') === 'studio' || /студи/i.test(desc.slice(0, 200));
  let layout = '';
  if (isStudio) layout = 'Студия';
  else if (rooms) layout = rooms === 1 ? 'Гостиная' : rooms === 2 ? 'Гостиная + спальня' : `Гостиная + ${rooms - 1} спальни`;
  if (layout && /гардероб/i.test(desc)) layout += ' + гардеробная';
  if (layout && /кабинет/i.test(desc)) layout += ' + кабинет';
  const deal = /"dealType":"rent"/.test(flat) || /"paymentPeriod":"monthly"/.test(flat) ? 'Аренда' : 'Продажа';
  return {
    title: jk ? titleCase(jk) : '',
    price: price ? sp(price) : '',
    priceNum: price || null, currency: '₽',
    floor: floor ? `${floor} этаж` : '', floors: floors || null,
    area: area ? `${String(area).replace('.', ',')} м²` : '', areaNum: area || null,
    rooms: layout, facts: repair, deal, cianDesc: desc,
    photos: [...new Set([...flat.matchAll(/"fullUrl":"(https:[^"]+?\.(?:jpe?g|webp))"/g)].map(m => m[1]))].slice(0, 40),
  };
}

/** Видео объявления: Циан хранит их в JSON страницы ("videos":[{id,url}]), чаще всего на Kinescope; иногда YouTube/Rutube. */
function findVideos(flat) {
  const out = new Map();
  for (const m of flat.matchAll(/"videos"\s*:\s*\[([^\]]{0,4000})\]/g)) {
    for (const u of m[1].matchAll(/"url"\s*:\s*"([^"]+)"/g)) out.set(u[1], { url: u[1] });
  }
  for (const m of flat.matchAll(/https:\/\/kinescope\.io\/(?:embed\/)?[0-9a-f-]{36}/g)) out.set(m[0], { url: m[0] });
  for (const m of flat.matchAll(/https:\/\/(?:www\.)?(?:youtube\.com\/(?:embed\/|watch\?v=)|youtu\.be\/)[\w-]{11}/g)) out.set(m[0], { url: m[0] });
  return [...out.values()].map(v => ({ ...v, kind: /kinescope/.test(v.url) ? 'kinescope' : /youtu/.test(v.url) ? 'youtube' : 'other' })).slice(0, 5);
}

/** Kinescope: master.m3u8 → лучшее качество → цельный mp4 (без звука) → обычный MP4 через ffmpeg (-c copy). */
async function kinescopeFile(url) {
  const id = (/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/.exec(url) || [])[1];
  if (!id) throw new Error('Не нашёл номер видео');
  const H = { 'user-agent': UA, referer: 'https://www.cian.ru/' };
  const master = await (await fetch(`https://kinescope.io/${id}/master.m3u8`, { headers: H })).text();
  const qs = [...master.matchAll(/RESOLUTION=(\d+)x(\d+)[^\n]*\n(media\.m3u8\?[^\n]*type=video[^\n]*)/g)]
    .map(m => ({ px: Number(m[1]) * Number(m[2]), path: m[3] })).sort((a, b) => b.px - a.px);
  if (!qs.length) throw new Error('Видео недоступно');
  const media = await (await fetch(`https://kinescope.io/${id}/${qs[0].path}`, { headers: H })).text();
  const file = (/EXT-X-MAP:URI="([^"]+)"/.exec(media) || /(https:\/\/[^\s?]+\.mp4)/.exec(media) || [])[1];
  if (!file) throw new Error('Не нашёл файл видео');
  return { file: file.split('?')[0], headers: H };
}

async function videoStream(req, res, body) {
  const { file, headers } = await kinescopeFile(body.url || '');
  const tmpIn = `/tmp/v-${Date.now()}.mp4`, tmpOut = tmpIn.replace('.mp4', '-out.mp4');
  const fs = await import('node:fs');
  const r = await fetch(file, { headers });
  if (!r.ok) throw new Error('Видео не скачалось: ' + r.status);
  fs.writeFileSync(tmpIn, Buffer.from(await r.arrayBuffer()));
  let path = tmpIn;
  try {
    const ff = (await import('ffmpeg-static')).default;
    const { execFileSync } = await import('node:child_process');
    execFileSync(ff, ['-y', '-i', tmpIn, '-c', 'copy', '-an', '-movflags', '+faststart', tmpOut], { stdio: 'ignore', timeout: 120000 });
    path = tmpOut;
  } catch (e) { log('ffmpeg', e.message); }
  const buf = fs.readFileSync(path);
  for (const f of [tmpIn, tmpOut]) fs.rmSync(f, { force: true });
  res.writeHead(200, { 'content-type': 'video/mp4', 'content-length': buf.length, 'content-disposition': 'attachment; filename="video.mp4"' });
  res.end(buf);
}

/** Поиск Property Finder по фильтрам: район, спальни, бюджет, покупка/аренда, готовое/строящееся. */
async function pfSearch(b) {
  let l = '';
  if (b.area) {
    const r = await fetch(`https://www.propertyfinder.ae/api/pwa/locations?locale=en&filters.name=${encodeURIComponent(b.area)}`, { headers: { 'user-agent': UA } });
    const j = await r.json().catch(() => ({}));
    l = j?.data?.attributes?.[0]?.id ? String(j.data.attributes[0].id) : '';
    if (!l) throw new Error(`Район «${b.area}» не найден на Property Finder`);
  }
  const q = new URLSearchParams({ c: b.deal === 'rent' ? '2' : '1', fu: '0', ob: 'mr' });
  if (l) q.set('l', l);
  if (b.beds !== undefined && b.beds !== '') q.append('bdr[]', String(b.beds));
  if (b.maxPrice) q.set('pt', String(b.maxPrice));
  if (b.minPrice) q.set('pf', String(b.minPrice));
  if (b.page) q.set('page', String(b.page));
  const url = `https://www.propertyfinder.ae/en/search?${q}`;
  const nd = nextData(await get(url));
  const sr = nd?.props?.pageProps?.searchResult;
  if (!sr) throw new Error('Property Finder не отдал выдачу — пришлите ссылку');
  let items = (sr.listings || []).filter(x => x.property).map(x => pfListing(x.property));
  if (b.status === 'ready') items = items.filter(i => i.completion !== 'off_plan');
  if (b.status === 'offplan') items = items.filter(i => i.completion === 'off_plan');
  return { url, total: sr.meta?.total_count || items.length, page: sr.meta?.page || 1, pages: sr.meta?.page_count || 1, items };
}

// ——— HTTP ———
const routes = {
  'GET /health': async () => ({ ok: true, authorized, lastMessageAt, startedAt, queue: queue.size }),
  'POST /login': doLogin,
  'POST /logout': doLogout,
  'POST /reload': async () => { await loadApiKeys(); if (!client && API_ID) boot(); return { ok: true, keys: !!API_ID }; },
  'POST /fetch': fetchPage,
  'POST /pf-search': pfSearch,
  'POST /pf-project': async ({ url }) => {
    // Страница проекта застройщика: полная галерея в оригинальном размере и описание.
    const nd = nextData(await get(url));
    const d = nd?.props?.pageProps?.detailResult || {};
    const images = (d.images || []).filter(i => i.type === 'image' || !i.type).map(i => String(i.source || i).replace(/\/[a-z]+\.webp$/, '/original.jpg'));
    const desc = textOf(String(d.description || '')).slice(0, 3000);
    return { images: [...new Set(images)].slice(0, 20), desc };
  },
  'POST /video': 'video',
  'POST /img': 'img',
};
http.createServer(async (req, res) => {
  const send = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json; charset=utf-8' }); res.end(JSON.stringify(obj)); };
  if (req.url === '/' ) return send(200, { service: 'broker-sync', ok: true });
  const h = routes[`${req.method} ${req.url.split('?')[0]}`];
  if (!h) return send(404, { error: 'not found' });
  const key = req.headers['x-sync-key'] || '';
  if (!KEY || key.length !== KEY.length || !crypto.timingSafeEqual(Buffer.from(key), Buffer.from(KEY))) return send(403, { error: 'forbidden' });
  let body = '';
  for await (const ch of req) { body += ch; if (body.length > 1e6) return send(413, { error: 'too big' }); }
  if (h === 'img') {
    try {
      const b = body ? JSON.parse(body) : {};
      const r = await fetch(b.url, { headers: { 'user-agent': UA, referer: 'https://www.cian.ru/', accept: 'image/avif,image/webp,image/*,*/*' } });
      const type = r.headers.get('content-type') || '';
      if (!r.ok || !type.startsWith('image/')) return send(400, { error: 'Фото не скачалось: ' + r.status });
      const buf = Buffer.from(await r.arrayBuffer());
      res.writeHead(200, { 'content-type': type, 'content-length': buf.length }); return res.end(buf);
    } catch (e) { return send(400, { error: e.message }); }
  }
  if (h === 'video') { try { return await videoStream(req, res, body ? JSON.parse(body) : {}); } catch (e) { return send(400, { error: e.message }); } }
  try { send(200, await h(body ? JSON.parse(body) : {})); }
  catch (e) { send(400, { error: e.message || String(e) }); }
}).listen(PORT, () => { log('broker-sync на порту', PORT); boot(); });
