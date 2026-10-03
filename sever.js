/**
 * =====================================================================
 *  T-REX LIVE — BACKEND (server.js)
 *  ---------------------------------------------------------------------
 *  - Express      : phục vụ giao diện game trong thư mục /public
 *  - Socket.io    : đẩy sự kiện quà tặng xuống trình duyệt theo thời gian thực
 *  - tiktok-live-connector (v2) : lắng nghe sự kiện từ phòng live TikTok
 *
 *  Luồng dữ liệu:
 *    TikTok LIVE ──(gift)──► server.js ──("tiktok-gift")──► game.js (Canvas)
 *
 *  Chạy:        npm start            (hoặc: npm run start:env để đọc file .env)
 *  Biến môi trường (đều tuỳ chọn):
 *    PORT=3000
 *    TIKTOK_USERNAME=ten_kenh   -> tự kết nối ngay khi server khởi động
 *    EULER_API_KEY=xxxx         -> API key Euler Stream (tăng giới hạn ký kết nối)
 *    ALLOW_SIMULATE=false       -> tắt tính năng giả lập quà từ trình duyệt
 * =====================================================================
 */

import express from 'express';
import { createServer } from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Server } from 'socket.io';
import { TikTokLiveConnection, WebcastEvent, ControlEvent } from 'tiktok-live-connector';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/* ------------------------------------------------------------------ */
/* 1. CẤU HÌNH                                                         */
/* ------------------------------------------------------------------ */
const PORT = Number(process.env.PORT) || 3000;
const TIKTOK_USERNAME = (process.env.TIKTOK_USERNAME || '').trim();
const EULER_API_KEY = (process.env.EULER_API_KEY || '').trim() || undefined;
const ALLOW_SIMULATE = process.env.ALLOW_SIMULATE !== 'false';

const RECONNECT_DELAY_MS = 10_000;   // chờ trước khi kết nối lại (tránh bị rate-limit)
const RECONNECT_MAX_ATTEMPTS = 5;    // số lần tự kết nối lại tối đa khi rớt mạng
const STREAK_TTL_MS = 60_000;        // combo quà quá 60s không cập nhật => coi như đã xong

// Username TikTok hợp lệ: chữ, số, dấu chấm, gạch dưới (2–24 ký tự)
const USERNAME_PATTERN = /^[A-Za-z0-9._]{2,24}$/;

/* ------------------------------------------------------------------ */
/* 2. KHỞI TẠO EXPRESS + SOCKET.IO                                     */
/* ------------------------------------------------------------------ */
const app = express();
const httpServer = createServer(app);
// cors: cho phép file index.html mở từ nơi khác (VD: GitHub Pages) kết nối tới server này
const io = new Server(httpServer, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

// Cho phép đặt file index.html (bản gộp 1 file) nằm ngay cạnh server.js – tiện khi tải lên GitHub bằng điện thoại
const ROOT_INDEX = path.join(__dirname, 'index.html');
app.get('/', (_req, res, next) => (fs.existsSync(ROOT_INDEX) ? res.sendFile(ROOT_INDEX) : next()));

// API nhỏ để kiểm tra nhanh trạng thái: http://localhost:3000/api/status
app.get('/api/status', (_req, res) => res.json(liveState));

/* ------------------------------------------------------------------ */
/* 3. TRẠNG THÁI KẾT NỐI TIKTOK                                        */
/* ------------------------------------------------------------------ */
const liveState = {
  status: 'idle',      // idle | connecting | connected | reconnecting | ended | error
  username: '',
  roomId: null,
  message: 'Chưa kết nối phòng live nào.',
};

let tiktokConnection = null;   // đối tượng TikTokLiveConnection đang dùng
let connectionToken = 0;       // tăng mỗi lần kết nối mới => sự kiện của kết nối cũ bị bỏ qua
let reconnectTimer = null;
let reconnectAttempts = 0;
let manualDisconnect = false;  // true khi người dùng chủ động ngắt => không tự kết nối lại

/**
 * Bộ nhớ combo quà. Key = "userId:giftId", value = { count, time }.
 * Dùng để tính "số quà MỚI" trong mỗi sự kiện của một combo (xem computeNewGiftCount).
 */
const streakTracker = new Map();

/** Cập nhật trạng thái + báo cho mọi trình duyệt đang mở */
function setStatus(patch) {
  Object.assign(liveState, patch);
  io.emit('tiktok-status', { ...liveState });
  console.log(`[TikTok] ${liveState.status.toUpperCase()} – ${liveState.message}`);
}

/** Chấp nhận "@ten_kenh", "ten_kenh" hoặc link "https://www.tiktok.com/@ten_kenh/live" */
export function normalizeUsername(input) {
  let value = String(input ?? '').trim();
  const fromUrl = value.match(/tiktok\.com\/@([^/?#\s]+)/i);
  if (fromUrl) value = fromUrl[1];
  return value.replace(/^@+/, '').trim();
}

/** Đổi lỗi kỹ thuật thành thông báo dễ hiểu */
function describeError(err) {
  const name = err?.name || err?.constructor?.name || '';
  const msg = err?.message || String(err);
  if (/UserOffline/i.test(name) || /offline|isn't online|not.*live/i.test(msg)) {
    return 'Kênh này hiện KHÔNG phát live (hoặc username sai).';
  }
  if (/rate.?limit|429/i.test(msg)) {
    return 'Bị giới hạn tần suất (rate limit) từ máy chủ ký kết nối. Đợi vài phút hoặc khai báo EULER_API_KEY.';
  }
  return msg;
}

/* ------------------------------------------------------------------ */
/* 4. XỬ LÝ DỮ LIỆU QUÀ TẶNG                                           */
/* ------------------------------------------------------------------ */

/**
 * Trích xuất thông tin cần thiết từ sự kiện quà của tiktok-live-connector v2.
 *  - v2 đặt thông tin người tặng trong data.user và thông tin quà trong data.giftDetails.
 *  - Có fallback sang các trường kiểu cũ (v1) / extendedGiftInfo để code không vỡ khi thư viện đổi cấu trúc.
 */
export function extractGiftInfo(data = {}) {
  const user = data.user ?? {};
  const details = data.giftDetails ?? {};
  const extended = data.extendedGiftInfo ?? {};
  const giftId = Number(data.giftId ?? details.id ?? extended.id ?? 0);

  return {
    uniqueId: user.uniqueId ?? data.uniqueId ?? 'unknown',     // tên người tặng (@id)
    nickname: user.nickname ?? data.nickname ?? '',            // tên hiển thị
    userId: String(user.userId ?? data.userId ?? ''),
    giftId,
    giftName: details.giftName ?? extended.name ?? data.giftName ?? `Gift#${giftId}`,
    giftType: Number(details.giftType ?? data.giftType ?? 0),  // 1 = quà có thể tặng combo
    diamondCount: Number(details.diamondCount ?? extended.diamond_count ?? data.diamondCount ?? 0),
    repeatCount: Math.max(1, Number(data.repeatCount) || 1),   // số lượng quà (tổng trong combo)
    repeatEnd: Boolean(data.repeatEnd),                         // true = combo đã kết thúc
  };
}

/**
 * Tính số quà MỚI cần xử lý ở sự kiện hiện tại.
 *
 * Vì sao cần? Với quà combo (giftType = 1, ví dụ Rose), TikTok gửi sự kiện LIÊN TỤC
 * trong lúc người xem bấm combo, repeatCount tăng dần 1 → 2 → 3..., và cuối cùng gửi
 * thêm 1 sự kiện repeatEnd = true với repeatCount cuối cùng.
 * Nếu cứ thấy sự kiện là cộng repeatCount thì sẽ bị cộng TRÙNG (1+2+3+3 = 9 thay vì 3).
 *
 * Cách làm: nhớ repeatCount lần trước của (người tặng, loại quà) và chỉ gửi xuống game
 * phần chênh lệch => game phản ứng NGAY từng quà mà không bị đếm trùng.
 */
export function computeNewGiftCount(info, tracker = streakTracker, now = Date.now()) {
  // Quà không combo: mỗi sự kiện là một lần tặng hoàn chỉnh
  if (info.giftType !== 1) return info.repeatCount;

  const key = `${info.userId || info.uniqueId}:${info.giftId}`;
  const prev = tracker.get(key);
  const prevCount = prev && now - prev.time < STREAK_TTL_MS ? prev.count : 0;

  // repeatCount nhỏ hơn lần trước => đây là combo MỚI (lỡ mất sự kiện kết thúc combo cũ)
  const added = info.repeatCount >= prevCount ? info.repeatCount - prevCount : info.repeatCount;

  if (info.repeatEnd) tracker.delete(key);
  else tracker.set(key, { count: info.repeatCount, time: now });

  return added;
}

// Dọn các combo "treo" quá lâu (ví dụ bị lỡ sự kiện kết thúc)
setInterval(() => {
  const now = Date.now();
  for (const [key, value] of streakTracker) {
    if (now - value.time > STREAK_TTL_MS) streakTracker.delete(key);
  }
}, STREAK_TTL_MS).unref();

let giftSeq = 0;
const nextGiftId = () => `${Date.now().toString(36)}-${(++giftSeq).toString(36)}`;

/**
 * HÀM XỬ LÝ SỰ KIỆN QUÀ TỪ TIKTOK
 * 1) Trích xuất uniqueId, giftName, repeatCount...
 * 2) Tính số quà mới (chống đếm trùng combo)
 * 3) Phát sự kiện "tiktok-gift" xuống TẤT CẢ trình duyệt đang mở game
 */
function onTikTokGift(data) {
  const info = extractGiftInfo(data);
  const addedCount = computeNewGiftCount(info);
  if (addedCount <= 0) return; // sự kiện "kết thúc combo" đã được xử lý trước đó => bỏ qua

  const payload = {
    id: nextGiftId(),
    ...info,
    addedCount,          // <- game dùng số này để kích hoạt hiệu ứng
    simulated: false,
    timestamp: Date.now(),
  };

  console.log(
    `[Gift] ${info.uniqueId} tặng "${info.giftName}" (id ${info.giftId}) +${addedCount}` +
      ` [combo ${info.repeatCount}${info.repeatEnd ? ', kết thúc' : ''}]`
  );
  io.emit('tiktok-gift', payload);
}

/* ------------------------------------------------------------------ */
/* 5. KẾT NỐI / NGẮT KẾT NỐI TIKTOK LIVE                               */
/* ------------------------------------------------------------------ */

async function safeDisconnect(connection) {
  try {
    await connection.disconnect();
  } catch (err) {
    console.warn('[TikTok] Lỗi khi ngắt kết nối cũ:', err?.message || err);
  }
}

/** Gắn các listener cho một kết nối. `token` giúp bỏ qua sự kiện của kết nối đã bị thay thế. */
function registerTikTokEvents(connection, username, token, session) {
  const isCurrent = () => token === connectionToken;

  // ===== Sự kiện chính: người xem tặng quà =====
  connection.on(WebcastEvent.GIFT, (data) => {
    if (isCurrent()) onTikTokGift(data);
  });

  // Thả tim (like) – gửi kèm để bạn tuỳ biến thêm (mặc định game không dùng)
  connection.on(WebcastEvent.LIKE, (data) => {
    if (!isCurrent()) return;
    io.emit('tiktok-like', {
      uniqueId: data?.user?.uniqueId ?? data?.uniqueId ?? 'unknown',
      likeCount: Number(data?.likeCount) || 0,
      totalLikeCount: Number(data?.totalLikeCount) || 0,
    });
  });

  connection.on(WebcastEvent.STREAM_END, () => {
    if (!isCurrent()) return;
    manualDisconnect = true; // live đã tắt => không tự kết nối lại
    setStatus({ status: 'ended', message: `Phiên live của @${username} đã kết thúc.` });
  });

  connection.on(ControlEvent.DISCONNECTED, ({ code, reason } = {}) => {
    if (!isCurrent()) return;
    tiktokConnection = null;
    if (!session.established || manualDisconnect || liveState.status === 'ended') return;
    console.warn(`[TikTok] Mất kết nối (code ${code ?? '?'}) ${reason ?? ''}`);
    scheduleReconnect(username);
  });

  // LUÔN phải có listener 'error' – nếu không, Node.js sẽ crash khi thư viện phát lỗi
  connection.on(ControlEvent.ERROR, ({ info, exception } = {}) => {
    console.error('[TikTok] Lỗi:', info ?? '', exception?.message ?? exception ?? '');
  });
}

function scheduleReconnect(username) {
  if (reconnectAttempts >= RECONNECT_MAX_ATTEMPTS) {
    setStatus({
      status: 'error',
      message: `Mất kết nối @${username}, đã thử lại ${RECONNECT_MAX_ATTEMPTS} lần không thành công.`,
    });
    return;
  }
  reconnectAttempts += 1;
  const delay = RECONNECT_DELAY_MS * reconnectAttempts;
  const tokenAtSchedule = connectionToken;

  setStatus({
    status: 'reconnecting',
    message: `Mất kết nối – thử lại lần ${reconnectAttempts}/${RECONNECT_MAX_ATTEMPTS} sau ${delay / 1000}s...`,
  });

  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(() => {
    if (tokenAtSchedule !== connectionToken || manualDisconnect) return;
    connectToTikTok(username, { isReconnect: true }).catch(() => {
      if (!manualDisconnect) scheduleReconnect(username);
    });
  }, delay);
}

/**
 * Kết nối tới phòng live của `rawUsername`. Kết nối cũ (nếu có) sẽ bị đóng.
 * Trả về state của thư viện (có roomId) hoặc null nếu bị một yêu cầu mới hơn thay thế.
 */
async function connectToTikTok(rawUsername, { isReconnect = false } = {}) {
  const username = normalizeUsername(rawUsername);
  if (!USERNAME_PATTERN.test(username)) {
    throw new Error(`Username "${rawUsername}" không hợp lệ.`);
  }

  // Lấy token mới NGAY LẬP TỨC (đồng bộ) để xử lý trường hợp bấm "Kết nối" liên tục
  const token = ++connectionToken;
  manualDisconnect = false;
  if (!isReconnect) reconnectAttempts = 0;
  clearTimeout(reconnectTimer);

  const old = tiktokConnection;
  tiktokConnection = null;
  if (old) await safeDisconnect(old);
  if (token !== connectionToken) return null; // đã có yêu cầu kết nối khác mới hơn

  const options = { enableExtendedGiftInfo: true }; // lấy thêm tên/giá quà
  if (EULER_API_KEY) options.signApiKey = EULER_API_KEY;

  const connection = new TikTokLiveConnection(username, options);
  const session = { established: false };
  tiktokConnection = connection;
  streakTracker.clear();
  registerTikTokEvents(connection, username, token, session);

  setStatus({ status: 'connecting', username, roomId: null, message: `Đang kết nối tới @${username}...` });

  try {
    const state = await connection.connect();
    if (token !== connectionToken) {
      await safeDisconnect(connection); // bị thay thế trong lúc đang kết nối
      return null;
    }
    session.established = true;
    reconnectAttempts = 0;
    setStatus({
      status: 'connected',
      roomId: state?.roomId ?? null,
      message: `Đã kết nối phòng live @${username} (roomId ${state?.roomId ?? '?'}).`,
    });
    return state;
  } catch (err) {
    if (token === connectionToken) {
      tiktokConnection = null;
      setStatus({ status: 'error', message: describeError(err) });
    }
    throw err;
  }
}

async function disconnectTikTok() {
  manualDisconnect = true;
  connectionToken += 1;
  clearTimeout(reconnectTimer);
  const old = tiktokConnection;
  tiktokConnection = null;
  if (old) await safeDisconnect(old);
  setStatus({ status: 'idle', roomId: null, message: 'Đã ngắt kết nối.' });
}

/* ------------------------------------------------------------------ */
/* 6. SOCKET.IO – GIAO TIẾP VỚI TRÌNH DUYỆT                            */
/* ------------------------------------------------------------------ */
const clampInt = (value, min, max) => Math.min(max, Math.max(min, Math.round(Number(value) || min)));

io.on('connection', (socket) => {
  console.log(`[Socket] Trình duyệt ${socket.id} đã kết nối`);

  // Gửi ngay trạng thái hiện tại cho trình duyệt mới mở
  socket.emit('tiktok-status', { ...liveState });
  socket.emit('server-config', { allowSimulate: ALLOW_SIMULATE });

  // Trình duyệt yêu cầu kết nối tới phòng live
  socket.on('connect-tiktok', async (username, ack) => {
    const reply = typeof ack === 'function' ? ack : () => {};
    try {
      const state = await connectToTikTok(username);
      if (state === null) reply({ ok: false, message: 'Yêu cầu đã bị thay thế bởi yêu cầu kết nối mới hơn.' });
      else reply({ ok: true, roomId: state?.roomId ?? null });
    } catch (err) {
      reply({ ok: false, message: describeError(err) });
    }
  });

  socket.on('disconnect-tiktok', async (ack) => {
    await disconnectTikTok();
    if (typeof ack === 'function') ack({ ok: true });
  });

  // Giả lập quà để test khi chưa live (đi qua đúng đường ống thật => mọi tab đều nhận)
  socket.on('simulate-gift', (raw = {}) => {
    if (!ALLOW_SIMULATE) return;
    const count = clampInt(raw.repeatCount, 1, 999);
    const payload = {
      id: nextGiftId(),
      uniqueId: String(raw.uniqueId || 'tester').slice(0, 40),
      nickname: '',
      userId: '',
      giftId: Number(raw.giftId) || 0,
      giftName: String(raw.giftName || 'Rose').slice(0, 60),
      giftType: 0,
      diamondCount: 1,
      repeatCount: count,
      repeatEnd: true,
      addedCount: count,
      simulated: true,
      timestamp: Date.now(),
    };
    console.log(`[Gift][GIẢ LẬP] ${payload.uniqueId} tặng "${payload.giftName}" x${count}`);
    io.emit('tiktok-gift', payload);
  });

  socket.on('disconnect', () => console.log(`[Socket] Trình duyệt ${socket.id} đã thoát`));
});

/* ------------------------------------------------------------------ */
/* 7. KHỞI ĐỘNG SERVER                                                 */
/* ------------------------------------------------------------------ */
httpServer.listen(PORT, () => {
  console.log(`🦖 T-Rex Live đang chạy tại: http://localhost:${PORT}`);
  console.log(`   Chế độ OBS (chỉ hiện game):  http://localhost:${PORT}/?overlay=1`);
  if (TIKTOK_USERNAME) {
    connectToTikTok(TIKTOK_USERNAME).catch((err) =>
      console.error('[TikTok] Không tự kết nối được:', describeError(err))
    );
  }
});

process.on('unhandledRejection', (reason) => console.error('[Server] Promise lỗi chưa xử lý:', reason));

async function shutdown() {
  console.log('\nĐang tắt server...');
  manualDisconnect = true;
  if (tiktokConnection) await safeDisconnect(tiktokConnection);
  io.close();
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
