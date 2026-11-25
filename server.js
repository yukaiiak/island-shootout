// 荒島槍戰 — 本機／區域網路對戰伺服器
// 靜態檔案 + 零依賴 WebSocket 中繼。伺服器只管房間與轉送，不跑遊戲模擬；
// 房主（房間裡第一個進來的人）負責 AI、縮圈、計分，其他人只送自己的狀態。
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const os = require("os");

const ROOT = __dirname;
const PORT = process.env.PORT ? Number(process.env.PORT) : 8765;
const MAX_PER_ROOM = 12;
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const TYPES = {".html":"text/html; charset=utf-8", ".js":"text/javascript", ".css":"text/css",
  ".json":"application/json", ".png":"image/png", ".jpg":"image/jpeg", ".svg":"image/svg+xml"};

/* ---------------- 靜態檔案 ---------------- */
const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split("?")[0]);
  if (p === "/") p = "/game.html";
  const file = path.join(ROOT, path.normalize(p).replace(/^(\.\.[/\\])+/, ""));
  if (!file.startsWith(ROOT)) { res.writeHead(403).end("forbidden"); return; }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404).end("not found"); return; }
    res.writeHead(200, {"Content-Type": TYPES[path.extname(file)] || "application/octet-stream"});
    res.end(data);
  });
});

/* ---------------- WebSocket（手寫，不裝套件） ---------------- */
function sendFrame(sock, str, opcode) {
  if (sock.destroyed) return;
  const body = Buffer.from(str === undefined ? "" : str, "utf8");
  const n = body.length;
  let head;
  if (n < 126) { head = Buffer.alloc(2); head[1] = n; }
  else if (n < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(n, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(n, 6); }
  head[0] = 0x80 | (opcode === undefined ? 1 : opcode);
  try { sock.write(Buffer.concat([head, body])); } catch (e) { /* 連線已斷 */ }
}
const sendJSON = (sock, obj) => sendFrame(sock, JSON.stringify(obj), 1);

// 從累積的 buffer 裡盡量取出完整訊框，回傳剩下的 buffer
function drain(client, onText) {
  let buf = client.buf;
  for (;;) {
    if (buf.length < 2) break;
    const b0 = buf[0], b1 = buf[1];
    const opcode = b0 & 0x0f, fin = (b0 & 0x80) !== 0, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (buf.length < 4) break; len = buf.readUInt16BE(2); off = 4; }
    else if (len === 127) {
      if (buf.length < 10) break;
      if (buf.readUInt32BE(2) !== 0) { client.sock.destroy(); return Buffer.alloc(0); }
      len = buf.readUInt32BE(6); off = 10;
    }
    if (len > 4 * 1024 * 1024) { client.sock.destroy(); return Buffer.alloc(0); }
    const need = off + (masked ? 4 : 0) + len;
    if (buf.length < need) break;
    let payload = buf.slice(off + (masked ? 4 : 0), need);
    if (masked) {
      const mask = buf.slice(off, off + 4);
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    }
    buf = buf.slice(need);
    if (opcode === 8) { sendFrame(client.sock, "", 8); client.sock.end(); return Buffer.alloc(0); }
    else if (opcode === 9) sendFrame(client.sock, payload.toString("utf8"), 10);
    else if (opcode === 10) { /* pong */ }
    else if (opcode === 1 || opcode === 0) {
      client.frag = opcode === 0 ? Buffer.concat([client.frag || Buffer.alloc(0), payload]) : payload;
      if (fin) { const txt = (client.frag || payload).toString("utf8"); client.frag = null; onText(txt); }
    }
  }
  return buf;
}

/* ---------------- 房間 ---------------- */
const rooms = new Map();            // code -> {code, hostId, clients:Map(id -> client)}
let nextId = 1;
const CODE_CHARS = "ACDEFGHJKLMNPQRSTUVWXYZ2345679";   // 去掉容易看錯的 0O1I8B
function newCode() {
  for (let tries = 0; tries < 200; tries++) {
    let c = "";
    for (let i = 0; i < 4; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  return "R" + (nextId++);
}
const peerList = room => [...room.clients.values()].map(c => ({ id: c.id, name: c.name }));
function broadcast(room, obj, exceptId) {
  for (const c of room.clients.values()) if (c.id !== exceptId) sendJSON(c.sock, obj);
}
function leaveRoom(client) {
  const room = client.room;
  if (!room) return;
  client.room = null;
  room.clients.delete(client.id);
  if (!room.clients.size) { rooms.delete(room.code); log(`房間 ${room.code} 關閉`); return; }
  broadcast(room, { t: "bye", id: client.id });
  if (room.hostId === client.id) {                       // 房主跑了：交棒給最早進來的人
    room.hostId = [...room.clients.keys()].sort((a, b) => a - b)[0];
    broadcast(room, { t: "host", id: room.hostId });
    log(`房間 ${room.code} 換房主 -> #${room.hostId}`);
  }
  log(`#${client.id} 離開 ${room.code}（剩 ${room.clients.size} 人）`);
}

const log = m => console.log(`[${new Date().toLocaleTimeString()}] ${m}`);

server.on("upgrade", (req, sock) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) { sock.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
  sock.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
             "Sec-WebSocket-Accept: " + accept + "\r\n\r\n");
  sock.setNoDelay(true);

  const client = { id: nextId++, sock, name: "玩家", room: null, buf: Buffer.alloc(0), frag: null };

  function handle(txt) {
    let m; try { m = JSON.parse(txt); } catch (e) { return; }
    if (!m || typeof m.t !== "string") return;
    if (m.t === "create" || m.t === "join") {
      if (client.room) leaveRoom(client);
      client.name = String(m.name || "玩家").slice(0, 12);
      let room;
      if (m.t === "create") {
        const code = newCode();
        room = { code, hostId: client.id, clients: new Map() };
        rooms.set(code, room);
        log(`#${client.id} ${client.name} 開了房間 ${code}`);
      } else {
        const code = String(m.code || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6);
        room = rooms.get(code);
        if (!room) { sendJSON(sock, { t: "err", msg: "找不到房間 " + code }); return; }
        if (room.clients.size >= MAX_PER_ROOM) { sendJSON(sock, { t: "err", msg: "房間人滿了" }); return; }
        log(`#${client.id} ${client.name} 加入 ${code}`);
      }
      const peers = peerList(room);
      room.clients.set(client.id, client);
      client.room = room;
      sendJSON(sock, { t: "hello", id: client.id, code: room.code, host: room.hostId === client.id, peers });
      broadcast(room, { t: "peer", id: client.id, name: client.name }, client.id);
      return;
    }
    if (m.t === "leave") { leaveRoom(client); return; }
    const room = client.room;
    if (!room) return;
    m.from = client.id;                                  // 來源由伺服器蓋章，客戶端不能偽造
    if (m.to) { const d = room.clients.get(m.to); if (d) sendJSON(d.sock, m); }
    else broadcast(room, m, client.id);
  }

  sock.on("data", d => {
    client.buf = Buffer.concat([client.buf, d]);
    client.buf = drain(client, handle);
  });
  const gone = () => { leaveRoom(client); };
  sock.on("close", gone);
  sock.on("end", gone);
  sock.on("error", gone);
});

// 幫使用者找出區域網路 IP，朋友要連的就是這個
function lanIPs() {
  const out = [];
  const ifs = os.networkInterfaces();
  for (const name of Object.keys(ifs)) for (const i of ifs[name] || [])
    if (i.family === "IPv4" && !i.internal) out.push(i.address);
  return out;
}
server.listen(PORT, () => {
  console.log("荒島槍戰伺服器已啟動");
  console.log("  本機：      http://localhost:" + PORT);
  for (const ip of lanIPs()) console.log("  同網路的人：http://" + ip + ":" + PORT);
  console.log("房主按「建立房間」拿到 4 碼代碼，其他人輸入代碼即可加入。");
});
