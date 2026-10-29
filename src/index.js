/**
 * Minimal partner lab: Power Hub bridge WebSocket start/stop.
 *
 *   EV_HUB_BRIDGE_URL = wss://…/ocpp/{provider}/bridge   (no chargePointId)
 *   POST /start  { chargePointId, connectorId?, idTag? }
 *   POST /stop   { chargePointId, transactionId, connectorId? }
 *   GET  /health
 *   WS   /ws     start | stop | list  (list returns remembered transactionIds)
 */

const http = require("http");
const path = require("path");
const fs = require("fs");
const { randomUUID } = require("crypto");
const { WebSocket, WebSocketServer } = require("ws");

loadEnv(path.join(__dirname, "..", ".env"));

const PORT = Number(process.env.PORT || "3037");
const BRIDGE_BASE = normalizeBridgeBase(`${process.env.EV_HUB_BRIDGE_URL || ""}`.trim());
const PLATFORM_KEY = `${process.env.EV_HUB_PLATFORM_KEY || ""}`.trim();
const ID_TAG = `${process.env.EV_HUB_ID_TAG || "OUTSIDE"}`.trim();

if (!BRIDGE_BASE) {
  console.error("EV_HUB_BRIDGE_URL is required (e.g. wss://host/ocpp/{provider}/bridge)");
  process.exit(1);
}

/** chargePointId → open Power Hub bridge socket */
const bridges = new Map();
/** chargePointId → connecting promise */
const connecting = new Map();
/** request id → waiter */
const pending = new Map();
/** chargePointId → { chargePointId, connectorId, transactionId, startedAt } */
const sessions = new Map();
/** local /ws clients */
const clients = new Set();

function loadEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  for (const line of fs.readFileSync(filePath, "utf8").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq <= 0) continue;
    const key = trimmed.slice(0, eq).trim();
    if (process.env[key] === undefined) {
      process.env[key] = trimmed.slice(eq + 1).trim();
    }
  }
}

/** Strip trailing /{chargePointId} if someone pasted a full connect URL. */
function normalizeBridgeBase(raw) {
  if (!raw) return "";
  try {
    const url = new URL(raw.replace(/\/+$/, ""));
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts.length >= 4 && parts[0] === "ocpp" && parts[2] === "bridge") {
      url.pathname = `/${parts[0]}/${parts[1]}/${parts[2]}`;
      url.search = "";
      return url.toString().replace(/\/+$/, "");
    }
    if (parts.length === 3 && parts[0] === "ocpp" && parts[2] === "bridge") {
      url.search = "";
      return url.toString().replace(/\/+$/, "");
    }
  } catch {
    /* keep raw */
  }
  return raw.replace(/\/+$/, "");
}

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function send(ws, frame) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(typeof frame === "string" ? frame : JSON.stringify(frame));
  }
}

function broadcast(frame) {
  const text = typeof frame === "string" ? frame : JSON.stringify(frame);
  for (const client of clients) send(client, text);
}

function extractTransactionId(data) {
  if (!data || typeof data !== "object") return null;
  const candidates = [
    data.transactionId,
    data.data?.transactionId,
    data.data?.data?.transactionId,
  ];
  for (const value of candidates) {
    const n = Number(value);
    if (Number.isInteger(n) && n > 0) return n;
  }
  return null;
}

function rememberSession(chargePointId, connectorId, transactionId) {
  const cpid = `${chargePointId || ""}`.trim();
  const tx = Number(transactionId);
  if (!cpid || !Number.isInteger(tx) || tx <= 0) return null;
  const row = {
    chargePointId: cpid,
    connectorId: Number(connectorId || 1),
    transactionId: tx,
    startedAt: new Date().toISOString(),
  };
  sessions.set(cpid, row);
  broadcast({ ok: true, event: "session.started", data: row });
  return row;
}

function forgetSession(chargePointId) {
  const cpid = `${chargePointId || ""}`.trim();
  const prev = sessions.get(cpid);
  if (!prev) return;
  sessions.delete(cpid);
  broadcast({ ok: true, event: "session.stopped", data: prev });
}

function listSessions() {
  return [...sessions.values()];
}

function connectUrl(chargePointId) {
  const url = new URL(
    `${BRIDGE_BASE}/${encodeURIComponent(chargePointId)}`.replace(
      /([^:]\/)\/+/g,
      "$1",
    ),
  );
  if (PLATFORM_KEY) url.searchParams.set("apiKey", PLATFORM_KEY);
  return url.toString();
}

function ensureBridge(chargePointId) {
  const cpid = `${chargePointId || ""}`.trim();
  if (!cpid) return Promise.reject(new Error("chargePointId required"));

  const open = bridges.get(cpid);
  if (open?.readyState === WebSocket.OPEN) return Promise.resolve(open);
  if (connecting.has(cpid)) return connecting.get(cpid);

  const promise = new Promise((resolve, reject) => {
    const url = connectUrl(cpid);
    const headers = PLATFORM_KEY ? { "X-Platform-Key": PLATFORM_KEY } : {};
    const ws = new WebSocket(url, { headers });
    let pingTimer = null;

    ws.on("open", () => {
      bridges.set(cpid, ws);
      connecting.delete(cpid);
      log(`[${cpid}] bridge open`, url);
      pingTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return;
        try {
          ws.ping();
          ws.send(JSON.stringify({ id: `ping-${Date.now()}`, action: "ping" }));
        } catch {
          /* ignore */
        }
      }, 25_000);
      resolve(ws);
    });

    ws.on("message", (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }
      if (msg?.event) {
        log(`[${cpid}] ←`, msg.event, JSON.stringify(msg.data || {}));
        const tx = extractTransactionId(msg.data);
        if (tx) {
          rememberSession(
            msg.data?.chargePointId || cpid,
            msg.data?.connectorId || 1,
            tx,
          );
        }
        broadcast({ ok: true, event: msg.event, data: { chargePointId: cpid, ...(msg.data || {}) } });
        return;
      }
      const wait = pending.get(`${msg?.id || ""}`);
      if (!wait) return;
      pending.delete(`${msg.id}`);
      clearTimeout(wait.timer);
      wait.resolve(msg);
    });

    ws.on("close", (code, reason) => {
      if (pingTimer) clearInterval(pingTimer);
      if (bridges.get(cpid) === ws) bridges.delete(cpid);
      connecting.delete(cpid);
      log(`[${cpid}] bridge close`, code, reason?.toString() || "");
      for (const [id, wait] of pending) {
        if (wait.chargePointId !== cpid) continue;
        clearTimeout(wait.timer);
        wait.resolve({ ok: false, error: "bridge closed" });
        pending.delete(id);
      }
    });

    ws.on("error", (err) => {
      if (bridges.get(cpid) !== ws && connecting.has(cpid)) {
        connecting.delete(cpid);
        reject(err);
      } else {
        log(`[${cpid}] bridge error`, err.message);
      }
    });
  });

  connecting.set(cpid, promise);
  return promise;
}

function command(action, fields = {}) {
  const chargePointId = `${fields.chargePointId || ""}`.trim();
  if (!chargePointId) {
    return Promise.resolve({
      ok: false,
      status: 400,
      error: "chargePointId required",
    });
  }

  return ensureBridge(chargePointId).then(
    (ws) =>
      new Promise((resolve) => {
        const id = randomUUID();
        const timer = setTimeout(() => {
          pending.delete(id);
          resolve({ ok: false, status: 503, error: `${action} timed out` });
        }, 15_000);
        pending.set(id, {
          chargePointId,
          timer,
          resolve: (msg) => {
            if (!msg.ok) {
              resolve({
                ok: false,
                status: Number(msg.status) >= 400 ? Number(msg.status) : 400,
                error: msg.error || `${action} failed`,
                data: msg.data,
              });
              return;
            }
            resolve({ ok: true, status: 200, data: msg.data || {} });
          },
        });
        const frame = { ...fields, id, action, chargePointId };
        log(`[${chargePointId}] →`, action, JSON.stringify(frame));
        ws.send(JSON.stringify(frame));
      }),
  );
}

async function start(body = {}) {
  const chargePointId = `${body.chargePointId || ""}`.trim();
  if (!chargePointId) {
    return { status: 400, body: { ok: false, error: "chargePointId required" } };
  }
  const connectorId = Number(body.connectorId || 1);
  const result = await command("remoteStart", {
    chargePointId,
    connectorId,
    idTag: `${body.idTag || ID_TAG}`.trim(),
  });
  if (!result.ok) {
    return {
      status: result.status || 400,
      body: { ok: false, error: result.error, data: result.data },
    };
  }
  const transactionId = extractTransactionId(result.data);
  log(
    `[${chargePointId}] remoteStart reply`,
    `transactionId=${transactionId ?? "(none)"}`,
    JSON.stringify(result.data || {}),
  );
  const session = rememberSession(chargePointId, connectorId, transactionId);
  return {
    status: 200,
    body: {
      ok: true,
      via: "bridge",
      action: "remoteStart",
      transactionId: session?.transactionId || null,
      data: result.data,
    },
  };
}

async function stop(body = {}) {
  const chargePointId = `${body.chargePointId || ""}`.trim();
  if (!chargePointId) {
    return { status: 400, body: { ok: false, error: "chargePointId required" } };
  }
  const remembered = sessions.get(chargePointId);
  const transactionId = Number(
    body.transactionId != null && `${body.transactionId}` !== ""
      ? body.transactionId
      : remembered?.transactionId,
  );
  if (!Number.isInteger(transactionId) || transactionId <= 0) {
    return { status: 400, body: { ok: false, error: "transactionId required" } };
  }
  const fields = {
    chargePointId,
    connectorId: Number(body.connectorId || remembered?.connectorId || 1),
    transactionId,
  };
  const result = await command("remoteStop", fields);
  if (result.ok) forgetSession(chargePointId);
  return {
    status: result.status || (result.ok ? 200 : 400),
    body: result.ok
      ? { ok: true, via: "bridge", action: "remoteStop", transactionId, data: result.data }
      : { ok: false, error: result.error, data: result.data },
  };
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

const server = http.createServer(async (req, res) => {
  const pathname = new URL(req.url || "/", "http://localhost").pathname;

  try {
    if (req.method === "GET" && (pathname === "/" || pathname === "/health")) {
      const open = [...bridges.entries()]
        .filter(([, ws]) => ws.readyState === WebSocket.OPEN)
        .map(([id]) => id);
      sendJson(res, 200, {
        ok: true,
        bridgeBase: BRIDGE_BASE,
        open,
        sessions: listSessions(),
      });
      return;
    }

    if (req.method === "GET" && pathname === "/sessions") {
      sendJson(res, 200, { ok: true, sessions: listSessions() });
      return;
    }

    if (
      req.method === "POST" &&
      (pathname === "/start" ||
        pathname === "/api/proxy/start" ||
        pathname === "/api/bridge/start")
    ) {
      const result = await start(await readJson(req));
      sendJson(res, result.status, result.body);
      return;
    }

    if (
      req.method === "POST" &&
      (pathname === "/stop" ||
        pathname === "/api/proxy/stop" ||
        pathname === "/api/bridge/stop")
    ) {
      const result = await stop(await readJson(req));
      sendJson(res, result.status, result.body);
      return;
    }

    sendJson(res, 404, { ok: false, error: "not found" });
  } catch (err) {
    sendJson(res, 400, { ok: false, error: err.message || "bad request" });
  }
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const pathname = new URL(req.url || "/", "http://localhost").pathname;
  if (pathname !== "/ws") {
    socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    clients.add(ws);
    send(ws, {
      ok: true,
      event: "connected",
      data: { sessions: listSessions(), bridgeBase: BRIDGE_BASE },
    });

    ws.on("message", async (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        send(ws, { ok: false, error: "Invalid JSON" });
        return;
      }
      const action = `${msg.action || ""}`.trim();
      const id = msg.id;

      if (action === "list" || action === "sessions.list") {
        send(ws, { id, ok: true, action: "list", data: { sessions: listSessions() } });
        return;
      }
      if (action === "ping") {
        send(ws, { id, ok: true, action: "pong" });
        return;
      }
      if (action === "remoteStart" || action === "start") {
        const result = await start(msg);
        send(ws, {
          id,
          ok: result.body.ok,
          status: result.status,
          ...(result.body.error ? { error: result.body.error } : {}),
          data: result.body,
        });
        return;
      }
      if (action === "remoteStop" || action === "stop") {
        const result = await stop(msg);
        send(ws, {
          id,
          ok: result.body.ok,
          status: result.status,
          ...(result.body.error ? { error: result.body.error } : {}),
          data: result.body,
        });
        return;
      }
      send(ws, {
        id,
        ok: false,
        error: "action required: list | remoteStart | remoteStop | ping",
      });
    });

    ws.on("close", () => clients.delete(ws));
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`virtual-server start/stop via bridge`);
  console.log(`  GET  http://0.0.0.0:${PORT}/health`);
  console.log(`  GET  http://0.0.0.0:${PORT}/sessions`);
  console.log(`  POST http://0.0.0.0:${PORT}/start  { chargePointId, connectorId?, idTag? }`);
  console.log(`  POST http://0.0.0.0:${PORT}/stop   { chargePointId, transactionId, connectorId? }`);
  console.log(`  WS   ws://0.0.0.0:${PORT}/ws  (list | remoteStart | remoteStop)`);
  console.log(`  bridge base ${BRIDGE_BASE}/{chargePointId}`);
});
