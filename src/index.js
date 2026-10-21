/**
 * Virtual partner CSMS / platform lab.
 *
 * Shared-master (recommended): chargers dial Power Hub /ocpp only.
 * This host uses EV_HUB_PROXY_URL + /bridge for control/events — never receives master OCPP URL.
 *
 * Optional local OCPP: /ocpp/{chargePointId} for chargers that belong only to this lab.
 *
 * HTTP:
 *   POST /api/proxy/start|stop → Power Hub /ocpp/proxy when EV_HUB_PROXY_URL is set (else bridge WS)
 *   GET  /api/proxy/events
 *   GET  /health
 */

const http = require("http");
const path = require("path");
const fs = require("fs");
const { randomUUID, timingSafeEqual } = require("crypto");
const { WebSocket, WebSocketServer } = require("ws");

loadEnv(path.join(__dirname, "..", ".env"));

const PORT = Number(process.env.PORT || "3037");
const WS_PATH = normalizeBasePath(process.env.WS_PATH || "/ocpp");
const BRIDGE_PATH = normalizeBasePath(process.env.BRIDGE_PATH || "/bridge");
const HEARTBEAT_INTERVAL = Number(process.env.HEARTBEAT_INTERVAL || "60") || 60;
const PUBLIC_URL = (
  process.env.PUBLIC_URL || "https://api-dev.pointhub.io"
).replace(/\/+$/, "");
const EV_HUB_WS_BASE = (process.env.EV_HUB_WS_BASE || "")
  .trim()
  .replace(/\/+$/, "");
const EV_HUB_BRIDGE_URL = (process.env.EV_HUB_BRIDGE_URL || "")
  .trim()
  .replace(/\/+$/, "");
/** Shared-master HTTP control plane (never share master OCPP URL). */
const EV_HUB_PROXY_URL = (process.env.EV_HUB_PROXY_URL || "")
  .trim()
  .replace(/\/+$/, "");
const EV_HUB_PROVIDER = (process.env.EV_HUB_PROVIDER || "virtual-server").trim();
const EV_HUB_PLATFORM_KEY = (process.env.EV_HUB_PLATFORM_KEY || "").trim();
const SIMULATOR_MOCK_PASSWORD = (
  process.env.SIMULATOR_MOCK_PASSWORD || "simulator-mock-1"
).trim();
const PRICE_PER_KWH = Number(process.env.PRICE_PER_KWH || "500");
/** Live charger sockets @type {Map<string, any>} */
const chargers = new Map();

/** ev-hub centralizer sockets on /bridge */
const bridgeClients = new Set();

/** chargePointId → bridge sockets that sent start for that charger */
const meterSubscribers = new Map();

/** chargePointId → meter / stop history (GET /api/proxy/events) */
const lastMeterEvents = new Map();
/** chargePointId → last start price (local mock or ev-hub charger price) */
const lastStartPrices = new Map();
const METER_EVENTS_PATH = "/api/proxy/events";
const MAX_METER_EVENTS = 200;

/**
 * @type {Map<string, { targetWsBase: string, authPassword: string, provider: string, source?: string, updatedAt: Date }>}
 */
const routes = new Map();

let nextTransactionId = 1;

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

function normalizeBasePath(value) {
  const withSlash = value.startsWith("/") ? value : `/${value}`;
  return withSlash.replace(/\/+$/, "") || "/ocpp";
}

function log(chargePointId, direction, payload) {
  const body = typeof payload === "string" ? payload : JSON.stringify(payload);
  console.log(
    `${new Date().toISOString()} [${chargePointId}] ${direction} ${body}`,
  );
}

function extractChargePointId(rawUrl) {
  const pathname = (rawUrl || "/").split("?")[0];
  if (!pathname.startsWith(`${WS_PATH}/`)) return null;
  const id = pathname
    .slice(WS_PATH.length + 1)
    .split("/")
    .filter(Boolean)[0];
  if (!id) return null;
  try {
    return decodeURIComponent(id);
  } catch {
    return id;
  }
}

function readPlatformAuthPassword(rawUrl) {
  try {
    const parsed = new URL(rawUrl || "/", "http://localhost");
    for (const key of ["platformAuthPassword", "authPassword", "password"]) {
      const value = `${parsed.searchParams.get(key) || ""}`.trim();
      if (value) return value;
    }
  } catch {
    /* ignore malformed handshake URL */
  }
  return "";
}

function passwordsMatch(left, right) {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  if (a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

function chargerPasswordError(platformAuthPassword) {
  const provided = `${platformAuthPassword || ""}`.trim();
  if (!provided) return null;
  if (passwordsMatch(provided, SIMULATOR_MOCK_PASSWORD)) return null;
  return {
    status: 401,
    body: { ok: false, error: "Invalid platformAuthPassword" },
  };
}

function evHubBridgeUrl() {
  const explicit = `${EV_HUB_BRIDGE_URL || ""}`.trim().replace(/\/+$/, "");
  if (explicit) return explicit.endsWith("/bridge") ? explicit : `${explicit}/bridge`;
  const fromOcpp = `${EV_HUB_WS_BASE || ""}`
    .trim()
    .replace(/\/+$/, "")
    .replace(/\/ocpp$/i, "");
  if (/^wss?:\/\//i.test(fromOcpp)) return `${fromOcpp}/bridge`;
  throw new Error("EV_HUB_BRIDGE_URL is not configured");
}

function evHubBridgeConnectUrl() {
  // Bridge is URL-only — do not append ?provider=
  const base = evHubBridgeUrl();
  if (!EV_HUB_PLATFORM_KEY) return base;
  const url = new URL(base);
  url.searchParams.set("apiKey", EV_HUB_PLATFORM_KEY);
  return url.toString();
}

let evHubSocket = null;
let evHubConnecting = null;
let evHubReconnectTimer = null;
const evHubPending = new Map();

function handleEvHubEvent(msg) {
  const event = `${msg.event || ""}`.trim();
  const data = msg.data && typeof msg.data === "object" ? msg.data : {};
  if (event === "platform.whoami" || event === "csms.chargers") {
    const chargers = Array.isArray(data.chargers) ? data.chargers : [];
    const summary = chargers
      .map((row) => `${row.chargePointId || "?"}:${row.available || row.online ? "up" : "down"}`)
      .join(", ");
    log(
      "centralizer",
      "·",
      `ev-hub ${event} requestedBy=${data.requestedBy || EV_HUB_PROVIDER || ""} [${summary || "none"}]`,
    );
    if (event === "csms.chargers") ingestCsmsChargers(data.chargers);
    return;
  }
  // Master OCPP events from Power Hub /bridge (status, boot, meters, …)
  ingestEvHubChargerEvent(event, data);
}

function failEvHubPending(error) {
  for (const [id, pending] of evHubPending) {
    clearTimeout(pending.timer);
    pending.reject(error);
    evHubPending.delete(id);
  }
}

function scheduleEvHubReconnect() {
  if (evHubReconnectTimer) return;
  evHubReconnectTimer = setTimeout(() => {
    evHubReconnectTimer = null;
    ensureEvHubSocket().catch((error) => {
      log("centralizer", "·", `ev-hub bridge reconnect failed ${error.message}`);
    });
  }, 3000);
}

function ensureEvHubSocket() {
  if (evHubSocket?.readyState === WebSocket.OPEN) return Promise.resolve(evHubSocket);
  if (evHubConnecting) return evHubConnecting;
  const url = evHubBridgeConnectUrl();
  const headers = {};
  if (EV_HUB_PLATFORM_KEY) headers["X-Platform-Key"] = EV_HUB_PLATFORM_KEY;
  evHubConnecting = new Promise((resolve, reject) => {
    const ws = new WebSocket(url, { headers });
    let pingTimer = null;
    const stopPing = () => {
      if (pingTimer) clearInterval(pingTimer);
      pingTimer = null;
    };
    ws.on("open", () => {
      evHubSocket = ws;
      evHubConnecting = null;
      log("centralizer", "·", `ev-hub bridge open ${evHubBridgeUrl()}`);
      // Keepalive so Cloudflare / proxies do not idle-drop the bridge (~100s).
      pingTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) {
          stopPing();
          return;
        }
        try {
          ws.ping();
        } catch {
          /* ignore */
        }
        try {
          ws.send(JSON.stringify({ id: `ping-${Date.now()}`, action: "ping" }));
        } catch {
          /* ignore */
        }
      }, 25_000);
      flushEvHubEvents();
      resolve(ws);
    });
    ws.on("message", (raw) => {
      let msg = null;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }
      if (msg?.event) {
        handleEvHubEvent(msg);
        return;
      }
      const pending = evHubPending.get(`${msg?.id || ""}`);
      if (pending) {
        evHubPending.delete(`${msg.id}`);
        clearTimeout(pending.timer);
        pending.resolve(msg);
        return;
      }
      if (msg?.action) {
        handleBridgeMessage(ws, raw, { fromEvHub: true }).catch((error) =>
          log("centralizer", "·", `ev-hub command ${error.message}`),
        );
      }
    });
    ws.on("close", () => {
      stopPing();
      if (evHubSocket === ws) evHubSocket = null;
      evHubConnecting = null;
      failEvHubPending(new Error("ev-hub bridge closed"));
      log("centralizer", "·", "ev-hub bridge closed");
      scheduleEvHubReconnect();
    });
    ws.on("error", (error) => {
      if (ws.readyState !== WebSocket.OPEN) reject(error);
    });
  });
  evHubConnecting.catch(() => {
    evHubConnecting = null;
  });
  return evHubConnecting;
}

const hubForwardInflight = new Set();

function commandEvHub(action, fields) {
  return ensureEvHubSocket()
    .then(
      (ws) =>
        new Promise((resolve) => {
          const id = randomUUID();
          const timer = setTimeout(() => {
            evHubPending.delete(id);
            resolve({
              status: 503,
              body: { ok: false, error: `ev-hub ${action} timed out` },
            });
          }, 15_000);
          evHubPending.set(id, {
            timer,
            resolve: (msg) => {
              if (!msg.ok) {
                const status = Number(msg.status);
                resolve({
                  status: status >= 400 && status < 600 ? status : 400,
                  body: { ok: false, error: msg.error || `${action} failed` },
                });
                return;
              }
              resolve({ status: 200, body: { ok: true, data: msg.data || {} } });
            },
            reject: (error) => {
              resolve({
                status: 503,
                body: { ok: false, error: error.message || `${action} failed` },
              });
            },
          });
          if (ws.readyState !== WebSocket.OPEN) {
            clearTimeout(timer);
            evHubPending.delete(id);
            resolve({ status: 503, body: { ok: false, error: "ev-hub bridge is not open" } });
            return;
          }
          log(
            "centralizer",
            "→",
            `request ${action} ${fields?.chargePointId || ""} on ${evHubBridgeUrl()}?token=(attached)`,
          );
          ws.send(JSON.stringify({ id, action, ...fields }));
        }),
    )
    .catch((error) => ({
      status: 503,
      body: { ok: false, error: error.message || "ev-hub bridge failed" },
    }));
}

function send(ws, frame) {
  if (ws.readyState === ws.OPEN) {
    ws.send(typeof frame === "string" ? frame : JSON.stringify(frame));
  }
}

/** Push platform JSON events onto the same /ocpp/{cpid} socket (Postman receive pane). */
function emitToChargerSocket(chargePointId, event, data = {}) {
  const charger = chargers.get(chargePointId);
  if (!charger?.ws || charger.ws.readyState !== WebSocket.OPEN) return false;
  send(charger.ws, {
    ok: true,
    event,
    data: { chargePointId, ...data },
  });
  return true;
}

function stopPlatformMeterMirror(chargePointId) {
  const charger = chargers.get(chargePointId);
  if (!charger?.meterMirrorTimer) return;
  clearInterval(charger.meterMirrorTimer);
  charger.meterMirrorTimer = null;
}

/** After StartTransaction: push charger.meterValues on /ocpp/{cpid} every 5s. */
function startPlatformMeterMirror(chargePointId, opts = {}) {
  const charger = chargers.get(chargePointId);
  if (!charger?.ws) return;
  stopPlatformMeterMirror(chargePointId);

  const connectorId = Number(opts.connectorId) || 1;
  const transactionId = Number(opts.transactionId) || 0;
  let energyWh = Number(opts.meterStart);
  if (!Number.isFinite(energyWh) || energyWh < 0) energyWh = 0;

  const tick = () => {
    if (!chargers.get(chargePointId)?.ws) {
      stopPlatformMeterMirror(chargePointId);
      return;
    }
    energyWh += 150;
    relayMeterValues(chargePointId, [
      2,
      "mirror",
      "MeterValues",
      {
        connectorId,
        transactionId: transactionId || undefined,
        meterValue: [
          {
            timestamp: new Date().toISOString(),
            sampledValue: [
              {
                value: String(energyWh),
                measurand: "Energy.Active.Import.Register",
                unit: "Wh",
              },
              {
                value: "7.2",
                measurand: "Power.Active.Import",
                unit: "kW",
              },
            ],
          },
        ],
      },
    ]);
  };

  tick();
  charger.meterMirrorTimer = setInterval(tick, 5_000);
}

const pendingEvHubEvents = [];

function sendToEvHub(event, data) {
  const frame = {
    ok: true,
    event,
    data: { ...data, channel: data?.channel || "partner" },
  };
  if (!evHubSocket || evHubSocket.readyState !== WebSocket.OPEN) {
    pendingEvHubEvents.push(frame);
    if (pendingEvHubEvents.length > 50) pendingEvHubEvents.shift();
    ensureEvHubSocket().catch((error) => {
      log("centralizer", "·", `ev-hub bridge connect failed ${error.message}`);
    });
    return false;
  }
  evHubSocket.send(JSON.stringify(frame));
  return true;
}

function announceLocalChargers() {
  for (const [chargePointId, charger] of chargers) {
    if (charger?.ws?.readyState !== WebSocket.OPEN) continue;
    sendToEvHub("charger.connected", {
      chargePointId,
      mode: "local",
      provider: "local",
    });
  }
}

function publicOcppBase() {
  return `${PUBLIC_URL.replace(/^http/i, "ws")}${WS_PATH}`;
}

function identifyWithEvHub() {
  sendToEvHub("connected", {
    httpBase: PUBLIC_URL,
    targetWsBase: publicOcppBase(),
    chargers: listChargers().map((c) => ({
      chargePointId: c.chargePointId,
      channel: chargerChannel(c.chargePointId),
    })),
  });
}

function flushEvHubEvents() {
  identifyWithEvHub();
  announceLocalChargers();
  while (pendingEvHubEvents.length && evHubSocket?.readyState === WebSocket.OPEN) {
    evHubSocket.send(JSON.stringify(pendingEvHubEvents.shift()));
  }
}

function callResult(ws, chargePointId, messageId, payload) {
  const frame = [3, messageId, payload];
  log(chargePointId, "→", frame);
  send(ws, frame);
}

function callError(ws, chargePointId, messageId, code, description) {
  const frame = [4, messageId, code, description, {}];
  log(chargePointId, "→", frame);
  send(ws, frame);
}

function sendOcppCall(chargePointId, action, payload) {
  const charger = chargers.get(chargePointId);
  if (!charger?.ws || charger.ws.readyState !== charger.ws.OPEN) {
    throw new Error(`Charger ${chargePointId} not connected on virtual-server`);
  }
  const messageId = randomUUID();
  const frame = [2, messageId, action, payload || {}];
  log(chargePointId, "→ ocpp", frame);
  send(charger.ws, frame);
  return { messageId, action };
}

function rememberCsmsCharger(chargePointId, extra = {}) {
  const id = `${chargePointId || extra.chargePointId || ""}`.trim();
  if (!id) return;
  const previous = routes.get(id);
  routes.set(id, {
    targetWsBase: extra.targetWsBase || previous?.targetWsBase || EV_HUB_WS_BASE,
    authPassword: extra.authPassword || previous?.authPassword || SIMULATOR_MOCK_PASSWORD,
    provider: extra.provider || previous?.provider || "ev-hub",
    source: extra.source === "charger-schema" ? "csms" : extra.source || previous?.source || "csms",
    name: extra.name ?? previous?.name,
    vendor: extra.vendor ?? previous?.vendor,
    chargerModel: extra.chargerModel ?? previous?.chargerModel,
    status: extra.status ?? previous?.status,
    connectors: extra.connectors ?? previous?.connectors,
    online: extra.online !== false,
    updatedAt: new Date(),
  });
  log("centralizer", "·", `saved ${id} on centralizer (${routes.get(id).provider})`);
}

function forgetCsmsCharger(chargePointId) {
  const id = `${chargePointId || ""}`.trim();
  const route = routes.get(id);
  if (!route || route.source !== "csms") return;
  routes.set(id, { ...route, online: false, updatedAt: new Date() });
  log("centralizer", "·", `marked ${id} offline on centralizer`);
}

function ingestCsmsChargers(list) {
  const chargersList = Array.isArray(list) ? list : [];
  for (const row of chargersList) {
    const id = typeof row === "string" ? row : row?.chargePointId;
    rememberCsmsCharger(id, row && typeof row === "object" ? row : {});
  }
}

function getChargerSnapshot(chargePointId, info) {
  const route = routes.get(chargePointId);
  const localOpen = Boolean(info?.ws && info.ws.readyState === info.ws.OPEN);
  const csms = !localOpen && (route?.source === "csms" || route?.provider === "ev-hub");
  const mode = localOpen ? "local" : route ? (csms ? "csms" : "routed") : "unknown";
  const providedFrom =
    mode === "local"
      ? "virtual-server-local"
      : mode === "csms"
        ? "ev-hub"
        : route
          ? `pending→${route.provider}`
          : "none";

  const meterValuesRoute = localOpen
    ? {
        receivesAt: info?.lastMeterValues?.destination || "virtual-server-local",
        listeners: info?.lastMeterValues?.listeners ?? 0,
        last: info?.lastMeterValues || null,
      }
    : {
        receivesAt: lastMeterEvent(chargePointId) ? "virtual-server" : "none",
        last: lastMeterEvent(chargePointId) || null,
      };

  return {
    chargePointId,
    connected: localOpen,
    mode,
    providedFrom,
    meterValues: meterValuesRoute,
    name: route?.name || info?.name || null,
    vendor: route?.vendor || null,
    chargerModel: route?.chargerModel || null,
    status: route?.status || null,
    source: route?.source === "csms" ? "charger-schema" : route?.source || null,
    route: route
      ? {
          targetWsBase: route.targetWsBase,
          provider: route.provider,
          updatedAt: route.updatedAt,
        }
      : null,
    connectedAt: info?.connectedAt || null,
    transactions: info
      ? [...info.transactions.entries()].map(([connectorId, tx]) => ({
          connectorId,
          transactionId: tx.transactionId,
          idTag: tx.idTag,
        }))
      : [],
    connectors: info?.connectorStatus
      ? [...info.connectorStatus.entries()].map(([connectorId, value]) => {
          const record =
            typeof value === "string" ? { status: value } : value || {};
          return {
            connectorId: Number(connectorId),
            status: record.status || "",
            errorCode: record.errorCode || null,
            info: record.info || null,
            timestamp: record.timestamp || null,
            vendorId: record.vendorId || null,
            vendorErrorCode: record.vendorErrorCode || null,
            updatedAt: record.updatedAt || null,
          };
        })
      : Array.isArray(route?.connectors)
        ? route.connectors
        : [],
    boot: info?.boot || null,
    paired: localOpen,
    statusUpdated: Boolean(
      info?.connectorStatus &&
      [...info.connectorStatus.values()].some((value) => statusText(value)),
    ),
  };
}

function listChargers() {
  const ids = new Set([...chargers.keys(), ...routes.keys()]);
  return [...ids].map((id) => getChargerSnapshot(id, chargers.get(id)));
}

function handleLocalCall(chargePointId, action, payload) {
  const charger = chargers.get(chargePointId);
  if (!charger) {
    if (action === "BootNotification") {
      return {
        status: "Accepted",
        currentTime: new Date().toISOString(),
        interval: HEARTBEAT_INTERVAL,
      };
    }
    if (action === "StartTransaction") {
      return { idTagInfo: { status: "Accepted" }, transactionId: 0 };
    }
    return {};
  }

  switch (action) {
    case "BootNotification":
      rememberBoot(chargePointId, payload);
      return {
        status: "Accepted",
        currentTime: new Date().toISOString(),
        interval: HEARTBEAT_INTERVAL,
      };
    case "Heartbeat":
      notifyChargerHeartbeat(chargePointId);
      return { currentTime: new Date().toISOString() };
    case "Authorize":
      return {
        idTagInfo: {
          status: "Accepted",
          expiryDate: new Date(
            Date.now() + 365 * 24 * 60 * 60 * 1000,
          ).toISOString(),
        },
      };
    case "StartTransaction": {
      const transactionId = nextTransactionId++;
      const connectorId = String(payload?.connectorId ?? "1");
      charger.transactions.set(connectorId, {
        transactionId,
        idTag: payload?.idTag || "",
        meterStart: Number(payload?.meterStart) || 0,
      });
      log(
        chargePointId,
        "·",
        `transaction ${transactionId} started connector=${connectorId}`,
      );
      const startData = {
        chargePointId,
        connectorId: Number(connectorId),
        idTag: payload?.idTag || "",
        meterStart: Number(payload?.meterStart) || 0,
        timestamp: payload?.timestamp || new Date().toISOString(),
        transactionId,
        ...startPriceFields(chargePointId),
      };
      sendToSubscribers(chargePointId, "charger.startTransaction", startData);
      sendToEvHub("charger.startTransaction", startData);
      emitToChargerSocket(chargePointId, "charger.startTransaction", startData);
      startPlatformMeterMirror(chargePointId, {
        connectorId: Number(connectorId),
        transactionId,
        meterStart: Number(payload?.meterStart) || 0,
      });
      return { idTagInfo: { status: "Accepted" }, transactionId };
    }
    case "StopTransaction": {
      const transactionId = Number(payload?.transactionId);
      for (const [connectorId, tx] of charger.transactions) {
        if (tx.transactionId === transactionId) {
          charger.transactions.delete(connectorId);
        }
      }
      log(chargePointId, "·", `transaction ${transactionId} stopped`);
      stopPlatformMeterMirror(chargePointId);
      const stopData = {
        chargePointId,
        transactionId,
        meterStop: Number(payload?.meterStop) || 0,
        timestamp: payload?.timestamp || new Date().toISOString(),
        reason: payload?.reason || "Local",
      };
      sendToSubscribers(chargePointId, "charger.stopTransaction", stopData);
      sendToEvHub("charger.stopTransaction", stopData);
      emitToChargerSocket(chargePointId, "charger.stopTransaction", stopData);
      if (charger.transactions.size === 0) clearMeterSubscribers(chargePointId);
      return { idTagInfo: { status: "Accepted" } };
    }
    case "MeterValues": {
      const connectorId = payload?.connectorId ?? null;
      const transactionId = payload?.transactionId ?? null;
      charger.lastMeterValues = {
        at: new Date().toISOString(),
        destination: "virtual-server-local",
        provider: "local",
        upstreamUrl: null,
        connectorId,
        transactionId,
        meterEntries: Array.isArray(payload?.meterValue)
          ? payload.meterValue.length
          : 0,
      };
      return {};
    }
    case "StatusNotification": {
      rememberStatus(chargePointId, payload);
      const status = `${payload?.status || ""}`.trim();
      if (status === "Available" && charger.transactions.size === 0) {
        clearMeterSubscribers(chargePointId);
      }
      return {};
    }
    case "DataTransfer":
      return { status: "Accepted" };
    default:
      return {};
  }
}

function adoptLocalCharger(ws, chargePointId, platformAuthPassword) {
  const previous = chargers.get(chargePointId);
  if (previous?.meterMirrorTimer) clearInterval(previous.meterMirrorTimer);
  chargers.set(chargePointId, {
    ws,
    transactions: new Map(),
    connectorStatus: new Map(),
    connectedAt: new Date(),
    mode: "local",
    platformAuthPassword,
    meterMirrorTimer: null,
  });
  log(chargePointId, "·", "simulator connected (LOCAL CSMS)");
  notifyChargerConnected(chargePointId, "local", "local");
  emitToChargerSocket(chargePointId, "connected", {
    message: "Connected to virtual-server OCPP",
    mode: "local",
  });
}

function handleLocalOcppMessage(ws, chargePointId, message) {
  log(chargePointId, "←", message);
  const type = Number(message[0]);
  const messageId = message[1];

  if (type === 2) {
    try {
      const payload = handleLocalCall(chargePointId, message[2], message[3] || {});
      if (message[2] === "MeterValues") relayMeterValues(chargePointId, message);
      callResult(ws, chargePointId, messageId, payload);
    } catch (error) {
      callError(ws, chargePointId, messageId, "InternalError", error.message);
    }
    return;
  }

  if (type === 3 || type === 4) return;

  callError(ws, chargePointId, messageId, "FormationViolation", "Unsupported message type");
}

async function handleChargerSocketCommand(ws, chargePointId, msg) {
  const action = `${msg.action || ""}`.trim();
  const id = msg.id;
  const reply = (result) =>
    send(ws, {
      id,
      ok: result.status === 200,
      status: result.status,
      ...(result.body?.error ? { error: result.body.error } : {}),
      data: result.body?.data || result.body,
    });

  if (action === "remoteStart" || action === "RemoteStartTransaction") {
    log(chargePointId, "·", "socket remoteStart");
    reply(
      await startCharger({
        chargePointId: `${msg.chargePointId || chargePointId}`.trim(),
        connectorId: msg.connectorId,
        idTag: msg.idTag,
        platformAuthPassword: msg.platformAuthPassword,
      }),
    );
    return;
  }
  if (action === "remoteStop" || action === "RemoteStopTransaction") {
    log(chargePointId, "·", "socket remoteStop");
    reply(
      await stopCharger({
        chargePointId: `${msg.chargePointId || chargePointId}`.trim(),
        connectorId: msg.connectorId,
        transactionId: msg.transactionId,
        platformAuthPassword: msg.platformAuthPassword,
      }),
    );
    return;
  }
  send(ws, { id, ok: false, error: action ? `Unknown action: ${action}` : "action required" });
}

function onChargerConnection(ws, chargePointId, platformAuthPassword = "") {
  adoptLocalCharger(ws, chargePointId, platformAuthPassword);

  ws.on("message", (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      log(chargePointId, "·", "ignored non-json");
      return;
    }

    if (!Array.isArray(message)) {
      handleChargerSocketCommand(ws, chargePointId, message).catch((error) =>
        send(ws, { ok: false, error: error.message || "Command failed" }),
      );
      return;
    }

    handleLocalOcppMessage(ws, chargePointId, message);
  });

  ws.on("close", (code, reason) => {
    const current = chargers.get(chargePointId);
    if (current?.ws === ws) {
      stopPlatformMeterMirror(chargePointId);
      chargers.delete(chargePointId);
      clearMeterSubscribers(chargePointId);
      notifyChargerDisconnected(chargePointId);
    }
    log(chargePointId, "·", `disconnected ${code} ${reason?.toString?.() || ""}`);
  });

  ws.on("error", (error) => {
    log(chargePointId, "·", `socket error ${error.message}`);
  });
}

const BUSY_CONNECTOR_STATUSES = new Set([
  "Charging",
  "SuspendedEV",
  "SuspendedEVSE",
  "Finishing",
  "Reserved",
  "Unavailable",
  "Faulted",
]);

function connectorReadyForRemoteStart(status) {
  const normalized = `${status || ""}`.trim().toUpperCase();
  if (!normalized) return false;
  const allowed = `${process.env.REMOTE_START_ALLOWED_CONNECTOR_STATUSES || "PREPARING"}`
    .split(",")
    .map((value) => value.trim().toUpperCase())
    .filter(Boolean);
  return allowed.includes(normalized);
}

function chargerAvailability(chargePointId, connectorId) {
  const charger = chargers.get(chargePointId);

  if (!charger?.ws || charger.ws.readyState !== charger.ws.OPEN) {
    return {
      available: false,
      status: "Disconnected",
      reason: "not connected",
    };
  }

  const activeTx = charger.transactions?.get(String(connectorId));

  if (activeTx) {
    return {
      available: false,
      status: "Charging",
      reason: "active transaction",
      transactionId: activeTx.transactionId,
    };
  }

  const reported =
    statusText(charger.connectorStatus?.get(String(connectorId))) ||
    statusText(charger.connectorStatus?.get("0"));

  if (reported && BUSY_CONNECTOR_STATUSES.has(reported)) {
    return {
      available: false,
      status: reported,
      reason: "connector status",
    };
  }

  return {
    available: true,
    status: reported || "Available",
    reason: "available",
  };
}

function rememberStartPrice(chargePointId, pricePerKwh, priceSource) {
  const id = `${chargePointId || ""}`.trim();
  const n = Number(pricePerKwh);
  if (!id || !Number.isFinite(n) || n < 0) return undefined;
  lastStartPrices.set(id, {
    pricePerKwh: n,
    priceUnit: "kWh",
    priceSource: priceSource || "virtual-server",
    receivedAt: new Date().toISOString(),
  });
  log("centralizer", "·", `pricePerKwh ${n} source=${priceSource || "virtual-server"} for ${id}`);
  return n;
}

function startPriceFields(chargePointId) {
  const row = lastStartPrices.get(`${chargePointId || ""}`.trim());
  if (!row) return {};
  return {
    pricePerKwh: row.pricePerKwh,
    priceUnit: row.priceUnit,
    priceSource: row.priceSource,
  };
}

function localMockPrice() {
  return Number.isFinite(PRICE_PER_KWH) && PRICE_PER_KWH >= 0 ? PRICE_PER_KWH : 500;
}

function priceFromEvHubReply(payload) {
  const data = payload?.data && typeof payload.data === "object" ? payload.data : {};
  const nested = data.data && typeof data.data === "object" ? data.data : {};
  return data.pricePerKwh ?? nested.pricePerKwh ?? payload?.pricePerKwh;
}

function evHubProxyHeaders() {
  const headers = { "Content-Type": "application/json" };
  if (EV_HUB_PROVIDER) headers["X-Platform-Provider"] = EV_HUB_PROVIDER;
  if (EV_HUB_PLATFORM_KEY) headers["X-Platform-Key"] = EV_HUB_PLATFORM_KEY;
  return headers;
}

async function httpJson(url, { method = "GET", headers = {}, body } = {}) {
  const payload = body === undefined ? undefined : JSON.stringify(body);
  const response = await fetch(url, {
    method,
    headers: { ...headers, ...(payload ? { "Content-Type": "application/json" } : {}) },
    body: payload,
  });
  const text = await response.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }
  return { status: response.status, data };
}

async function startViaEvHubProxy(chargePointId, connectorId, idTag) {
  if (!EV_HUB_PROXY_URL) {
    return {
      status: 503,
      body: { ok: false, error: "EV_HUB_PROXY_URL is not set" },
    };
  }
  try {
    const { status, data } = await httpJson(`${EV_HUB_PROXY_URL}/ocpp/proxy/start`, {
      method: "POST",
      headers: evHubProxyHeaders(),
      body: {
        chargePointId,
        connectorId,
        idTag,
        provider: EV_HUB_PROVIDER,
        ...(EV_HUB_PLATFORM_KEY ? { apiKey: EV_HUB_PLATFORM_KEY } : {}),
        forwardedFrom: "virtual-server",
      },
    });
    if (status >= 400) {
      return {
        status,
        body: {
          ok: false,
          error: data?.message || data?.error || `Power Hub proxy start HTTP ${status}`,
          data,
        },
      };
    }
    rememberStartPrice(chargePointId, priceFromEvHubReply({ data }), data?.priceSource || "ev-hub");
    return {
      status: 200,
      body: {
        ok: true,
        action: "RemoteStartTransaction",
        via: "ev-hub-proxy",
        data: {
          chargePointId,
          connectorId,
          idTag,
          providedFrom: "ev-hub",
          ...data,
          ...startPriceFields(chargePointId),
        },
      },
    };
  } catch (error) {
    return {
      status: 503,
      body: { ok: false, error: error.message || "Power Hub proxy start failed" },
    };
  }
}

async function startViaEvHub(chargePointId, connectorId, idTag, platformAuthPassword) {
  rememberCsmsCharger(chargePointId, { provider: "ev-hub", source: "csms" });
  if (EV_HUB_PROXY_URL) {
    return startViaEvHubProxy(chargePointId, connectorId, idTag);
  }
  const password = `${platformAuthPassword || ""}`.trim();
  const url = evHubBridgeUrl();
  try {
    const response = await commandEvHub("remoteStart", {
      chargePointId,
      connectorId,
      idTag,
      ...(password ? { platformAuthPassword: password } : {}),
      forwardedFrom: "virtual-server",
    });
    if (response.status !== 200) return response;
    const payload = response.body;
    const evHubData = payload?.data?.data || payload?.data || {};
    if (
      evHubData?.response?.status === "Rejected" ||
      evHubData?.status === "Rejected" ||
      evHubData?.success === false
    ) {
      return {
        status: 409,
        body: {
          ok: false,
          error: `Charger ${chargePointId} rejected RemoteStart on connector ${connectorId}`,
        },
      };
    }
    const replySource =
      payload?.data?.priceSource ||
      (payload?.data?.data && payload.data.data.priceSource) ||
      "ev-hub";
    rememberStartPrice(chargePointId, priceFromEvHubReply(payload), replySource);
    return {
      status: 200,
      body: {
        ok: true,
        action: "RemoteStartTransaction",
        via: "ev-hub-bridge",
        data: {
          chargePointId,
          connectorId,
          idTag,
          providedFrom: "ev-hub",
          ...evHubData,
          ...startPriceFields(chargePointId),
        },
      },
    };
  } catch (error) {
    return {
      status: 503,
      body: {
        ok: false,
        error: error.message || `ev-hub remoteStart on ${url} failed`,
      },
    };
  }
}

async function startCharger(body) {
  const chargePointId = `${body.chargePointId || ""}`.trim();
  const connectorId = Number(body.connectorId || 1);
  const idTag = `${body.idTag || "PARTNER"}`.trim();
  const platformAuthPassword =
    `${body.platformAuthPassword ?? body.authPassword ?? ""}`.trim();

  if (!chargePointId) {
    return {
      status: 400,
      body: { ok: false, error: "chargePointId required" },
    };
  }

  const fromEvHub = `${body.commandedBy || ""}`.trim() === "ev-hub";
  const local = chargers.get(chargePointId);
  const localOpen = Boolean(local?.ws && local.ws.readyState === local.ws.OPEN);
  const forwardKey = `start:${chargePointId}:${connectorId}`;

  if (!localOpen) {
    if (fromEvHub) {
      return {
        status: 404,
        body: {
          ok: false,
          error: `Charger ${chargePointId} is not connected on this server`,
        },
      };
    }
    return startViaEvHub(chargePointId, connectorId, idTag, platformAuthPassword);
  }

  // Local charger: ask ev-hub on the token socket. ev-hub calls back with
  // commandedBy=ev-hub, and that call takes the local OCPP path below.
  if (!fromEvHub && !hubForwardInflight.has(forwardKey)) {
    hubForwardInflight.add(forwardKey);
    try {
      const viaHub = await startViaEvHub(chargePointId, connectorId, idTag, platformAuthPassword);
      if (viaHub.status !== 404 && viaHub.status !== 503) return viaHub;
      log(
        "centralizer",
        "·",
        `ev-hub bridge remoteStart ${chargePointId} returned ${viaHub.status}; starting on the local socket`,
      );
    } finally {
      hubForwardInflight.delete(forwardKey);
    }
  }

  const passwordError = chargerPasswordError(platformAuthPassword);
  if (passwordError) return passwordError;

  const availability = chargerAvailability(chargePointId, connectorId);
  if (!availability.available) {
    return {
      status: 409,
      body: {
        ok: false,
        error: `Charger ${chargePointId} connector ${connectorId} is busy (${availability.status})`,
        data: { chargePointId, connectorId, ...availability },
      },
    };
  }
  if (!connectorReadyForRemoteStart(availability.status)) {
    return {
      status: 400,
      body: {
        ok: false,
        error: `Connector ${connectorId} on ${chargePointId} is not ready for remote start (status: ${availability.status || "unknown"}). Plug in the vehicle first.`,
        data: { chargePointId, connectorId, ...availability },
      },
    };
  }

  try {
    const sent = sendOcppCall(chargePointId, "RemoteStartTransaction", {
      connectorId,
      idTag,
    });
    subscribeEvHubListeners(chargePointId);
    rememberStartPrice(chargePointId, localMockPrice(), "virtual-server");
    return {
      status: 200,
      body: {
        ok: true,
        action: "RemoteStartTransaction",
        via: "centralizer",
        data: {
          chargePointId,
          connectorId,
          idTag,
          ocppMessageId: sent.messageId,
          status: "Accepted",
          providedFrom: getChargerSnapshot(
            chargePointId,
            chargers.get(chargePointId),
          ).providedFrom,
          ...startPriceFields(chargePointId),
        },
      },
    };
  } catch (error) {
    return { status: 503, body: { ok: false, error: error.message } };
  }
}

function resolveTransactionIdForConnector(chargePointId, connectorId) {
  const entry = chargers.get(chargePointId);
  if (!entry?.transactions) return null;
  const key = String(connectorId);
  const tx = entry.transactions.get(key);
  const tid = Number(tx?.transactionId);
  return Number.isInteger(tid) && tid > 0 ? tid : null;
}

function resolveStopTransactionId(chargePointId, connectorId, bodyTransactionId) {
  const fromBody = Number(bodyTransactionId);
  if (Number.isInteger(fromBody) && fromBody > 0) {
    return fromBody;
  }
  const fromConnector = resolveTransactionIdForConnector(chargePointId, connectorId);
  if (fromConnector) return fromConnector;
  const last = lastMeterEvent(chargePointId);
  const fromMeter = Number(last?.transactionId);
  if (Number.isInteger(fromMeter) && fromMeter > 0) {
    return fromMeter;
  }
  return null;
}

async function stopViaEvHubProxy(chargePointId, connectorId, transactionId) {
  if (!EV_HUB_PROXY_URL) {
    return {
      status: 503,
      body: { ok: false, error: "EV_HUB_PROXY_URL is not set" },
    };
  }
  if (!Number.isInteger(Number(transactionId)) || Number(transactionId) <= 0) {
    return {
      status: 400,
      body: { ok: false, error: "transactionId is required for Power Hub proxy stop" },
    };
  }
  try {
    const { status, data } = await httpJson(`${EV_HUB_PROXY_URL}/ocpp/proxy/stop`, {
      method: "POST",
      headers: evHubProxyHeaders(),
      body: {
        chargePointId,
        connectorId,
        transactionId: Number(transactionId),
        provider: EV_HUB_PROVIDER,
        ...(EV_HUB_PLATFORM_KEY ? { apiKey: EV_HUB_PLATFORM_KEY } : {}),
      },
    });
    if (status >= 400) {
      return {
        status,
        body: {
          ok: false,
          error: data?.message || data?.error || `Power Hub proxy stop HTTP ${status}`,
          data,
        },
      };
    }
    return {
      status: 200,
      body: {
        ok: true,
        action: "RemoteStopTransaction",
        via: "ev-hub-proxy",
        data: { chargePointId, connectorId, transactionId, ...data },
      },
    };
  } catch (error) {
    return { status: 503, body: { ok: false, error: error.message } };
  }
}

async function stopViaEvHub(
  chargePointId,
  connectorId,
  transactionId,
  platformAuthPassword,
) {
  if (EV_HUB_PROXY_URL) {
    return stopViaEvHubProxy(chargePointId, connectorId, transactionId);
  }
  try {
    const response = await commandEvHub("remoteStop", {
      chargePointId,
      connectorId,
      ...(transactionId ? { transactionId } : {}),
      ...(platformAuthPassword ? { platformAuthPassword } : {}),
      forwardedFrom: "virtual-server",
    });
    if (response.status !== 200) return response;
    const payload = response.body;
    return {
      status: 200,
      body: {
        ok: true,
        action: "RemoteStopTransaction",
        via: "ev-hub-bridge",
        data: {
          chargePointId,
          connectorId,
          transactionId,
          ...(payload?.data || payload || {}),
        },
      },
    };
  } catch (error) {
    return { status: 503, body: { ok: false, error: error.message } };
  }
}

async function stopCharger(body) {
  const chargePointId = `${body.chargePointId || ""}`.trim();
  const connectorId = Number(body.connectorId || 1);
  const platformAuthPassword =
    `${body.platformAuthPassword ?? body.authPassword ?? ""}`.trim();
  const transactionId = resolveStopTransactionId(
    chargePointId,
    connectorId,
    body.transactionId,
  );

  if (!chargePointId) {
    return {
      status: 400,
      body: { ok: false, error: "chargePointId required" },
    };
  }
  if (!Number.isInteger(connectorId) || connectorId < 1) {
    return {
      status: 400,
      body: { ok: false, error: "connectorId must be an integer >= 1" },
    };
  }

  const fromEvHub = `${body.commandedBy || ""}`.trim() === "ev-hub";
  const local = chargers.get(chargePointId);
  const localOpen = Boolean(local?.ws && local.ws.readyState === local.ws.OPEN);

  if (!localOpen) {
    if (fromEvHub) {
      return {
        status: 404,
        body: { ok: false, error: `Charger ${chargePointId} is not connected on this server` },
      };
    }
    return stopViaEvHub(chargePointId, connectorId, transactionId, platformAuthPassword);
  }
  const passwordError = chargerPasswordError(platformAuthPassword);
  if (passwordError) return passwordError;
  if (!transactionId) {
    const availability = chargerAvailability(chargePointId, connectorId);
    if (!availability.available && availability.reason !== "not connected") {
      return {
        status: 409,
        body: {
          ok: false,
          error: `Charger ${chargePointId} connector ${connectorId} is busy (${availability.status})`,
        },
      };
    }
    return {
      status: 409,
      body: {
        ok: false,
        error: `No active transaction on connector ${connectorId}. Wait for StartTransaction/MeterValues, then stop with connectorId.`,
        chargePointId,
        connectorId,
      },
    };
  }

  try {
    const sent = sendOcppCall(chargePointId, "RemoteStopTransaction", {
      transactionId,
    });
    return {
      status: 200,
      body: {
        ok: true,
        action: "RemoteStopTransaction",
        via: "centralizer",
        data: {
          chargePointId,
          connectorId,
          transactionId,
          ocppMessageId: sent.messageId,
          status: "Accepted",
        },
      },
    };
  } catch (error) {
    return { status: 503, body: { ok: false, error: error.message } };
  }
}

function sendJson(res, status, body) {
  const payload = JSON.stringify({ source: "virtual-server", ...body });
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
  });
  res.end(payload);
}

function sendResult(res, result) {
  sendJson(res, result.status, result.body);
}

function readRawBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 1_000_000) {
        reject(new Error("Payload too large"));
        req.destroy();
      }
    });
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

function readJson(req) {
  return readRawBody(req).then((data) => {
    if (!data) return {};
    try {
      return JSON.parse(data);
    } catch {
      throw new Error("Invalid JSON body");
    }
  });
}

async function handleHttp(req, res) {
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    });
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url || "/", "http://localhost");
  const pathname = parsedUrl.pathname;

  try {
    if (req.method === "GET" && (pathname === "/" || pathname === "/health")) {
      sendJson(res, 200, { ok: true, service: "virtual-server" });
      return;
    }

    if (
      req.method === "POST" &&
      (pathname === "/api/proxy/start" || pathname === "/ocpp/proxy/start")
    ) {
      const body = (await readJson(req)) || {};
      log(
        "centralizer",
        "·",
        `HTTP POST ${pathname} ${body.chargePointId || ""} commandedBy=${body.commandedBy || "none"}`,
      );
      sendResult(res, await startCharger(body));
      return;
    }

    if (
      req.method === "POST" &&
      (pathname === "/api/proxy/stop" || pathname === "/ocpp/proxy/stop")
    ) {
      const body = await readJson(req);
      sendResult(res, await stopCharger(body || {}));
      return;
    }

    if (req.method === "GET" && pathname === METER_EVENTS_PATH) {
      const chargePointId = `${parsedUrl.searchParams.get("chargePointId") || ""}`.trim();
      sendJson(res, 200, { ok: true, events: eventsForChargePoint(chargePointId) });
      return;
    }

    sendJson(res, 404, {
      ok: false,
      error: `Not found: ${req.method} ${pathname}`,
    });
  } catch (error) {
    sendJson(res, error.status || 500, { ok: false, error: error.message || "Internal error" });
  }
}

function chargerChannel(chargePointId) {
  const provider = routes.get(chargePointId)?.provider;
  return provider === "ev-hub" ? "ev-hub" : "partner";
}

function lastMeterEvent(chargePointId) {
  const history = lastMeterEvents.get(`${chargePointId || ""}`.trim());
  return Array.isArray(history) && history.length ? history[history.length - 1] : null;
}

function eventsForChargePoint(chargePointId) {
  const filter = `${chargePointId || ""}`.trim();
  if (filter) {
    const history = lastMeterEvents.get(filter);
    return history ? { [filter]: history } : {};
  }
  return Object.fromEntries(lastMeterEvents);
}

function summarizeMeterPayload(data) {
  const samples = [];
  const entries = Array.isArray(data?.meterValue)
    ? data.meterValue
    : Array.isArray(data?.payload?.meterValue)
      ? data.payload.meterValue
      : [];
  for (const entry of entries) {
    for (const sample of entry?.sampledValue || []) {
      samples.push(
        `${sample.measurand || "value"}=${sample.value}${sample.unit ? sample.unit : ""}`,
      );
    }
  }
  return samples.length ? samples.join(" ") : JSON.stringify(data);
}

// ---- bridge (WS) kept for ev-hub compatibility ----
function handleCsmsEvent(msg) {
  const event = `${msg.event || ""}`.trim();
  const data = msg.data && typeof msg.data === "object" ? msg.data : {};
  if (event === "csms.chargers") {
    ingestCsmsChargers(data.chargers);
    return;
  }
  ingestEvHubChargerEvent(event, data);
}

/**
 * Passive receive from Power Hub /bridge — no remoteStart required.
 * Only charger status / presence (not meters). Debounced; never echo back to ev-hub.
 */
function ingestEvHubChargerEvent(event, data) {
  const chargePointId = `${data?.chargePointId || ""}`.trim();
  if (!event || !chargePointId) return;

  if (event === "charger.connected") {
    if (!shouldSendBridge(`evhub-in:connected:${chargePointId}`)) return;
    rememberCsmsCharger(chargePointId, data);
    log(chargePointId, "·", `RECEIVED charger.connected`);
    broadcastBridge(
      "charger.connected",
      { ...data, chargePointId, channel: "ev-hub" },
      { localOnly: true },
    );
    return;
  }
  if (event === "charger.disconnected") {
    if (!shouldSendBridge(`evhub-in:disconnected:${chargePointId}`)) return;
    forgetCsmsCharger(chargePointId);
    log(chargePointId, "·", "RECEIVED charger.disconnected");
    broadcastBridge(
      "charger.disconnected",
      { chargePointId, channel: "ev-hub" },
      { localOnly: true },
    );
    return;
  }
  if (event === "charger.status") {
    const connectorId = data.connectorId ?? "";
    const status = data.status || "";
    if (!shouldSendBridge(`evhub-in:status:${chargePointId}:${connectorId}:${status}`)) {
      return;
    }
    log(
      chargePointId,
      "·",
      `RECEIVED charger.status connector=${connectorId} status=${status}`,
    );
    broadcastBridge(
      "charger.status",
      { ...data, chargePointId, channel: "ev-hub" },
      { localOnly: true },
    );
    return;
  }
  if (event === "charger.boot") {
    if (!shouldSendBridge(`evhub-in:boot:${chargePointId}`)) return;
    log(chargePointId, "·", `RECEIVED charger.boot`);
    broadcastBridge(
      "charger.boot",
      { ...data, chargePointId, channel: "ev-hub" },
      { localOnly: true },
    );
    return;
  }
  if (
    event === "charger.meterValues" ||
    event === "charger.startTransaction" ||
    event === "charger.stopTransaction"
  ) {
    ingestMeterEvent(event, data);
    broadcastBridge(event, { ...data, chargePointId, channel: "ev-hub" }, { localOnly: true });
  }
}

async function handleBridgeMessage(ws, raw, options = {}) {
  let msg;
  try {
    msg = JSON.parse(raw.toString());
  } catch {
    send(ws, { ok: false, error: "Invalid JSON" });
    return;
  }

  const id = msg.id;
  const action = `${msg.action || ""}`.trim();
  const fromEvHub = Boolean(options.fromEvHub) || `${msg.commandedBy || ""}`.trim() === "ev-hub";

  if (msg.event) {
    handleCsmsEvent(msg);
    return;
  }

  try {
    if (action === "list" || action === "chargers.list") {
      send(ws, { id, ok: true, data: { chargers: listChargers() } });
      return;
    }
    if (action === "registerChannel") {
      const channel = `${msg.channel || ""}`.trim();
      if (!channel) {
        send(ws, { id, ok: false, error: "channel required" });
        return;
      }
      ws.channelName = channel;
      log("bridge", "·", `channel registered ${channel}`);
      send(ws, { id, ok: true, data: { channel } });
      return;
    }
    if (action === "remoteStart" || action === "RemoteStartTransaction") {
      log(
        "centralizer",
        "·",
        `bridge remoteStart ${msg.chargePointId || ""} from ${fromEvHub ? "ev-hub" : "local"}`,
      );
      const result = await startCharger({
        chargePointId: msg.chargePointId,
        connectorId: msg.connectorId,
        idTag: msg.idTag,
        platformAuthPassword: msg.platformAuthPassword,
        commandedBy: fromEvHub ? "ev-hub" : msg.commandedBy,
      });
      send(ws, {
        id,
        ok: result.status === 200,
        status: result.status,
        ...(result.body?.error ? { error: result.body.error } : {}),
        data: result.body?.data || result.body,
      });
      return;
    }
    if (action === "remoteStop" || action === "RemoteStopTransaction") {
      const result = await stopCharger({
        chargePointId: msg.chargePointId,
        connectorId: msg.connectorId,
        transactionId: msg.transactionId,
        platformAuthPassword: msg.platformAuthPassword,
        commandedBy: fromEvHub ? "ev-hub" : msg.commandedBy,
      });
      send(ws, {
        id,
        ok: result.status === 200,
        status: result.status,
        ...(result.body?.error ? { error: result.body.error } : {}),
        data: result.body?.data || result.body,
      });
      return;
    }
    if (action === "setProxy" || action === "provideProxy") {
      send(ws, {
        id,
        ok: false,
        error: "Proxy setup is not accepted on /bridge.",
      });
      return;
    }
    if (action === "subscribe") {
      const chargePointId = `${msg.chargePointId || ""}`.trim();
      if (!chargePointId) {
        send(ws, { id, ok: false, error: "chargePointId required" });
        return;
      }
      subscribeMeter(ws, chargePointId);
      send(ws, { id, ok: true, data: { chargePointId } });
      return;
    }
    if (action === "ping") {
      send(ws, { id, ok: true, data: { pong: true, ts: Date.now() } });
      return;
    }
    send(ws, { id, ok: false, error: `Unknown action: ${action}` });
  } catch (error) {
    send(ws, {
      id,
      ok: false,
      error: error.message || "Bridge command failed",
    });
  }
}

function ingestMeterEvent(event, data) {
  const chargePointId = `${data?.chargePointId || ""}`.trim();
  if (!chargePointId || !event) return;
  const received = {
    event,
    receivedAt: new Date().toISOString(),
    connectorId: data.connectorId ?? data.payload?.connectorId ?? null,
    transactionId: data.transactionId ?? data.payload?.transactionId ?? null,
    meterValue: data.meterValue || data.payload?.meterValue || null,
    payload: data,
  };
  const history = lastMeterEvents.get(chargePointId) || [];
  history.push(received);
  lastMeterEvents.set(chargePointId, history.slice(-MAX_METER_EVENTS));
  log(
    chargePointId,
    "· meter",
    event === "charger.meterValues"
      ? `RECEIVED ${summarizeMeterPayload(data)} connector=${received.connectorId ?? ""} tx=${received.transactionId ?? ""}`
      : `RECEIVED ${event} ${JSON.stringify(data)}`,
  );
  sendToSubscribers(chargePointId, event, data);
  if (event === "charger.stopTransaction") {
    clearMeterSubscribers(chargePointId);
  }
}

function subscribeMeter(ws, chargePointId) {
  const id = `${chargePointId || ""}`.trim();
  if (!id || !ws) return;
  let listeners = meterSubscribers.get(id);
  if (!listeners) {
    listeners = new Set();
    meterSubscribers.set(id, listeners);
  }
  listeners.add(ws);
  log(
    "bridge",
    "·",
    `meter subscriber added ${id} channel=${ws.channelName || "unnamed"} listeners=${listeners.size}`,
  );
}

function subscribeEvHubListeners(chargePointId) {
  for (const client of bridgeClients) {
    if (client.readyState === WebSocket.OPEN) subscribeMeter(client, chargePointId);
  }
}

function clearMeterSubscribers(chargePointId) {
  const id = `${chargePointId || ""}`.trim();
  if (!id) return;
  meterSubscribers.delete(id);
}

function dropMeterSubscriber(ws) {
  for (const [id, listeners] of meterSubscribers) {
    listeners.delete(ws);
    if (listeners.size === 0) meterSubscribers.delete(id);
  }
}

function sendToSubscribers(chargePointId, event, data) {
  const listeners = meterSubscribers.get(chargePointId);
  if (!listeners || listeners.size === 0) {
    log("centralizer", "·", `${event} for ${chargePointId} has no local /bridge subscribers`);
    return 0;
  }
  let sent = 0;
  for (const client of listeners) {
    if (client.readyState !== client.OPEN) continue;
    client.send(
      JSON.stringify({
        ok: true,
        event,
        data: { ...data, channel: client.channelName || null },
      }),
    );
    sent += 1;
  }
  return sent;
}

function broadcastBridge(event, data, options = {}) {
  const frame = JSON.stringify({ ok: true, event, data });
  for (const client of bridgeClients) {
    if (client.readyState === client.OPEN) client.send(frame);
  }
  // Do not echo events that came FROM ev-hub back TO ev-hub (causes duplicate spam).
  if (!options.localOnly) sendToEvHub(event, data);
}

const lastBridgeSendAt = new Map();
const BRIDGE_SLOW_MS = 5000;

function shouldSendBridge(key) {
  const now = Date.now();
  const last = lastBridgeSendAt.get(key) || 0;
  if (now - last < BRIDGE_SLOW_MS) return false;
  lastBridgeSendAt.set(key, now);
  return true;
}

function notifyChargerConnected(chargePointId, mode, provider) {
  if (!shouldSendBridge(`connected:${chargePointId}`)) return;
  const channel = chargerChannel(chargePointId);
  broadcastBridge("charger.connected", {
    chargePointId,
    mode,
    provider,
    channel,
  });
  log(
    "bridge",
    "·",
    `charger.connected ${chargePointId} channel=${channel} mode=${mode} provider=${provider}`,
  );
}

function notifyChargerDisconnected(chargePointId) {
  const channel = chargerChannel(chargePointId);
  broadcastBridge("charger.disconnected", { chargePointId, channel });
  log(
    "bridge",
    "·",
    `charger.disconnected ${chargePointId} channel=${channel}`,
  );
}

function relayMeterValues(chargePointId, frame) {
  const payload = frame?.[3] || {};
  const charger = chargers.get(chargePointId);
  const tx = charger?.transactions?.get(String(payload.connectorId ?? "1"));
  const meterData = {
    chargePointId,
    connectorId: payload.connectorId ?? null,
    transactionId: payload.transactionId ?? null,
    idTag: tx?.idTag || null,
    meterStart: tx?.meterStart ?? 0,
    meterValue: Array.isArray(payload.meterValue) ? payload.meterValue : [],
    payload,
  };
  // Real MeterValues from the socket replace the auto-mirror ticks.
  if (frame?.[1] !== "mirror") stopPlatformMeterMirror(chargePointId);
  emitToChargerSocket(chargePointId, "charger.meterValues", meterData);
  const sent = sendToSubscribers(chargePointId, "charger.meterValues", meterData);
  const forwarded = sendToEvHub("charger.meterValues", meterData);
  if (charger?.lastMeterValues) {
    charger.lastMeterValues.destination = forwarded
      ? "ev-hub"
      : sent > 0
        ? "centralizer"
        : "virtual-server-local";
    charger.lastMeterValues.provider = forwarded ? "ev-hub" : sent > 0 ? "centralizer" : "local";
    charger.lastMeterValues.listeners = sent;
  }
  log(
    chargePointId,
    "· meter",
    forwarded
      ? `MeterValues → ev-hub bridge connector=${payload.connectorId ?? ""} tx=${payload.transactionId ?? ""}`
      : sent > 0
        ? `MeterValues → subscribed listeners=${sent} connector=${payload.connectorId ?? ""} tx=${payload.transactionId ?? ""}`
        : `MeterValues queued for ev-hub bridge connector=${payload.connectorId ?? ""} tx=${payload.transactionId ?? ""}`,
  );
}

function statusText(value) {
  if (!value) return "";
  if (typeof value === "string") return value.trim();
  return `${value.status || ""}`.trim();
}

function rememberStatus(chargePointId, payload) {
  const charger = chargers.get(chargePointId);
  if (!charger) return;
  const connectorId = Number(payload?.connectorId ?? 0);
  const status = `${payload?.status || ""}`.trim();
  if (!status || !Number.isInteger(connectorId) || connectorId < 0) return;
  if (!charger.connectorStatus) charger.connectorStatus = new Map();
  const record = {
    status,
    errorCode: payload?.errorCode || null,
    info: payload?.info || null,
    timestamp: payload?.timestamp || new Date().toISOString(),
    vendorId: payload?.vendorId || null,
    vendorErrorCode: payload?.vendorErrorCode || null,
    updatedAt: new Date().toISOString(),
  };
  charger.connectorStatus.set(String(connectorId), record);
  log(chargePointId, "·", `connector ${connectorId} status=${status}`);
  emitToChargerSocket(chargePointId, "charger.status", {
    connectorId,
    ...record,
  });
  if (!shouldSendBridge(`status:${chargePointId}:${connectorId}:${status}`)) return;
  const channel = chargerChannel(chargePointId);
  broadcastBridge("charger.status", {
    chargePointId,
    channel,
    connectorId,
    ...record,
  });
}

function rememberBoot(chargePointId, payload) {
  const charger = chargers.get(chargePointId);
  if (!charger) return;
  charger.boot = {
    chargePointVendor: payload?.chargePointVendor || null,
    chargePointModel: payload?.chargePointModel || null,
    chargePointSerialNumber: payload?.chargePointSerialNumber || null,
    firmwareVersion: payload?.firmwareVersion || null,
    updatedAt: new Date().toISOString(),
  };
  log(
    chargePointId,
    "·",
    `boot ${charger.boot.chargePointVendor || ""} ${charger.boot.chargePointModel || ""}`,
  );
  emitToChargerSocket(chargePointId, "charger.boot", { ...charger.boot });
  if (!shouldSendBridge(`boot:${chargePointId}`)) return;
  broadcastBridge("charger.boot", {
    chargePointId,
    channel: chargerChannel(chargePointId),
    ...charger.boot,
  });
}

function onBridgeConnection(ws) {
  bridgeClients.add(ws);
  log("bridge", "·", "ev-hub bridge connected");
  ws.on("message", (raw) => {
    handleBridgeMessage(ws, raw).catch((error) =>
      log("bridge", "·", `message handler ${error.message}`),
    );
  });
  ws.on("close", () => {
    bridgeClients.delete(ws);
    dropMeterSubscriber(ws);
    log("bridge", "·", "ev-hub bridge disconnected");
  });
  ws.on("error", (error) => log("bridge", "·", `error ${error.message}`));
  send(ws, {
    ok: true,
    event: "connected",
    data: {
      path: BRIDGE_PATH,
      httpBase: PUBLIC_URL,
      chargers: listChargers().map((c) => ({
        chargePointId: c.chargePointId,
        channel: chargerChannel(c.chargePointId),
      })),
    },
  });
}

function notifyChargerHeartbeat(chargePointId) {
  const channel = chargerChannel(chargePointId);
  if (!shouldSendBridge(`heartbeat:${chargePointId}`)) return;
  broadcastBridge("charger.heartbeat", {
    chargePointId,
    channel,
    currentTime: new Date().toISOString(),
  });
  log("bridge", "·", `charger.heartbeat ${chargePointId} channel=${channel}`);
}

const server = http.createServer(handleHttp);

const wss = new WebSocketServer({
  noServer: true,
  handleProtocols: () => "ocpp1.6",
});

server.on("upgrade", (request, socket, head) => {
  const pathname = (request.url || "/").split("?")[0];

  if (pathname === BRIDGE_PATH || pathname === `${BRIDGE_PATH}/`) {
    wss.handleUpgrade(request, socket, head, (ws) => onBridgeConnection(ws));
    return;
  }

  const chargePointId = extractChargePointId(request.url);
  if (!chargePointId) {
    log("centralizer", "·", `rejected OCPP upgrade (404): no chargePointId in "${request.url}"`);
    socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }

  const platformAuthPassword = readPlatformAuthPassword(request.url);
  if (!passwordsMatch(platformAuthPassword, SIMULATOR_MOCK_PASSWORD)) {
    log(
      chargePointId,
      "·",
      `rejected OCPP upgrade (401): platformAuthPassword did not match SIMULATOR_MOCK_PASSWORD`,
    );
    socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (ws) =>
    onChargerConnection(ws, chargePointId, platformAuthPassword),
  );
});

server.listen(PORT, "0.0.0.0", () => {
  const publicOcpp = `${PUBLIC_URL.replace(/^http/i, "ws")}${WS_PATH}`;
  console.log(`centralizer virtual-server (local CSMS)`);
  console.log(`OCPP WS ws://0.0.0.0:${PORT}${WS_PATH}/{chargePointId}`);
  console.log(
    `Public OCPP ${publicOcpp}/{chargePointId}?platformAuthPassword=${SIMULATOR_MOCK_PASSWORD}`,
  );
  console.log(`simulator password ${SIMULATOR_MOCK_PASSWORD}`);
  console.log(`mock pricePerKwh ${localMockPrice()}`);
  console.log(`HTTP API      http://0.0.0.0:${PORT}/api/proxy`);
  console.log(`  POST /api/proxy/start`);
  console.log(`  POST /api/proxy/stop`);
  console.log(`  GET  /api/proxy/events`);
  console.log(`ev-hub bridge listen ws://0.0.0.0:${PORT}${BRIDGE_PATH}`);
  if (EV_HUB_PROXY_URL) {
    console.log(
      `ev-hub proxy  ${EV_HUB_PROXY_URL}/ocpp/proxy  provider=${EV_HUB_PROVIDER}`,
    );
  } else {
    console.log(`ev-hub command ${evHubBridgeUrl()} remoteStart|remoteStop`);
  }
  ensureEvHubSocket().catch((error) => {
    log("centralizer", "·", `ev-hub bridge connect failed ${error.message}`);
  });
});
