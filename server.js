const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const rooms = new Map();
const clients = new Map();

const MAX_PLAYERS_PER_ROOM = 10;
const MAX_WORLD_BYTES = 700000;
const MAX_SKIN_BYTES = 500000;
const HOST_RECONNECT_GRACE_MS = 120000;
const MAX_MESSAGES_PER_SECOND = 120;
const RESET_COOLDOWN_MS = 750;
const RESET_RANGE = 100;
const HEARTBEAT_MS = 30000;
const EMBED_COOLDOWN_MS = 2000;
const MAX_EMBED_URL_LENGTH = 500;
const PARTICLE_KINDS = new Set(["spark", "smoke", "glitch", "confetti"]);

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end("Flux multiplayer servers are enabled! If they stop, check back again! (Game link: https://calico-boy.itch.io/flux-sandbox)");
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocket.Server({
  server,
  maxPayload: 1024 * 1024
});

/* ---------- helpers ---------- */

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function sendError(ws, message) {
  send(ws, { type: "error", message });
}

function broadcast(room, data, except = null) {
  for (const client of room.clients.values()) {
    if (client !== except) send(client, data);
  }
}

// Skins can be hundreds of KB, so they are only included when needed
// (joins, skin changes) and left out of the ~10/sec movement packets.
function playerPacket(state, includeSkin = true) {
  const packet = {
    id: state.id,
    name: state.name,
    pos: state.pos,
    yaw: state.yaw
  };

  if (includeSkin) packet.skin = state.skin;
  return packet;
}

function cleanName(name) {
  return String(name || "Player")
    .replace(/[<>\u0000-\u001f]/g, "")
    .trim()
    .slice(0, 20) || "Player";
}

function cleanCode(code) {
  return String(code || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 12);
}

function createCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code;

  do {
    code = Array.from(
      { length: 5 },
      () => chars[crypto.randomInt(chars.length)]
    ).join("");
  } while (rooms.has(code));

  return code;
}

function isValidPos(pos) {
  return (
    Array.isArray(pos) &&
    pos.length === 3 &&
    pos.every(n => typeof n === "number" && Number.isFinite(n) && Math.abs(n) < 100000)
  );
}

function safeWorld(input) {
  if (
    !input ||
    !Array.isArray(input.bricks) ||
    input.bricks.length > 12000
  ) {
    return null;
  }

  let encoded;

  try {
    encoded = JSON.stringify(input);
  } catch {
    return null;
  }

  if (Buffer.byteLength(encoded) > MAX_WORLD_BYTES) return null;

  return {
    bricks: input.bricks,
    worldName: String(input.worldName || "My World").slice(0, 80),
    settings: input.settings || null
  };
}

// Shared embeds must be plain https links (no embedded credentials).
function cleanEmbedUrl(input) {
  const text = String(input || "").trim();
  if (!text || text.length > MAX_EMBED_URL_LENGTH) return null;

  let url;

  try {
    url = new URL(text);
  } catch {
    return null;
  }

  if (url.protocol !== "https:" || url.username || url.password) return null;
  if (url.href.length > MAX_EMBED_URL_LENGTH) return null;
  return url.href;
}

function cleanEmbedTitle(title) {
  return String(title || "Shared embed")
    .replace(/[<>\u0000-\u001f]/g, "")
    .trim()
    .slice(0, 60) || "Shared embed";
}

function embedPacket(room) {
  return {
    type: "embed_share",
    url: room.embed.url,
    title: room.embed.title,
    byId: room.embed.byId,
    byName: room.embed.byName
  };
}

function safeSkin(skin) {
  if (!skin || typeof skin !== "object") return { ok: true, skin: null };

  let encoded;

  try {
    encoded = JSON.stringify(skin);
  } catch {
    return { ok: false };
  }

  if (Buffer.byteLength(encoded) > MAX_SKIN_BYTES) return { ok: false };
  return { ok: true, skin };
}

function tokensMatch(a, b) {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}

function otherPlayers(room, selfId) {
  const players = [];

  for (const [id, client] of room.clients) {
    if (id === selfId) continue;
    const other = clients.get(client);
    if (other) players.push(playerPacket(other));
  }

  return players;
}

function removeRoom(room) {
  clearTimeout(room.expireTimer);
  room.expireTimer = null;
  rooms.delete(room.code);
}

/**
 * Removes a socket from its room.
 *  - deliberate = false: the connection dropped. A host gets a grace period
 *    to resume the room with their host token.
 *  - deliberate = true: the player chose to leave (created/joined another room).
 *    A host leaving on purpose closes the room right away.
 */
function leave(ws, deliberate = false) {
  const state = clients.get(ws);
  if (!state || !state.room) return;

  const room = rooms.get(state.room);
  state.room = null;

  if (!room) return;

  room.clients.delete(state.id);

  if (room.hostId === state.id) {
    if (deliberate) {
      broadcast(room, { type: "room_closed" });
      removeRoom(room);
      return;
    }

    room.hostDisconnectedAt = Date.now();

    // The host gets a new id when they resume, so drop the old avatar now.
    broadcast(room, { type: "player_left", id: state.id });
    broadcast(room, {
      type: "host_disconnected",
      graceMs: HOST_RECONNECT_GRACE_MS
    });

    clearTimeout(room.expireTimer);

    room.expireTimer = setTimeout(() => {
      if (rooms.get(room.code) === room && room.hostDisconnectedAt) {
        broadcast(room, { type: "room_closed" });
        removeRoom(room);
      }
    }, HOST_RECONNECT_GRACE_MS);

    room.expireTimer.unref?.();
    return;
  }

  broadcast(room, { type: "player_left", id: state.id });

  if (room.clients.size === 0 && !room.hostDisconnectedAt) {
    removeRoom(room);
  }
}

/* ---------- connections ---------- */

wss.on("connection", (ws) => {
  const state = {
    id: crypto.randomUUID(),
    room: null,
    name: "Player",
    pos: [0, 3, 0],
    yaw: 0,
    skin: null,
    windowStart: Date.now(),
    windowCount: 0,
    lastReset: 0,
    lastEmbed: 0
  };

  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  clients.set(ws, state);
  send(ws, { type: "connected", id: state.id });

  ws.on("message", (raw) => {
    try {
      handleMessage(raw);
    } catch (err) {
      console.error("Message handler error:", err);
      sendError(ws, "Server error handling that message.");
    }
  });

  function handleMessage(raw) {
    // Basic flood protection.
    const now = Date.now();

    if (now - state.windowStart >= 1000) {
      state.windowStart = now;
      state.windowCount = 0;
    }

    if (++state.windowCount > MAX_MESSAGES_PER_SECOND) return;

    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return sendError(ws, "Invalid message format.");
    }

    if (!msg || typeof msg.type !== "string") return;

    /* ----- room lifecycle ----- */

    if (msg.type === "create_room") {
      leave(ws, true);

      const code = createCode();
      const hostToken = crypto.randomBytes(24).toString("hex");

      state.room = code;
      state.name = cleanName(msg.name);

      const room = {
        code,
        hostId: state.id,
        hostToken,
        hostDisconnectedAt: null,
        expireTimer: null,
        clients: new Map([[state.id, ws]]),
        embed: null,
        world: {
          bricks: [],
          worldName: "My World",
          settings: null
        }
      };

      rooms.set(code, room);

      send(ws, {
        type: "room_created",
        id: state.id,
        code,
        hostToken,
        players: []
      });

      return;
    }

    if (msg.type === "resume_room") {
      const code = cleanCode(msg.code);
      const room = rooms.get(code);
      const suppliedToken = String(msg.hostToken || "");

      if (
        !room ||
        !suppliedToken ||
        !tokensMatch(suppliedToken, room.hostToken)
      ) {
        return sendError(ws, "That room can’t be resumed, create a new room!");
      }

      // Already the live host of this room on this very socket: just confirm.
      if (state.room === code && room.hostId === state.id) {
        send(ws, {
          type: "room_resumed", id: state.id, code, hostToken: room.hostToken,
          players: otherPlayers(room, state.id), world: room.world
        });
        return;
      }

      // The old host connection may be half-open (not yet noticed as dead).
      // The valid host token wins: retire that socket so resuming works right away.
      if (!room.hostDisconnectedAt) {
        const oldWs = room.clients.get(room.hostId);
        if (oldWs && oldWs !== ws) {
          const oldState = clients.get(oldWs);
          if (oldState) oldState.room = null;
          room.clients.delete(room.hostId);
          broadcast(room, { type: "player_left", id: room.hostId });
          try { oldWs.close(4000, "Replaced by a newer connection"); } catch {}
        }
      }

      leave(ws, true);

      clearTimeout(room.expireTimer);
      room.expireTimer = null;
      room.hostDisconnectedAt = null;
      room.hostId = state.id;

      state.room = code;
      state.name = cleanName(msg.name);

      room.clients.set(state.id, ws);

      send(ws, {
        type: "room_resumed",
        id: state.id,
        code,
        hostToken: room.hostToken,
        players: otherPlayers(room, state.id),
        world: room.world
      });

      broadcast(room, { type: "host_reconnected" }, ws);
      return;
    }

    if (msg.type === "join_room") {
      const code = cleanCode(msg.code);
      const room = rooms.get(code);

      if (!room) {
        return sendError(ws, "Room code was not found, maybe try another");
      }

      if (room.hostDisconnectedAt) {
        return sendError(
          ws,
          "The host has disconnected from the server! Ask them to reconnect or create a new room."
        );
      }

      if (state.room === code) {
        // Already in this room: just resend the room info.
        send(ws, {
          type: "room_joined",
          id: state.id,
          code,
          players: otherPlayers(room, state.id)
        });
        send(ws, { type: "world_sync_ready", world: room.world });
        if (room.embed) send(ws, { ...embedPacket(room), late: true });
        return;
      }

      if (room.clients.size >= MAX_PLAYERS_PER_ROOM) {
        return sendError(ws, "That server is full! Maybe try again later. ;)");
      }

      leave(ws, true);

      state.room = code;
      state.name = cleanName(msg.name);

      room.clients.set(state.id, ws);

      send(ws, {
        type: "room_joined",
        id: state.id,
        code,
        players: otherPlayers(room, state.id)
      });

      broadcast(room, { type: "player_joined", player: playerPacket(state) }, ws);

      send(ws, { type: "world_sync_ready", world: room.world });
      if (room.embed) send(ws, { ...embedPacket(room), late: true });
      return;
    }

    if (msg.type === "leave_room") {
      leave(ws, true);
      return;
    }

    /* ----- everything below needs a room ----- */

    if (!state.room) {
      return sendError(ws, "Create or join a room first!");
    }

    const room = rooms.get(state.room);
    if (!room) return;

    if (msg.type === "world_sync_request") {
      send(ws, {
        type: "world_sync_ready",
        world: room.world || { bricks: [], worldName: "My World" }
      });
      return;
    }

    if (msg.type === "world_load") {
      if (state.id !== room.hostId) {
        return sendError(ws, "Only the room owner can load a saved world.");
      }

      const world = safeWorld(msg.world || msg.state || msg);

      if (!world) {
        return sendError(ws, "Saved world was invalid or too large for our servers :(");
      }

      room.world = world;
      broadcast(room, { type: "world_state", world: room.world }, ws);
      return;
    }

    if (msg.type === "world_state" || msg.type === "world_update") {
      const world = safeWorld(msg.world || msg.state || msg);

      if (!world) {
        return sendError(
          ws,
          "Shared world update was invalid or too large for our servers :("
        );
      }

      room.world = world;
      broadcast(room, { type: "world_state", world: room.world }, ws);
      return;
    }

    if (msg.type === "embed_share") {
      if (state.id !== room.hostId) {
        return sendError(ws, "Only the room owner can share an embed.");
      }

      const url = cleanEmbedUrl(msg.url);

      if (!url) {
        return sendError(ws, "Shared embeds must be https:// links.");
      }

      if (now - state.lastEmbed < EMBED_COOLDOWN_MS) {
        return sendError(ws, "Slow down! Wait a moment before sharing again.");
      }

      state.lastEmbed = now;

      room.embed = {
        url,
        title: cleanEmbedTitle(msg.title),
        byId: state.id,
        byName: state.name
      };

      broadcast(room, embedPacket(room), ws);
      return;
    }

    if (msg.type === "embed_close") {
      if (state.id !== room.hostId) {
        return sendError(ws, "Only the room owner can close a shared embed.");
      }

      room.embed = null;
      broadcast(room, { type: "embed_close" }, ws);
      return;
    }

    if (msg.type === "particle_effect") {
      if (!PARTICLE_KINDS.has(msg.kind) || !isValidPos(msg.pos)) {
        return sendError(ws, "Invalid particle effect.");
      }

      broadcast(room, {
        type: "particle_effect",
        id: state.id,
        kind: msg.kind,
        pos: msg.pos.map(Number)
      }, ws);
      return;
    }

    if (msg.type === "player_name") {
      state.name = cleanName(msg.name);

      broadcast(room, { type: "player_state", player: playerPacket(state, false) }, ws);
      send(ws, { type: "player_name", player: playerPacket(state, false) });
      return;
    }

    if (msg.type === "player_skin" || msg.type === "player_avatar") {
      const result = safeSkin(msg.skin);

      if (!result.ok) {
        return sendError(
          ws,
          "Skin data is too large to share. (We cant render that much!)"
        );
      }

      state.skin = result.skin;
      if (msg.name) state.name = cleanName(msg.name);

      broadcast(room, { type: "player_skin", player: playerPacket(state) }, ws);
      return;
    }

    if (msg.type === "player_reset_request") {
      const targetId = String(msg.targetId || "");
      const targetWs = room.clients.get(targetId);
      const targetState = targetWs ? clients.get(targetWs) : null;

      if (!targetState || targetId === state.id) {
        return sendError(
          ws,
          "Target player was not found in this server, maybe try again! (Or it might be a ghost player)"
        );
      }

      if (now - state.lastReset < RESET_COOLDOWN_MS) {
        return sendError(ws, "Slow down! Wait a moment before resetting again.");
      }

      const dx = state.pos[0] - targetState.pos[0];
      const dy = state.pos[1] - targetState.pos[1];
      const dz = state.pos[2] - targetState.pos[2];
      const distance = Math.sqrt(dx * dx + dy * dy + dz * dz);

      if (distance > RESET_RANGE) {
        return sendError(ws, "Player isn't in range!");
      }

      state.lastReset = now;

      broadcast(room, {
        type: "player_reset",
        targetId,
        byId: state.id,
        spawn: [0, 3.2, 0]
      });

      // Keep the server's copy in sync so repeat resets use the spawn position.
      targetState.pos = [0, 3.2, 0];
      return;
    }

    if (msg.type === "player_state") {
      if (!isValidPos(msg.pos)) return;

      state.pos = msg.pos.map(Number);

      if (Number.isFinite(msg.yaw)) state.yaw = msg.yaw;
      if (msg.name) state.name = cleanName(msg.name);

      // A skin may ride along, but it is never rebroadcast with movement.
      if (msg.skin && typeof msg.skin === "object") {
        const result = safeSkin(msg.skin);
        if (result.ok) state.skin = result.skin;
      }

      broadcast(room, { type: "player_state", player: playerPacket(state, false) }, ws);
    }
  }

  ws.on("close", () => {
    leave(ws, false);
    clients.delete(ws);
  });

  ws.on("error", () => {
    leave(ws, false);
    clients.delete(ws);
  });
});

// Drop dead connections (also keeps the host alive on proxies like Render).
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;

    try {
      ws.ping();
    } catch {
      ws.terminate();
    }
  }
}, HEARTBEAT_MS);

heartbeat.unref?.();
wss.on("close", () => clearInterval(heartbeat));

const port = Number(process.env.PORT) || 3000;

server.listen(port, "0.0.0.0", () => {
  console.log(`FLUX multiplayer server listening on ${port}`);
});

function shutdown() {
  for (const ws of wss.clients) {
    try { ws.close(1001, "Server restarting"); } catch {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}

process.on("uncaughtException", (err) => console.error("Uncaught exception:", err));
process.on("unhandledRejection", (err) => console.error("Unhandled rejection:", err));
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
