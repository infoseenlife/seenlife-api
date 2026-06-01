import { createServer } from "node:http";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "0.0.0.0";
const DB_PATH = resolve(process.env.SEENLIFE_DB_PATH || "./data/seenlife-db.json");
const DEEPSEEK_URL = "https://api.deepseek.com/chat/completions";

const MODELS = {
  "deepseek/deepseek-v4-flash": {
    id: "deepseek/deepseek-v4-flash",
    provider: "deepseek",
    upstreamModel: "deepseek-v4-flash",
    inputUsdPerMillion: 0.25,
    outputUsdPerMillion: 0.5
  },
  "deepseek/deepseek-v4-pro": {
    id: "deepseek/deepseek-v4-pro",
    provider: "deepseek",
    upstreamModel: "deepseek-v4-pro",
    inputUsdPerMillion: 1.576642,
    outputUsdPerMillion: 3.153285
  }
};

const EMPTY_DB = { users: [], apiKeys: [], ledger: [], usageLogs: [] };

const server = createServer(async (req, res) => {
  try {
    if (req.method === "OPTIONS") return sendJson(res, 204, {});
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === "GET" && url.pathname === "/health") {
      return sendJson(res, 200, { ok: true, service: "seenlife-api", time: new Date().toISOString() });
    }

    if (req.method === "GET" && url.pathname === "/v1/models") {
      return sendJson(res, 200, {
        object: "list",
        data: Object.values(MODELS).map((model) => ({
          id: model.id,
          object: "model",
          owned_by: "seenlife",
          pricing: {
            input_usd_per_1m_tokens: model.inputUsdPerMillion,
            output_usd_per_1m_tokens: model.outputUsdPerMillion
          }
        }))
      });
    }

    if (req.method === "GET" && url.pathname === "/dashboard/me") {
      const auth = await authenticate(req);
      return sendJson(res, 200, {
        user: publicUser(auth.user),
        apiKey: {
          name: auth.apiKey.name,
          createdAt: auth.apiKey.createdAt,
          lastUsedAt: auth.apiKey.lastUsedAt || null
        }
      });
    }

    if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
      return handleChat(req, res);
    }

    if (url.pathname.startsWith("/admin/")) {
      requireAdmin(req);
      return handleAdmin(req, res, url);
    }

    return sendJson(res, 404, { error: { message: "Route not found", type: "not_found" } });
  } catch (error) {
    return sendError(res, error);
  }
});

server.listen(PORT, HOST, () => {
  console.log(`Seenlife API listening on http://${HOST}:${PORT}`);
});

async function handleChat(req, res) {
  const body = await readJson(req);
  if (body.stream === true) {
    throw httpError("Streaming is not supported in v1 yet", 400);
  }

  const model = MODELS[body.model];
  if (!model) throw httpError(`Unsupported model: ${body.model}`, 400, "model_not_supported");

  const auth = await authenticate(req);
  if (auth.user.balanceMicroUsd < 1) throw httpError("Insufficient balance", 402, "insufficient_balance");

  const startedAt = Date.now();
  const upstream = await callDeepSeek(model.upstreamModel, body);
  const cost = calculateUsageCost(model, upstream.usage || {});

  const result = await withDb(async (db) => {
    const apiKey = db.apiKeys.find((key) => key.keyHash === auth.apiKey.keyHash && !key.revokedAt);
    const user = apiKey ? findUserById(db, apiKey.userId) : null;
    if (!apiKey || !user) throw httpError("API key is no longer valid", 401);

    if (user.balanceMicroUsd < cost.totalMicroUsd) {
      throw httpError(`Insufficient balance. Required $${formatUsd(cost.totalMicroUsd)}, available $${formatUsd(user.balanceMicroUsd)}`, 402, "insufficient_balance");
    }

    user.balanceMicroUsd -= cost.totalMicroUsd;
    user.updatedAt = nowIso();
    apiKey.lastUsedAt = nowIso();

    const usageLog = {
      id: `usage_${randomUUID()}`,
      userId: user.id,
      apiKeyId: apiKey.id,
      model: model.id,
      provider: model.provider,
      upstreamModel: model.upstreamModel,
      inputTokens: cost.inputTokens,
      outputTokens: cost.outputTokens,
      totalTokens: cost.totalTokens,
      chargedMicroUsd: cost.totalMicroUsd,
      latencyMs: Date.now() - startedAt,
      createdAt: nowIso()
    };

    db.usageLogs.push(usageLog);
    db.ledger.push({
      id: `txn_${randomUUID()}`,
      userId: user.id,
      type: "usage",
      amountMicroUsd: -cost.totalMicroUsd,
      balanceAfterMicroUsd: user.balanceMicroUsd,
      referenceId: usageLog.id,
      note: `${model.id} ${cost.totalTokens} tokens`,
      createdAt: nowIso()
    });

    return { usageLog, balanceMicroUsd: user.balanceMicroUsd };
  });

  return sendJson(res, 200, {
    ...upstream,
    seenlife: {
      charged_usd: Number(formatUsd(cost.totalMicroUsd)),
      balance_usd: Number(formatUsd(result.balanceMicroUsd)),
      usage_log_id: result.usageLog.id
    }
  });
}

async function handleAdmin(req, res, url) {
  if (req.method === "POST" && url.pathname === "/admin/users") {
    const body = await readJson(req);
    if (!body.email) throw httpError("email is required", 400);

    const user = await withDb(async (db) => {
      const existing = findUserByEmail(db, body.email);
      if (existing) return existing;
      const created = createUserRecord({ email: body.email, name: body.name });
      db.users.push(created);
      return created;
    });

    return sendJson(res, 200, { user: publicUser(user) });
  }

  if (req.method === "POST" && url.pathname === "/admin/topups") {
    const body = await readJson(req);
    const amountMicroUsd = usdToMicro(body.amountUsd);
    if (amountMicroUsd <= 0) throw httpError("amountUsd must be greater than 0", 400);

    const user = await withDb(async (db) => {
      const target = findUserByEmail(db, body.email);
      if (!target) throw httpError("User not found. Create the user first.", 404);
      target.balanceMicroUsd += amountMicroUsd;
      target.updatedAt = nowIso();
      db.ledger.push({
        id: `txn_${randomUUID()}`,
        userId: target.id,
        type: "topup",
        amountMicroUsd,
        balanceAfterMicroUsd: target.balanceMicroUsd,
        referenceId: body.orderId || null,
        note: body.note || "Manual top-up",
        createdAt: nowIso()
      });
      return target;
    });

    return sendJson(res, 200, { user: publicUser(user) });
  }

  if (req.method === "POST" && url.pathname === "/admin/api-keys") {
    const body = await readJson(req);
    const plainKey = createApiKey();
    const key = await withDb(async (db) => {
      const user = findUserByEmail(db, body.email);
      if (!user) throw httpError("User not found. Create the user first.", 404);
      const record = {
        id: `key_${randomUUID()}`,
        userId: user.id,
        name: body.name || "Default key",
        keyHash: hashApiKey(plainKey),
        createdAt: nowIso(),
        lastUsedAt: null,
        revokedAt: null
      };
      db.apiKeys.push(record);
      return record;
    });

    return sendJson(res, 201, {
      apiKey: {
        id: key.id,
        key: plainKey,
        name: key.name,
        createdAt: key.createdAt
      }
    });
  }

  if (req.method === "GET" && url.pathname === "/admin/users") {
    const db = await readDb();
    return sendJson(res, 200, { users: db.users.map(publicUser) });
  }

  if (req.method === "GET" && url.pathname === "/admin/usage") {
    const db = await readDb();
    return sendJson(res, 200, { usageLogs: db.usageLogs.slice(-200).reverse() });
  }

  return sendJson(res, 404, { error: { message: "Route not found", type: "not_found" } });
}

async function callDeepSeek(upstreamModel, body) {
  const apiKey = process.env.DEEPSEEK_API_KEY;
  if (!apiKey) throw httpError("DEEPSEEK_API_KEY is not configured", 500);

  const response = await fetch(DEEPSEEK_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({ ...body, model: upstreamModel, stream: false })
  });

  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { error: { message: text || "Invalid upstream response" } };
  }

  if (!response.ok) {
    throw httpError(data?.error?.message || "DeepSeek upstream request failed", response.status);
  }

  return data;
}

async function authenticate(req) {
  const token = bearerToken(req);
  if (!token) throw httpError("Missing Bearer API key", 401);

  const db = await readDb();
  const apiKey = db.apiKeys.find((key) => key.keyHash === hashApiKey(token) && !key.revokedAt);
  if (!apiKey) throw httpError("Invalid API key", 401);

  const user = findUserById(db, apiKey.userId);
  if (!user) throw httpError("API key user was not found", 401);

  return { db, apiKey, user };
}

async function readDb() {
  try {
    const content = await readFile(DB_PATH, "utf8");
    return { ...EMPTY_DB, ...JSON.parse(content) };
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    await writeDb(EMPTY_DB);
    return structuredClone(EMPTY_DB);
  }
}

async function writeDb(db) {
  await mkdir(dirname(DB_PATH), { recursive: true });
  const temp = `${DB_PATH}.${Date.now()}.tmp`;
  await writeFile(temp, `${JSON.stringify(db, null, 2)}\n`);
  await rename(temp, DB_PATH);
}

async function withDb(mutator) {
  const db = await readDb();
  const result = await mutator(db);
  await writeDb(db);
  return result;
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw httpError("Request body must be valid JSON", 400);
  }
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "authorization, content-type",
    "access-control-allow-methods": "GET, POST, OPTIONS"
  });
  res.end(JSON.stringify(payload));
}

function sendError(res, error) {
  const status = error.statusCode || 500;
  sendJson(res, status, {
    error: {
      message: status === 500 ? "Internal server error" : error.message,
      type: "seenlife_error",
      code: error.code || undefined
    }
  });
  if (status === 500) console.error(error);
}

function requireAdmin(req) {
  const expected = process.env.SEENLIFE_ADMIN_TOKEN;
  if (!expected || expected === "change-me") throw httpError("SEENLIFE_ADMIN_TOKEN is not configured", 500);
  if (bearerToken(req) !== expected) throw httpError("Admin token is invalid", 401);
}

function bearerToken(req) {
  const match = String(req.headers.authorization || "").match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

function createUserRecord({ email, name }) {
  return {
    id: `user_${randomUUID()}`,
    email: normalizeEmail(email),
    name: String(name || "").trim(),
    balanceMicroUsd: 0,
    createdAt: nowIso(),
    updatedAt: nowIso()
  };
}

function publicUser(user) {
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    balanceUsd: user.balanceMicroUsd / 1_000_000,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt
  };
}

function findUserByEmail(db, email) {
  return db.users.find((user) => user.email === normalizeEmail(email)) || null;
}

function findUserById(db, userId) {
  return db.users.find((user) => user.id === userId) || null;
}

function calculateUsageCost(model, usage) {
  const inputTokens = Number(usage.prompt_tokens || usage.input_tokens || 0);
  const outputTokens = Number(usage.completion_tokens || usage.output_tokens || 0);
  const totalTokens = Number(usage.total_tokens || inputTokens + outputTokens);
  const inputMicroUsd = Math.ceil((inputTokens * model.inputUsdPerMillion * 1_000_000) / 1_000_000);
  const outputMicroUsd = Math.ceil((outputTokens * model.outputUsdPerMillion * 1_000_000) / 1_000_000);
  return {
    inputTokens,
    outputTokens,
    totalTokens,
    totalMicroUsd: inputMicroUsd + outputMicroUsd
  };
}

function usdToMicro(value) {
  return Math.round(Number(value || 0) * 1_000_000);
}

function formatUsd(microUsd) {
  return (microUsd / 1_000_000).toFixed(6);
}

function hashApiKey(apiKey) {
  return createHash("sha256").update(apiKey).digest("hex");
}

function createApiKey() {
  return `sk-seenlife-${randomBytes(24).toString("base64url")}`;
}

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function nowIso() {
  return new Date().toISOString();
}

function httpError(message, statusCode, code) {
  const error = new Error(message);
  error.statusCode = statusCode;
  if (code) error.code = code;
  return error;
}
