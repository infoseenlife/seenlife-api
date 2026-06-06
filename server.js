import { createServer } from "node:http";
import { createHash, createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { MODELS } from "./models.js";

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "0.0.0.0";
const DB_PATH = resolve(process.env.SEENLIFE_DB_PATH || "./data/seenlife-db.json");
const TOKENMIX_URL = process.env.TOKENMIX_BASE_URL || "https://api.tokenmix.ai/v1/chat/completions";
const OPENROUTER_URL = process.env.OPENROUTER_BASE_URL || "https://openrouter.ai/api/v1/chat/completions";

const SHOPIFY_CREDIT_PRODUCT_RE = /(seenlife\s+api\s+(balance|credits?)|starter\s+api\s+credits?|pro\s+api\s+credits?|business\s+api\s+credits?|api\s+credits?)/i;
const EMPTY_DB = { users: [], apiKeys: [], ledger: [], usageLogs: [] };

const server = createServer(async (req, res) => {
  try {
    if (req.method === "OPTIONS") return sendJson(res, 204, {});
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === "GET" && url.pathname === "/") {
      return sendHtml(res, 200, landingPage());
    }

    if (req.method === "GET" && url.pathname === "/admin") {
      return sendHtml(res, 200, adminPage());
    }

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

    if (req.method === "POST" && url.pathname === "/webhooks/shopify/orders-paid") {
      return handleShopifyOrderPaid(req, res);
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
  const upstream = await callModelGateway(model, body);
  const billableUsage = normalizeBillableUsage(upstream.usage);
  if (!billableUsage) {
    throw httpError("Provider did not return billable token usage. Request was not charged.", 502, "missing_usage");
  }
  const cost = calculateUsageCost(model, billableUsage);

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
      upstreamProvider: upstream.seenlifeUpstreamProvider || model.upstreamProvider,
      upstreamModel: model.upstreamModel,
      inputTokens: cost.inputTokens,
      outputTokens: cost.outputTokens,
      totalTokens: cost.totalTokens,
      upstreamCostMicroUsd: cost.upstreamMicroUsd,
      chargedMicroUsd: cost.totalMicroUsd,
      grossProfitMicroUsd: cost.grossProfitMicroUsd,
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

  const { seenlifeUpstreamProvider, ...customerResponse } = upstream;

  return sendJson(res, 200, {
    ...customerResponse,
    seenlife: {
      charged_usd: Number(formatUsd(cost.totalMicroUsd)),
      balance_usd: Number(formatUsd(result.balanceMicroUsd)),
      usage_log_id: result.usageLog.id
    }
  });
}

async function handleShopifyOrderPaid(req, res) {
  const rawBody = await readRawBody(req);
  verifyShopifyWebhook(req, rawBody);

  let order;
  try {
    order = JSON.parse(rawBody.toString("utf8"));
  } catch {
    throw httpError("Shopify webhook body must be valid JSON", 400);
  }

  const topup = parseShopifyTopup(order);
  if (topup.amountMicroUsd <= 0) {
    return sendJson(res, 200, { ok: true, ignored: true, reason: "No Seenlife API balance product found" });
  }

  const result = await processShopifyTopup(topup);
  await sendTopupEmail(result);

  return sendJson(res, 200, {
    ok: true,
    duplicate: result.duplicate,
    email: result.user.email,
    amountUsd: Number(formatUsd(topup.amountMicroUsd)),
    balanceUsd: result.user.balanceUsd,
    apiKeyGenerated: Boolean(result.plainApiKey)
  });
}

async function processShopifyTopup(topup) {
  let plainApiKey = "";

  const result = await withDb(async (db) => {
    let user = findUserByEmail(db, topup.email);
    if (!user) {
      user = createUserRecord({ email: topup.email, name: topup.name });
      db.users.push(user);
    } else if (!user.name && topup.name) {
      user.name = topup.name;
    }

    const duplicate = db.ledger.some((txn) => txn.type === "topup" && txn.referenceId === topup.orderReference);
    const existingApiKey = db.apiKeys.find((key) => key.userId === user.id && !key.revokedAt);

    if (!existingApiKey) {
      plainApiKey = createApiKey();
      db.apiKeys.push({
        id: `key_${randomUUID()}`,
        userId: user.id,
        name: "Main key",
        keyHash: hashApiKey(plainApiKey),
        createdAt: nowIso(),
        lastUsedAt: null,
        revokedAt: null
      });
    }

    if (!duplicate) {
      user.balanceMicroUsd += topup.amountMicroUsd;
      user.updatedAt = nowIso();
      db.ledger.push({
        id: `txn_${randomUUID()}`,
        userId: user.id,
        type: "topup",
        amountMicroUsd: topup.amountMicroUsd,
        balanceAfterMicroUsd: user.balanceMicroUsd,
        referenceId: topup.orderReference,
        note: `Shopify order ${topup.orderName}`,
        createdAt: nowIso()
      });
    }

    return {
      duplicate,
      user: publicUser(user),
      orderName: topup.orderName,
      plainApiKey,
      hasExistingApiKey: Boolean(existingApiKey)
    };
  });

  return {
    ...result,
    topupAmountUsd: formatUsd(topup.amountMicroUsd),
    lineItems: topup.lineItems
  };
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

  if (req.method === "POST" && url.pathname === "/admin/resend-topup-email") {
    const body = await readJson(req);
    const emailResult = await buildManualTopupEmailResult(body);
    await sendTopupEmail(emailResult);
    return sendJson(res, 200, {
      ok: true,
      email: emailResult.user.email,
      orderName: emailResult.orderName,
      apiKeyIncluded: Boolean(emailResult.plainApiKey)
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

async function callModelGateway(model, body) {
  try {
    const data = await callOpenAiCompatibleGateway({
      url: TOKENMIX_URL,
      apiKey: process.env.TOKENMIX_API_KEY,
      apiKeyName: "TOKENMIX_API_KEY",
      modelName: model.upstreamModel,
      body
    });
    data.seenlifeUpstreamProvider = "primary";
    return data;
  } catch (error) {
    if (!process.env.OPENROUTER_API_KEY) throw error;
    console.warn(`Primary model gateway failed for ${model.id}; trying fallback gateway:`, error.message);
    const data = await callOpenAiCompatibleGateway({
      url: OPENROUTER_URL,
      apiKey: process.env.OPENROUTER_API_KEY,
      apiKeyName: "OPENROUTER_API_KEY",
      modelName: model.upstreamModel,
      body,
      extraHeaders: {
        "http-referer": process.env.SEENLIFE_SITE_URL || "https://seenlife.us",
        "x-title": "Seenlife API"
      }
    });
    data.seenlifeUpstreamProvider = "fallback";
    return data;
  }
}

async function callOpenAiCompatibleGateway({ url, apiKey, apiKeyName, modelName, body, extraHeaders = {} }) {
  if (!apiKey) throw httpError(`${apiKeyName} is not configured`, 500);

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`,
      ...extraHeaders
    },
    body: JSON.stringify({ ...body, model: modelName, stream: false })
  });

  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { error: { message: text || "Invalid upstream response" } };
  }

  if (!response.ok) {
    throw httpError(data?.error?.message || "Upstream model gateway request failed", response.status);
  }

  return data;
}

function verifyShopifyWebhook(req, rawBody) {
  const secret = process.env.SHOPIFY_WEBHOOK_SECRET;
  if (!secret) throw httpError("SHOPIFY_WEBHOOK_SECRET is not configured", 500);

  const received = String(req.headers["x-shopify-hmac-sha256"] || "");
  if (!received) throw httpError("Missing Shopify webhook signature", 401);

  const expected = createHmac("sha256", secret).update(rawBody).digest("base64");
  const receivedBuffer = Buffer.from(received, "utf8");
  const expectedBuffer = Buffer.from(expected, "utf8");
  if (receivedBuffer.length !== expectedBuffer.length || !timingSafeEqual(receivedBuffer, expectedBuffer)) {
    throw httpError("Invalid Shopify webhook signature", 401);
  }
}

function parseShopifyTopup(order) {
  const email = normalizeEmail(order.email || order.contact_email || order.customer?.email);
  if (!email) throw httpError("Shopify order does not include a customer email", 400);

  const lineItems = Array.isArray(order.line_items) ? order.line_items : [];
  let amountMicroUsd = 0;
  const matchedItems = [];

  for (const item of lineItems) {
    const title = String(item.title || item.name || "");
    const sku = String(item.sku || "");
    const quantity = Math.max(1, Number(item.quantity || 1));
    const price = Number(item.price || item.pre_tax_price || 0);
    const isSeenlifeCredit = SHOPIFY_CREDIT_PRODUCT_RE.test(title) || SHOPIFY_CREDIT_PRODUCT_RE.test(sku);
    if (!isSeenlifeCredit || price <= 0) continue;

    amountMicroUsd += usdToMicro(price * quantity);
    matchedItems.push({ title, sku, quantity, price });
  }

  return {
    email,
    name: customerName(order),
    amountMicroUsd,
    orderName: String(order.name || order.order_number || order.id || "Shopify order"),
    orderReference: `shopify:${order.id || order.admin_graphql_api_id || order.name || randomUUID()}`,
    lineItems: matchedItems
  };
}

function customerName(order) {
  const direct = order.customer
    ? `${order.customer.first_name || ""} ${order.customer.last_name || ""}`.trim()
    : "";
  return direct || String(order.billing_address?.name || order.shipping_address?.name || "").trim();
}

async function buildManualTopupEmailResult(body) {
  const amountMicroUsd = usdToMicro(body.amountUsd);
  const orderName = String(body.orderName || body.orderId || "Manual resend").trim();
  if (amountMicroUsd <= 0) throw httpError("amountUsd must be greater than 0", 400);

  const db = await readDb();
  const user = findUserByEmail(db, body.email);
  if (!user) throw httpError("User not found. Create the user first.", 404);

  return {
    duplicate: false,
    user: publicUser(user),
    orderName,
    plainApiKey: "",
    hasExistingApiKey: db.apiKeys.some((key) => key.userId === user.id && !key.revokedAt),
    topupAmountUsd: formatUsd(amountMicroUsd),
    lineItems: []
  };
}

async function sendTopupEmail(result) {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) {
    console.warn("RESEND_API_KEY is not configured; skipping top-up email.");
    return;
  }

  const from = process.env.SEENLIFE_EMAIL_FROM || "Seenlife <support@seenlife.us>";
  const dashboardUrl = process.env.SEENLIFE_DASHBOARD_URL || "https://seenlife.us/pages/account-dashboard";
  const docsUrl = process.env.SEENLIFE_DOCS_URL || "https://seenlife.us/pages/api-docs";
  const apiUrl = process.env.SEENLIFE_API_URL || "https://seenlife-api-production.up.railway.app/v1/chat/completions";
  const subject = result.duplicate
    ? `Seenlife order ${result.orderName} was already processed`
    : "Your Seenlife API credits are ready";

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      "authorization": `Bearer ${apiKey}`,
      "content-type": "application/json"
    },
    body: JSON.stringify({
      from,
      to: [result.user.email],
      subject,
      text: topupEmailText(result, { dashboardUrl, docsUrl, apiUrl }),
      html: topupEmailHtml(result, { dashboardUrl, docsUrl, apiUrl })
    })
  });

  const text = await response.text();
  if (!response.ok) {
    console.error("Resend email failed:", text);
    throw httpError("Top-up succeeded, but email delivery failed", 502, "email_failed");
  }
}

function topupEmailText(result, urls) {
  const apiKeyBlock = result.plainApiKey
    ? `Your Seenlife API Key:\n${result.plainApiKey}\n`
    : "Your existing Seenlife API Key is still active. For security, we do not resend existing keys by email.\n";

  return `Hi ${result.user.name || result.user.email},

Your Seenlife API balance has been updated.

Recharge amount: $${Number(result.topupAmountUsd).toFixed(2)}
Current balance: $${Number(result.user.balanceUsd).toFixed(6)}
Shopify order: ${result.orderName}

${apiKeyBlock}
API endpoint:
${urls.apiUrl}

Manage your balance and API access:
${urls.dashboardUrl}

API docs:
${urls.docsUrl}

Thank you,
Seenlife`;
}

function topupEmailHtml(result, urls) {
  const apiKeyBlock = result.plainApiKey
    ? `<p style="margin:16px 0 6px;font-weight:700">Your Seenlife API Key</p><div style="padding:12px;border:1px solid #abefc6;background:#ecfdf3;border-radius:8px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;word-break:break-all">${escapeHtml(result.plainApiKey)}</div>`
    : `<p>Your existing Seenlife API Key is still active. For security, we do not resend existing keys by email.</p>`;

  return `<!doctype html>
<html>
<body style="margin:0;background:#f6f7f9;font-family:Inter,Arial,sans-serif;color:#111827">
  <div style="max-width:620px;margin:0 auto;padding:28px">
    <div style="background:white;border:1px solid #e5e7eb;border-radius:10px;padding:24px">
      <h1 style="font-size:22px;margin:0 0 16px">Your Seenlife API credits are ready</h1>
      <p>Hi ${escapeHtml(result.user.name || result.user.email)},</p>
      <p>Your Seenlife API balance has been updated.</p>
      <table style="width:100%;border-collapse:collapse;margin:18px 0">
        <tr><td style="padding:8px 0;color:#667085">Recharge amount</td><td style="padding:8px 0;text-align:right;font-weight:700">$${Number(result.topupAmountUsd).toFixed(2)}</td></tr>
        <tr><td style="padding:8px 0;color:#667085">Current balance</td><td style="padding:8px 0;text-align:right;font-weight:700">$${Number(result.user.balanceUsd).toFixed(6)}</td></tr>
        <tr><td style="padding:8px 0;color:#667085">Shopify order</td><td style="padding:8px 0;text-align:right;font-weight:700">${escapeHtml(result.orderName)}</td></tr>
      </table>
      ${apiKeyBlock}
      <p style="margin-top:22px">API endpoint:<br><a href="${escapeHtml(urls.apiUrl)}">${escapeHtml(urls.apiUrl)}</a></p>
      <p><a href="${escapeHtml(urls.dashboardUrl)}" style="display:inline-block;background:#0f766e;color:white;text-decoration:none;padding:11px 16px;border-radius:7px;font-weight:700">Open Seenlife dashboard</a></p>
      <p>API docs: <a href="${escapeHtml(urls.docsUrl)}">${escapeHtml(urls.docsUrl)}</a></p>
      <p style="color:#667085;margin-top:24px">Thank you,<br>Seenlife</p>
    </div>
  </div>
</body>
</html>`;
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

async function readRawBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return Buffer.concat(chunks);
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

function sendHtml(res, statusCode, html) {
  res.writeHead(statusCode, {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store"
  });
  res.end(html);
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

function normalizeBillableUsage(usage) {
  if (!usage || typeof usage !== "object") return null;

  const hasInputTokens = usage.prompt_tokens != null || usage.input_tokens != null;
  const hasOutputTokens = usage.completion_tokens != null || usage.output_tokens != null;
  if (!hasInputTokens || !hasOutputTokens) return null;

  const inputTokens = Number(usage.prompt_tokens ?? usage.input_tokens);
  const outputTokens = Number(usage.completion_tokens ?? usage.output_tokens);
  const reportedTotalTokens = Number(usage.total_tokens ?? inputTokens + outputTokens);
  if (!Number.isFinite(inputTokens) || !Number.isFinite(outputTokens) || !Number.isFinite(reportedTotalTokens)) return null;
  if (inputTokens < 0 || outputTokens < 0 || reportedTotalTokens < 0) return null;
  if (inputTokens + outputTokens <= 0) return null;

  const totalTokens = Math.max(reportedTotalTokens, inputTokens + outputTokens);
  return {
    prompt_tokens: inputTokens,
    completion_tokens: outputTokens,
    total_tokens: totalTokens
  };
}

function calculateUsageCost(model, usage) {
  const inputTokens = Number(usage.prompt_tokens || usage.input_tokens || 0);
  const outputTokens = Number(usage.completion_tokens || usage.output_tokens || 0);
  const totalTokens = Number(usage.total_tokens || inputTokens + outputTokens);
  const inputMicroUsd = Math.ceil((inputTokens * model.inputUsdPerMillion * 1_000_000) / 1_000_000);
  const outputMicroUsd = Math.ceil((outputTokens * model.outputUsdPerMillion * 1_000_000) / 1_000_000);
  const upstreamInputMicroUsd = Math.ceil((inputTokens * model.upstreamInputUsdPerMillion * 1_000_000) / 1_000_000);
  const upstreamOutputMicroUsd = Math.ceil((outputTokens * model.upstreamOutputUsdPerMillion * 1_000_000) / 1_000_000);
  const totalMicroUsd = inputMicroUsd + outputMicroUsd;
  const upstreamMicroUsd = upstreamInputMicroUsd + upstreamOutputMicroUsd;
  return {
    inputTokens,
    outputTokens,
    totalTokens,
    upstreamMicroUsd,
    totalMicroUsd,
    grossProfitMicroUsd: Math.max(0, totalMicroUsd - upstreamMicroUsd)
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

function escapeHtml(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;"
  }[ch]));
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

function landingPage() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Seenlife API</title>
  <style>
    body{margin:0;font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f6f7f9;color:#111827}
    main{max-width:760px;margin:12vh auto;padding:0 24px}
    h1{font-size:40px;line-height:1.1;margin:0 0 14px}
    p{font-size:17px;line-height:1.6;color:#4b5563}
    a{color:#0f766e;text-decoration:none;font-weight:700}
    .box{background:white;border:1px solid #e5e7eb;border-radius:8px;padding:24px;box-shadow:0 12px 30px rgba(15,23,42,.06)}
    code{background:#eef2f7;border-radius:6px;padding:3px 6px}
  </style>
</head>
<body>
  <main>
    <div class="box">
      <h1>Seenlife API is running</h1>
      <p>This is the API backend for Seenlife model access, balances, API keys, and unified model routing.</p>
      <p>Health check: <a href="/health">/health</a></p>
      <p>Admin panel: <a href="/admin">/admin</a></p>
      <p>OpenAI-compatible endpoint: <code>/v1/chat/completions</code></p>
    </div>
  </main>
</body>
</html>`;
}

function adminPage() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Seenlife Admin</title>
  <style>
    :root{color-scheme:light;--bg:#f5f7fb;--panel:#fff;--line:#dfe5ee;--text:#101828;--muted:#667085;--accent:#0f766e;--bad:#b42318}
    *{box-sizing:border-box}
    body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
    header{height:64px;display:flex;align-items:center;justify-content:space-between;padding:0 28px;background:#111827;color:white}
    header h1{font-size:18px;margin:0;font-weight:700}
    header span{font-size:13px;color:#cbd5e1}
    main{max-width:1180px;margin:0 auto;padding:26px}
    .grid{display:grid;grid-template-columns:1.1fr .9fr;gap:18px}
    .panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px}
    .panel h2{font-size:16px;margin:0 0 14px}
    label{display:block;font-size:12px;font-weight:700;color:#475467;margin:12px 0 6px}
    input,select,textarea{width:100%;height:40px;border:1px solid #cfd6e2;border-radius:7px;padding:0 11px;font:inherit;background:white}
    textarea{height:94px;padding:10px;resize:vertical}
    button{height:38px;border:0;border-radius:7px;background:var(--accent);color:white;font-weight:700;padding:0 14px;cursor:pointer}
    button.secondary{background:#344054}
    button.ghost{background:#eef2f6;color:#1f2937}
    button:disabled{opacity:.55;cursor:not-allowed}
    .row{display:flex;gap:10px;align-items:end}
    .row>*{flex:1}
    .actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}
    .status{margin:14px 0 0;font-size:13px;color:var(--muted);min-height:20px}
    .error{color:var(--bad)}
    .ok{color:var(--accent)}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th,td{text-align:left;border-bottom:1px solid #eef2f6;padding:10px 8px;vertical-align:top}
    th{color:#475467;font-size:12px}
    code{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;background:#f1f5f9;border-radius:6px;padding:2px 5px}
    .secret{word-break:break-all;background:#ecfdf3;border:1px solid #abefc6;border-radius:8px;padding:10px;margin-top:10px}
    .stack{display:grid;gap:18px}
    @media(max-width:860px){.grid{grid-template-columns:1fr}.row{display:block}.row>*{margin-bottom:10px}main{padding:16px}}
  </style>
</head>
<body>
  <header>
    <h1>Seenlife Admin</h1>
    <span>Balance, API keys, and usage</span>
  </header>
  <main>
    <div class="grid">
      <section class="stack">
        <div class="panel">
          <h2>Admin Login</h2>
          <label for="token">Admin token</label>
          <input id="token" type="password" autocomplete="current-password" placeholder="Paste SEENLIFE_ADMIN_TOKEN">
          <div class="actions">
            <button onclick="saveToken()">Save token</button>
            <button class="ghost" onclick="clearToken()">Clear</button>
            <button class="secondary" onclick="refreshAll()">Refresh data</button>
          </div>
          <div id="loginStatus" class="status"></div>
        </div>

        <div class="panel">
          <h2>Create Customer</h2>
          <div class="row">
            <div>
              <label for="userEmail">Email</label>
              <input id="userEmail" placeholder="customer@example.com">
            </div>
            <div>
              <label for="userName">Name</label>
              <input id="userName" placeholder="Customer name">
            </div>
          </div>
          <div class="actions">
            <button onclick="createUser()">Create or find user</button>
          </div>
        </div>

        <div class="panel">
          <h2>Add Balance</h2>
          <div class="row">
            <div>
              <label for="topupEmail">Email</label>
              <input id="topupEmail" placeholder="customer@example.com">
            </div>
            <div>
              <label for="topupAmount">Amount USD</label>
              <select id="topupAmount">
                <option value="10">$10</option>
                <option value="50">$50</option>
                <option value="100">$100</option>
              </select>
            </div>
            <div>
              <label for="orderId">Order ID</label>
              <input id="orderId" placeholder="Shopify order">
            </div>
          </div>
          <div class="actions">
            <button onclick="topup()">Add balance</button>
          </div>
        </div>

        <div class="panel">
          <h2>Create Seenlife API Key</h2>
          <div class="row">
            <div>
              <label for="keyEmail">Email</label>
              <input id="keyEmail" placeholder="customer@example.com">
            </div>
            <div>
              <label for="keyName">Key name</label>
              <input id="keyName" value="Main key">
            </div>
          </div>
          <div class="actions">
            <button onclick="createApiKey()">Generate API key</button>
          </div>
          <div id="newKey"></div>
        </div>

        <div class="panel">
          <h2>Resend Top-up Email</h2>
          <div class="row">
            <div>
              <label for="emailResendEmail">Email</label>
              <input id="emailResendEmail" placeholder="customer@example.com">
            </div>
            <div>
              <label for="emailResendAmount">Amount USD</label>
              <select id="emailResendAmount">
                <option value="10">$10</option>
                <option value="50">$50</option>
                <option value="100">$100</option>
              </select>
            </div>
            <div>
              <label for="emailResendOrder">Order</label>
              <input id="emailResendOrder" placeholder="#1448">
            </div>
          </div>
          <div class="actions">
            <button onclick="resendTopupEmail()">Send email</button>
          </div>
        </div>
      </section>

      <section class="stack">
        <div class="panel">
          <h2>Users</h2>
          <div id="users"></div>
        </div>
        <div class="panel">
          <h2>Recent Usage</h2>
          <div id="usage"></div>
        </div>
      </section>
    </div>
    <div id="status" class="status"></div>
  </main>
  <script>
    const tokenInput = document.getElementById('token');
    tokenInput.value = localStorage.getItem('seenlifeAdminToken') || '';
    function token(){ return tokenInput.value.trim(); }
    function setStatus(message, bad){
      const el = document.getElementById('status');
      el.textContent = message || '';
      el.className = 'status ' + (bad ? 'error' : 'ok');
    }
    function saveToken(){
      localStorage.setItem('seenlifeAdminToken', token());
      document.getElementById('loginStatus').textContent = 'Token saved in this browser.';
      refreshAll();
    }
    function clearToken(){
      localStorage.removeItem('seenlifeAdminToken');
      tokenInput.value = '';
      document.getElementById('loginStatus').textContent = 'Token cleared.';
    }
    async function api(path, options){
      const res = await fetch(path, Object.assign({
        headers: {
          'content-type': 'application/json',
          'authorization': 'Bearer ' + token()
        }
      }, options || {}));
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error((data.error && data.error.message) || 'Request failed');
      return data;
    }
    async function createUser(){
      try {
        const email = document.getElementById('userEmail').value;
        const name = document.getElementById('userName').value;
        await api('/admin/users', { method:'POST', body: JSON.stringify({ email, name }) });
        copyCommonEmails(email);
        setStatus('Customer is ready.');
        refreshAll();
      } catch (error) { setStatus(error.message, true); }
    }
    async function topup(){
      try {
        const email = document.getElementById('topupEmail').value;
        const amountUsd = Number(document.getElementById('topupAmount').value);
        const orderId = document.getElementById('orderId').value;
        await api('/admin/topups', { method:'POST', body: JSON.stringify({ email, amountUsd, orderId }) });
        setStatus('Balance added.');
        refreshAll();
      } catch (error) { setStatus(error.message, true); }
    }
    async function createApiKey(){
      try {
        const email = document.getElementById('keyEmail').value;
        const name = document.getElementById('keyName').value;
        const data = await api('/admin/api-keys', { method:'POST', body: JSON.stringify({ email, name }) });
        document.getElementById('newKey').innerHTML = '<div class="secret"><strong>Copy this key now:</strong><br><code>' + escapeHtml(data.apiKey.key) + '</code></div>';
        setStatus('API key generated. Copy it now; it will not be shown again.');
        refreshAll();
      } catch (error) { setStatus(error.message, true); }
    }
    async function resendTopupEmail(){
      try {
        const email = document.getElementById('emailResendEmail').value;
        const amountUsd = Number(document.getElementById('emailResendAmount').value);
        const orderName = document.getElementById('emailResendOrder').value;
        await api('/admin/resend-topup-email', { method:'POST', body: JSON.stringify({ email, amountUsd, orderName }) });
        setStatus('Top-up email sent.');
      } catch (error) { setStatus(error.message, true); }
    }
    async function refreshAll(){
      if (!token()) return;
      await Promise.allSettled([loadUsers(), loadUsage()]);
    }
    async function loadUsers(){
      const data = await api('/admin/users');
      const users = data.users || [];
      document.getElementById('users').innerHTML = users.length ? '<table><thead><tr><th>Email</th><th>Name</th><th>Balance</th></tr></thead><tbody>' + users.map(user => '<tr><td>' + escapeHtml(user.email) + '</td><td>' + escapeHtml(user.name || '') + '</td><td>$' + Number(user.balanceUsd).toFixed(6) + '</td></tr>').join('') + '</tbody></table>' : '<p class="status">No users yet.</p>';
    }
    async function loadUsage(){
      const data = await api('/admin/usage');
      const logs = data.usageLogs || [];
      document.getElementById('usage').innerHTML = logs.length ? '<table><thead><tr><th>Model</th><th>Tokens</th><th>Charged</th><th>Time</th></tr></thead><tbody>' + logs.slice(0,25).map(log => '<tr><td>' + escapeHtml(log.model) + '</td><td>' + Number(log.totalTokens || 0) + '</td><td>$' + (Number(log.chargedMicroUsd || 0) / 1000000).toFixed(6) + '</td><td>' + escapeHtml(log.createdAt || '') + '</td></tr>').join('') + '</tbody></table>' : '<p class="status">No usage yet.</p>';
    }
    function copyCommonEmails(email){
      ['topupEmail','keyEmail','emailResendEmail'].forEach(id => { if (!document.getElementById(id).value) document.getElementById(id).value = email; });
    }
    function escapeHtml(value){
      return String(value == null ? '' : value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[ch]));
    }
    refreshAll();
  </script>
</body>
</html>`;
}
