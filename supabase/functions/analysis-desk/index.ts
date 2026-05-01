import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json }
  | Json[];

type DashboardRecord = Record<string, number | null>;
type DashboardDataset = {
  meta?: Record<string, Json>;
  neighborhoods?: Record<string, Record<string, DashboardRecord>>;
  benchmarks?: Record<string, Record<string, DashboardRecord>>;
};

type AnalysisRequest = {
  action?: "bootstrap" | "chat" | "history";
  sessionId?: string | null;
  profileSlug?: string | null;
  prompt?: string | null;
  templateId?: string | null;
  pilotSlug?: string | null;
  scope?: Record<string, Json> | null;
};

type ToolResult = {
  name: string;
  summary: string;
  data: Json;
};

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
const supabaseAnonKey =
  Deno.env.get("SB_PUBLISHABLE_KEY") ?? Deno.env.get("SUPABASE_ANON_KEY")!;
const supabaseServiceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const openAiApiKey = Deno.env.get("OPENAI_API_KEY");
const openAiModel = Deno.env.get("OPENAI_MODEL") ?? "gpt-4o";
const dashboardDataUrl =
  Deno.env.get("DASHBOARD_DATA_URL") ??
  "https://romulloataides.github.io/BALTI2/data.json";
const maxHistoryMessages = 10;
const maxToolRounds = 4;

const authClient = createClient(supabaseUrl, supabaseAnonKey);
const adminClient = createClient(supabaseUrl, supabaseServiceRoleKey);

let dashboardCache: DashboardDataset | null = null;
let dashboardCacheFetchedAt = 0;

function jsonResponse(body: Json, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json",
    },
  });
}

function stringValue(value: unknown, fallback = "") {
  return typeof value === "string" ? value : fallback;
}

function arrayOfStrings(value: unknown) {
  return Array.isArray(value)
    ? value
        .map((entry) => stringValue(entry).trim())
        .filter(Boolean)
    : [];
}

function safeJsonParse(value: string) {
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch (_error) {
    return {};
  }
}

function truncate(value: string, max = 220) {
  if (value.length <= max) return value;
  return `${value.slice(0, max - 1)}…`;
}

function normalizeNeighborhoodLookup(dataset: DashboardDataset) {
  const keys = Object.keys(dataset.neighborhoods ?? {});
  const map = new Map<string, string>();
  for (const key of keys) {
    map.set(key.toLowerCase(), key);
    map.set(
      key.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim(),
      key,
    );
  }
  return { keys, map };
}

function normalizeNeighborhoods(
  dataset: DashboardDataset,
  requested: string[],
  scopeNeighborhoods: string[],
) {
  const { keys, map } = normalizeNeighborhoodLookup(dataset);
  const raw = requested.length ? requested : scopeNeighborhoods;
  if (!raw.length) return keys.slice(0, 8);
  const seen = new Set<string>();
  const resolved: string[] = [];
  for (const item of raw) {
    const normalized =
      map.get(item.toLowerCase()) ??
      map.get(item.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim());
    if (normalized && !seen.has(normalized)) {
      seen.add(normalized);
      resolved.push(normalized);
    }
  }
  return resolved.slice(0, 8);
}

function yearKeys(dataset: DashboardDataset) {
  const years = dataset.meta?.years;
  if (Array.isArray(years)) {
    return years
      .map((year) => Number(year))
      .filter((year) => Number.isFinite(year))
      .sort((a, b) => a - b);
  }
  return [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023];
}

async function loadDashboardData() {
  const now = Date.now();
  if (dashboardCache && now - dashboardCacheFetchedAt < 5 * 60 * 1000) {
    return dashboardCache;
  }
  const response = await fetch(dashboardDataUrl, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(
      `Dashboard data fetch failed with status ${response.status}.`,
    );
  }
  dashboardCache = (await response.json()) as DashboardDataset;
  dashboardCacheFetchedAt = now;
  return dashboardCache;
}

async function authenticateAdmin(req: Request) {
  const authHeader =
    req.headers.get("Authorization") ?? req.headers.get("authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    return { error: jsonResponse({ error: "Missing Bearer token." }, 401) };
  }
  const token = authHeader.replace("Bearer ", "");
  const { data, error } = await authClient.auth.getClaims(token);
  const email = stringValue(data?.claims?.email).toLowerCase();
  const userId = stringValue(data?.claims?.sub);
  if (error || !email || !userId) {
    return { error: jsonResponse({ error: "Invalid Supabase session." }, 401) };
  }
  const { data: adminRow, error: adminError } = await adminClient
    .from("admin_users")
    .select("email, role")
    .ilike("email", email)
    .maybeSingle();
  if (adminError) {
    return {
      error: jsonResponse(
        { error: "Admin allowlist lookup failed.", detail: adminError.message },
        500,
      ),
    };
  }
  if (!adminRow) {
    return {
      error: jsonResponse(
        {
          error: "Signed in successfully, but this email is not allowlisted for admin access yet.",
          email,
        },
        403,
      ),
    };
  }
  return { email, userId, role: adminRow.role as string };
}

async function fetchPromptProfiles() {
  const { data, error } = await adminClient
    .from("analysis_prompt_profiles")
    .select("slug, label, context_note")
    .order("slug");
  if (error) throw error;
  return data ?? [];
}

async function fetchRecentSessions(email: string) {
  const { data, error } = await adminClient
    .from("analysis_sessions")
    .select("id, title, profile_slug, pilot_slug, status, scope, updated_at")
    .ilike("admin_email", email)
    .order("updated_at", { ascending: false })
    .limit(12);
  if (error) throw error;
  return data ?? [];
}

async function fetchSession(email: string, sessionId: string) {
  const { data, error } = await adminClient
    .from("analysis_sessions")
    .select("*")
    .eq("id", sessionId)
    .ilike("admin_email", email)
    .maybeSingle();
  if (error) throw error;
  return data;
}

async function fetchSessionMessages(sessionId: string) {
  const { data, error } = await adminClient
    .from("analysis_messages")
    .select("id, role, content, tool_name, tool_payload, created_at")
    .eq("session_id", sessionId)
    .order("created_at", { ascending: true });
  if (error) throw error;
  return data ?? [];
}

async function ensureSession(args: {
  email: string;
  userId: string;
  sessionId?: string | null;
  profileSlug: string;
  pilotSlug?: string | null;
  scope: Record<string, Json>;
  prompt: string;
}) {
  if (args.sessionId) {
    const existing = await fetchSession(args.email, args.sessionId);
    if (existing) return existing;
  }
  const { data, error } = await adminClient
    .from("analysis_sessions")
    .insert({
      user_id: args.userId,
      admin_email: args.email,
      profile_slug: args.profileSlug,
      pilot_slug: args.pilotSlug ?? null,
      title: truncate(args.prompt || "New analysis session", 72),
      scope: args.scope,
      status: "active",
    })
    .select("*")
    .single();
  if (error) throw error;
  return data;
}

async function storeMessage(args: {
  sessionId: string;
  role: "user" | "assistant" | "tool" | "system";
  content: string;
  toolName?: string | null;
  toolPayload?: Json;
}) {
  const { error } = await adminClient.from("analysis_messages").insert({
    session_id: args.sessionId,
    role: args.role,
    content: args.content,
    tool_name: args.toolName ?? null,
    tool_payload: args.toolPayload ?? null,
  });
  if (error) throw error;
}

async function touchSession(args: {
  sessionId: string;
  title?: string | null;
  profileSlug?: string | null;
  scope?: Json;
}) {
  const patch: Record<string, Json> = {
    updated_at: new Date().toISOString(),
  };
  if (args.title) patch.title = truncate(args.title, 72);
  if (args.profileSlug) patch.profile_slug = args.profileSlug;
  if (args.scope) patch.scope = args.scope;
  const { error } = await adminClient
    .from("analysis_sessions")
    .update(patch)
    .eq("id", args.sessionId);
  if (error) throw error;
}

function buildInstructions(args: {
  profilePrompt: string;
  profileLabel: string;
  scope: Record<string, Json>;
  templateId?: string | null;
}) {
  const neighborhoods = arrayOfStrings(args.scope.neighborhoods);
  const scopeBits = [
    neighborhoods.length
      ? `Neighborhood scope: ${neighborhoods.join(", ")}.`
      : "Neighborhood scope: all available neighborhoods.",
    args.scope.metric
      ? `Current dashboard metric: ${stringValue(args.scope.metricLabel, stringValue(args.scope.metric))}.`
      : "",
    args.scope.year ? `Current dashboard year: ${args.scope.year}.` : "",
    args.scope.benchmarkLevel
      ? `Benchmark level: ${args.scope.benchmarkLevel}.`
      : "",
    args.scope.pilotScopeLabel
      ? `Pilot scope label: ${stringValue(args.scope.pilotScopeLabel)}.`
      : "",
    args.scope.gapMode ? "Gap mode is active in the dashboard." : "",
    args.scope.blindSpotsOnly
      ? "Blind-spots-only filtering is active in the dashboard."
      : "",
    args.templateId ? `Requested template: ${args.templateId}.` : "",
  ].filter(Boolean);
  return [
    `Prompt profile: ${args.profileLabel}.`,
    args.profilePrompt,
    "Always use tools before making specific factual claims about neighborhood metrics, reports, 311 history, or spending.",
    "When a metric is proxy, derived, or scaffolded, say so explicitly.",
    "Prefer compact operational answers with short sections or bullets rather than long essays.",
    scopeBits.join(" "),
  ].join("\n\n");
}

function extractYears(
  dataset: DashboardDataset,
  startYear?: number | null,
  endYear?: number | null,
) {
  const years = yearKeys(dataset);
  const start = Number.isFinite(startYear) ? Number(startYear) : years[0];
  const end = Number.isFinite(endYear) ? Number(endYear) : years[years.length - 1];
  return years.filter((year) => year >= start && year <= end);
}

function pickMetrics(
  requested: unknown,
  fallback: string[],
  allowlist: string[],
) {
  const raw = arrayOfStrings(requested).map((entry) => entry.toLowerCase());
  const selected = (raw.length ? raw : fallback).filter((metric) =>
    allowlist.includes(metric)
  );
  return [...new Set(selected)].slice(0, 12);
}

async function queryNeighborhoodData(args: Record<string, unknown>, scope: Record<string, Json>): Promise<ToolResult> {
  const dataset = await loadDashboardData();
  const neighborhoods = normalizeNeighborhoods(
    dataset,
    arrayOfStrings(args.neighborhoods),
    arrayOfStrings(scope.neighborhoods),
  );
  const years = extractYears(
    dataset,
    Number(args.startYear),
    Number(args.endYear),
  );
  const metrics = pickMetrics(
    args.metrics,
    [stringValue(scope.metric, "hi")],
    ["hi", "le", "as", "la", "va", "pv", "un", "hs", "hz", "rt", "dp", "ws"],
  );
  const benchmarkLevel = stringValue(
    args.benchmarkLevel,
    stringValue(scope.benchmarkLevel, "city"),
  );
  const neighborhoodRows = neighborhoods.map((name) => ({
    name,
    yearly: Object.fromEntries(
      years.map((year) => {
        const record = dataset.neighborhoods?.[name]?.[String(year)] ?? {};
        return [
          String(year),
          Object.fromEntries(metrics.map((metric) => [metric, record?.[metric] ?? null])),
        ];
      }),
    ),
  }));
  const benchmarks =
    benchmarkLevel && benchmarkLevel !== "neighborhood"
      ? Object.fromEntries(
          years.map((year) => [
            String(year),
            Object.fromEntries(
              metrics.map((metric) => [
                metric,
                dataset.benchmarks?.[benchmarkLevel]?.[String(year)]?.[metric] ?? null,
              ]),
            ),
          ]),
        )
      : null;
  return {
    name: "query_neighborhood_data",
    summary: `Returned ${neighborhoodRows.length} neighborhood record(s) for ${metrics.join(", ")} across ${years[0]}-${years[years.length - 1]}.`,
    data: {
      source: "BALTI2 data.json",
      benchmark_level: benchmarkLevel,
      neighborhoods: neighborhoodRows,
      benchmarks,
      meta: dataset.meta ?? {},
    },
  };
}

async function query311History(args: Record<string, unknown>, scope: Record<string, Json>): Promise<ToolResult> {
  const dataset = await loadDashboardData();
  const neighborhoods = normalizeNeighborhoods(
    dataset,
    arrayOfStrings(args.neighborhoods),
    arrayOfStrings(scope.neighborhoods),
  );
  const years = extractYears(
    dataset,
    Number(args.startYear),
    Number(args.endYear),
  );
  const metrics = pickMetrics(args.metrics, ["hz", "dp", "rt", "ws"], [
    "hz",
    "dp",
    "rt",
    "ws",
  ]);
  const rows = neighborhoods.map((name) => ({
    name,
    yearly: Object.fromEntries(
      years.map((year) => {
        const record = dataset.neighborhoods?.[name]?.[String(year)] ?? {};
        return [
          String(year),
          Object.fromEntries(metrics.map((metric) => [metric, record?.[metric] ?? null])),
        ];
      }),
    ),
  }));
  return {
    name: "query_311_history",
    summary: `Returned 311 proxy series for ${rows.length} neighborhood(s) across ${years[0]}-${years[years.length - 1]}.`,
    data: {
      source: "BALTI2 data.json 311 proxy metrics",
      metrics,
      neighborhoods: rows,
    },
  };
}

async function queryReports(args: Record<string, unknown>, scope: Record<string, Json>): Promise<ToolResult> {
  const dataset = await loadDashboardData();
  const neighborhoods = normalizeNeighborhoods(
    dataset,
    arrayOfStrings(args.neighborhoods),
    arrayOfStrings(scope.neighborhoods),
  );
  const limit = Math.min(Math.max(Number(args.limit) || 20, 1), 50);
  let query = adminClient
    .from("report_vote_counts")
    .select(
      "id, tracking_id, nsa, category, description, severity, author_role, status, created_at, source, pilot_slug, block_label, observed_on, metadata, confirms, disputes",
    )
    .order("created_at", { ascending: false })
    .limit(limit);
  if (neighborhoods.length) query = query.in("nsa", neighborhoods);
  const statuses = arrayOfStrings(args.statuses);
  if (statuses.length) query = query.in("status", statuses);
  const categories = arrayOfStrings(args.categories);
  if (categories.length) query = query.in("category", categories);
  const { data, error } = await query;
  if (error) throw error;
  return {
    name: "query_reports",
    summary: `Returned ${(data ?? []).length} community report(s).`,
    data: {
      source: "Supabase report_vote_counts",
      reports: data ?? [],
    },
  };
}

async function queryCityExpenditures(args: Record<string, unknown>, scope: Record<string, Json>): Promise<ToolResult> {
  const dataset = await loadDashboardData();
  const neighborhoods = normalizeNeighborhoods(
    dataset,
    arrayOfStrings(args.neighborhoods),
    arrayOfStrings(scope.neighborhoods),
  );
  const limit = Math.min(Math.max(Number(args.limit) || 20, 1), 50);
  let query = adminClient
    .from("spending_events")
    .select("*")
    .not("source", "like", "phase7_seed_%")
    .order("created_at", { ascending: false })
    .limit(limit);
  if (neighborhoods.length) query = query.in("nsa", neighborhoods);
  const categories = arrayOfStrings(args.categories);
  if (categories.length) query = query.in("category", categories);
  const statuses = arrayOfStrings(args.statuses);
  if (statuses.length) query = query.in("status", statuses);
  const { data, error } = await query;
  if (error) throw error;
  return {
    name: "query_city_expenditures",
    summary: `Returned ${(data ?? []).length} spending record(s).`,
    data: {
      source: "Supabase spending_events (legacy phase7 demo sources excluded)",
      events: data ?? [],
    },
  };
}

async function queryPilotAccuracy(args: Record<string, unknown>, scope: Record<string, Json>): Promise<ToolResult> {
  const pilotSlug = stringValue(args.pilotSlug, stringValue(scope.pilotSlug));
  let query = adminClient
    .from("pilot_accuracy_vote_counts")
    .select("*")
    .order("updated_at", { ascending: false })
    .limit(20);
  if (pilotSlug) query = query.eq("pilot_slug", pilotSlug);
  const issues = arrayOfStrings(args.issueKeys);
  if (issues.length) query = query.in("issue_key", issues);
  const { data, error } = await query;
  if (error) throw error;
  return {
    name: "query_pilot_accuracy",
    summary: `Returned ${(data ?? []).length} pilot accuracy vote group(s).`,
    data: {
      source: "Supabase pilot_accuracy_vote_counts",
      votes: data ?? [],
    },
  };
}

function toolDefinitions() {
  return [
    {
      type: "function",
      name: "query_neighborhood_data",
      description:
        "Fetch neighborhood-level longitudinal dashboard metrics and optional benchmark series.",
      strict: true,
      parameters: {
        type: "object",
        properties: {
          neighborhoods: {
            type: "array",
            items: { type: "string" },
          },
          metrics: {
            type: "array",
            items: { type: "string" },
          },
          startYear: { type: "number" },
          endYear: { type: "number" },
          benchmarkLevel: { type: "string" },
        },
        additionalProperties: false,
      },
    },
    {
      type: "function",
      name: "query_reports",
      description:
        "Fetch community report records, validation counts, and pilot metadata from Supabase.",
      strict: true,
      parameters: {
        type: "object",
        properties: {
          neighborhoods: {
            type: "array",
            items: { type: "string" },
          },
          categories: {
            type: "array",
            items: { type: "string" },
          },
          statuses: {
            type: "array",
            items: { type: "string" },
          },
          limit: { type: "number" },
        },
        additionalProperties: false,
      },
    },
    {
      type: "function",
      name: "query_311_history",
      description:
        "Fetch the dashboard's 311 proxy time series for hazards, dumping, rats, and water/sewer signals.",
      strict: true,
      parameters: {
        type: "object",
        properties: {
          neighborhoods: {
            type: "array",
            items: { type: "string" },
          },
          metrics: {
            type: "array",
            items: { type: "string" },
          },
          startYear: { type: "number" },
          endYear: { type: "number" },
        },
        additionalProperties: false,
      },
    },
    {
      type: "function",
      name: "query_city_expenditures",
      description:
        "Fetch spending or intervention records that have been loaded into Supabase.",
      strict: true,
      parameters: {
        type: "object",
        properties: {
          neighborhoods: {
            type: "array",
            items: { type: "string" },
          },
          categories: {
            type: "array",
            items: { type: "string" },
          },
          statuses: {
            type: "array",
            items: { type: "string" },
          },
          limit: { type: "number" },
        },
        additionalProperties: false,
      },
    },
    {
      type: "function",
      name: "query_pilot_accuracy",
      description:
        "Fetch persistent pilot accuracy votes for issue cards such as dumping or broadband access.",
      strict: true,
      parameters: {
        type: "object",
        properties: {
          pilotSlug: { type: "string" },
          issueKeys: {
            type: "array",
            items: { type: "string" },
          },
        },
        additionalProperties: false,
      },
    },
  ];
}

async function executeTool(
  name: string,
  args: Record<string, unknown>,
  scope: Record<string, Json>,
) {
  if (name === "query_neighborhood_data") {
    return await queryNeighborhoodData(args, scope);
  }
  if (name === "query_reports") {
    return await queryReports(args, scope);
  }
  if (name === "query_311_history") {
    return await query311History(args, scope);
  }
  if (name === "query_city_expenditures") {
    return await queryCityExpenditures(args, scope);
  }
  if (name === "query_pilot_accuracy") {
    return await queryPilotAccuracy(args, scope);
  }
  throw new Error(`Unknown tool requested: ${name}`);
}

async function openAiResponsesRequest(body: Record<string, unknown>) {
  if (!openAiApiKey) {
    throw new Error("OPENAI_API_KEY is not configured in Supabase secrets.");
  }
  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${openAiApiKey}`,
    },
    body: JSON.stringify(body),
  });
  const json = (await response.json()) as Record<string, unknown>;
  if (!response.ok) {
    throw new Error(
      `OpenAI Responses API error (${response.status}): ${JSON.stringify(json)}`,
    );
  }
  return json;
}

function conversationInputFromMessages(
  messages: Array<{ role: string; content: string }>,
) {
  return messages
    .filter((message) => message.role === "user" || message.role === "assistant")
    .map((message) => ({
      role: message.role,
      content: message.content,
    }));
}

async function runAnalysisTurn(args: {
  prompt: string;
  profile: { slug: string; label: string; system_prompt: string; context_note: string | null };
  history: Array<{ role: string; content: string }>;
  scope: Record<string, Json>;
  templateId?: string | null;
}) {
  const input: Record<string, unknown>[] = [
    ...conversationInputFromMessages(args.history),
    { role: "user", content: args.prompt },
  ];
  const tools = toolDefinitions();
  let response = await openAiResponsesRequest({
    model: openAiModel,
    instructions: buildInstructions({
      profilePrompt: args.profile.system_prompt,
      profileLabel: args.profile.label,
      scope: args.scope,
      templateId: args.templateId,
    }),
    tools,
    input,
    store: false,
  });

  const toolEvents: ToolResult[] = [];

  for (let round = 0; round < maxToolRounds; round += 1) {
    const output = Array.isArray(response.output)
      ? (response.output as Array<Record<string, unknown>>)
      : [];
    input.push(...output);
    const toolCalls = output.filter((item) => item.type === "function_call");
    if (!toolCalls.length) break;

    for (const call of toolCalls) {
      const toolName = stringValue(call.name);
      const toolArgs = safeJsonParse(stringValue(call.arguments, "{}"));
      const toolResult = await executeTool(toolName, toolArgs, args.scope);
      toolEvents.push(toolResult);
      input.push({
        type: "function_call_output",
        call_id: stringValue(call.call_id),
        output: JSON.stringify(toolResult.data),
      });
    }

    response = await openAiResponsesRequest({
      model: openAiModel,
      instructions: buildInstructions({
        profilePrompt: args.profile.system_prompt,
        profileLabel: args.profile.label,
        scope: args.scope,
        templateId: args.templateId,
      }),
      tools,
      input,
      store: false,
    });
  }

  const reply = stringValue(response.output_text).trim();
  if (!reply) {
    throw new Error("The model returned an empty response.");
  }
  return { reply, toolEvents };
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (req.method !== "POST") {
    return jsonResponse({ error: "Use POST." }, 405);
  }

  try {
    const auth = await authenticateAdmin(req);
    if ("error" in auth) return auth.error;

    const payload = (await req.json()) as AnalysisRequest;
    const action = payload.action ?? "bootstrap";

    if (action === "bootstrap") {
      const [profiles, sessions] = await Promise.all([
        fetchPromptProfiles(),
        fetchRecentSessions(auth.email),
      ]);
      return jsonResponse({
        authorized: true,
        email: auth.email,
        role: auth.role,
        model: openAiModel,
        profiles,
        sessions,
      });
    }

    if (action === "history") {
      const sessionId = stringValue(payload.sessionId);
      if (!sessionId) {
        return jsonResponse({ error: "sessionId is required for history." }, 400);
      }
      const session = await fetchSession(auth.email, sessionId);
      if (!session) {
        return jsonResponse({ error: "Session not found." }, 404);
      }
      const messages = await fetchSessionMessages(sessionId);
      return jsonResponse({
        authorized: true,
        session,
        messages,
      });
    }

    if (action === "chat") {
      const prompt = stringValue(payload.prompt).trim();
      if (!prompt) {
        return jsonResponse({ error: "prompt is required for chat." }, 400);
      }

      const { data: profileRows, error: profileError } = await adminClient
        .from("analysis_prompt_profiles")
        .select("slug, label, system_prompt, context_note")
        .order("slug");
      if (profileError) throw profileError;
      const profileMap = new Map(
        (profileRows ?? []).map((row) => [row.slug as string, row]),
      );
      const requestedProfileSlug = stringValue(
        payload.profileSlug,
        payload.pilotSlug ? "pilot" : "default",
      );
      const profile =
        profileMap.get(requestedProfileSlug) ?? profileMap.get("default");
      if (!profile) {
        throw new Error("No analysis prompt profiles are available.");
      }

      const scope =
        payload.scope && typeof payload.scope === "object"
          ? (payload.scope as Record<string, Json>)
          : {};

      const session = await ensureSession({
        email: auth.email,
        userId: auth.userId,
        sessionId: payload.sessionId ?? null,
        profileSlug: profile.slug as string,
        pilotSlug: (payload.pilotSlug ?? stringValue(scope.pilotSlug)) || null,
        scope,
        prompt,
      });

      await storeMessage({
        sessionId: session.id as string,
        role: "user",
        content: prompt,
      });

      const existingMessages = await fetchSessionMessages(session.id as string);
      const history = existingMessages
        .filter((message) => message.role === "user" || message.role === "assistant")
        .slice(-maxHistoryMessages)
        .map((message) => ({
          role: stringValue(message.role),
          content: stringValue(message.content),
        }));

      const { reply, toolEvents } = await runAnalysisTurn({
        prompt,
        profile: {
          slug: profile.slug as string,
          label: profile.label as string,
          system_prompt: profile.system_prompt as string,
          context_note: (profile.context_note as string | null) ?? null,
        },
        history,
        scope,
        templateId: payload.templateId ?? null,
      });

      for (const event of toolEvents) {
        await storeMessage({
          sessionId: session.id as string,
          role: "tool",
          content: event.summary,
          toolName: event.name,
          toolPayload: event.data,
        });
      }

      await storeMessage({
        sessionId: session.id as string,
        role: "assistant",
        content: reply,
      });

      await touchSession({
        sessionId: session.id as string,
        title:
          stringValue(session.title) === "New analysis session"
            ? prompt
            : undefined,
        profileSlug: profile.slug as string,
        scope,
      });

      const updatedSession = await fetchSession(auth.email, session.id as string);
      const messages = await fetchSessionMessages(session.id as string);
      return jsonResponse({
        authorized: true,
        session: updatedSession,
        messages,
        toolEvents,
        model: openAiModel,
      });
    }

    return jsonResponse({ error: `Unknown action: ${action}` }, 400);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown analysis error.";
    return jsonResponse({ error: message }, 500);
  }
});
