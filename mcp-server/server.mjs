import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

const BASE_URL = process.env.MEDICI_MONITOR_URL || "http://localhost:5000";

async function apiFetch(path) {
  const resp = await fetch(`${BASE_URL}${path}`);
  if (!resp.ok) throw new Error(`API ${path} returned ${resp.status}`);
  return resp.json();
}

const server = new McpServer({
  name: "medici-monitor",
  version: "1.0.0",
});

// ── System Status ──

server.tool(
  "medici_status",
  "Get full system status: DB health, active bookings, stuck cancellations, room waste, conversion, price drift, and more",
  {},
  async () => {
    const data = await apiFetch("/api/status");
    const summary = {
      dbConnected: data.dbConnected,
      totalActiveBookings: data.totalActiveBookings,
      stuckCancellations: data.stuckCancellations,
      futureBookings: data.futureBookings,
      upcomingCancellations: data.upcomingCancellations,
      reservationsToday: data.reservationsToday,
      reservationsThisWeek: data.reservationsThisWeek,
      wasteRoomsTotal: data.wasteRoomsTotal,
      wasteTotalValue: data.wasteTotalValue,
      totalBought: data.totalBought,
      totalSold: data.totalSold,
      profitLoss: data.profitLoss,
      buyRoomsHealthy: data.buyRoomsHealthy,
      timestamp: data.timestamp,
    };
    return { content: [{ type: "text", text: JSON.stringify(summary, null, 2) }] };
  }
);

// ── Active Alerts ──

server.tool(
  "medici_alerts",
  "Get all active monitoring alerts with severity, title, message, and category",
  {},
  async () => {
    const alerts = await apiFetch("/api/alerts");
    const summary = alerts.map((a) => ({
      id: a.id,
      severity: a.severity,
      title: a.title,
      message: a.message,
      category: a.category,
    }));
    return {
      content: [
        {
          type: "text",
          text: `${alerts.length} active alerts:\n\n${JSON.stringify(summary, null, 2)}`,
        },
      ],
    };
  }
);

// ── Booking Reconciliation ──

server.tool(
  "medici_reconciliation_status",
  "Get the latest booking reconciliation report — compares Medici DB vs Innstant API vs Zenith reservations",
  {},
  async () => {
    const data = await apiFetch("/api/reconciliation/status");
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

server.tool(
  "medici_reconciliation_run",
  "Run a booking reconciliation check now. Compares bookings across all systems and reports mismatches.",
  { hours: z.number().optional().describe("Lookback hours (default 24)") },
  async ({ hours }) => {
    const h = hours || 24;
    const data = await apiFetch(`/api/reconciliation/run?hours=${h}`);
    const summary = {
      mediciBookings: data.mediciBookingsCount,
      zenithReservations: data.mediciReservationsCount,
      salesOrders: data.salesOrdersCount,
      innstantVerified: data.innstantVerifiedCount,
      innstantMissing: data.innstantMissingCount,
      totalMismatches: data.totalMismatches,
      criticalMismatches: data.criticalMismatches,
      mismatches: data.mismatches?.slice(0, 10),
      durationMs: data.durationMs,
    };
    return { content: [{ type: "text", text: JSON.stringify(summary, null, 2) }] };
  }
);

// ── Circuit Breakers / Kill Switch ──

server.tool(
  "medici_breakers",
  "Get circuit breaker (kill switch) status — shows which breakers are open/closed",
  {},
  async () => {
    const data = await apiFetch("/api/failsafe/breakers");
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

server.tool(
  "medici_trip_breaker",
  "Trip (activate) a specific circuit breaker. Requires breaker name and PIN.",
  {
    breaker: z.string().describe("Breaker name: BUYING, SELLING, QUEUE, PUSH, or CANCELS"),
    pin: z.string().describe("Operator PIN for authorization"),
    reason: z.string().optional().describe("Reason for tripping"),
  },
  async ({ breaker, pin, reason }) => {
    const r = reason ? `&reason=${encodeURIComponent(reason)}` : "";
    const resp = await fetch(
      `${BASE_URL}/api/failsafe/breaker/${breaker}/trip?pin=${pin}${r}&actor=Claude-MCP`,
      { method: "POST" }
    );
    const data = await resp.json();
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

server.tool(
  "medici_reset_breaker",
  "Reset (deactivate) a specific circuit breaker. Requires breaker name and PIN.",
  {
    breaker: z.string().describe("Breaker name: BUYING, SELLING, QUEUE, PUSH, or CANCELS"),
    pin: z.string().describe("Operator PIN for authorization"),
  },
  async ({ breaker, pin }) => {
    const resp = await fetch(
      `${BASE_URL}/api/failsafe/breaker/${breaker}/reset?pin=${pin}&actor=Claude-MCP`,
      { method: "POST" }
    );
    const data = await resp.json();
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

// ── API Health ──

server.tool(
  "medici_api_health",
  "Check health of all monitored API endpoints (Production Backend, Zenith, Dev, SQL Server, Azure AD)",
  {},
  async () => {
    const data = await apiFetch("/api/azure/health");
    const summary = data.map((ep) => ({
      endpoint: ep.endpoint?.split("(")[0]?.trim(),
      healthy: ep.isHealthy,
      responseMs: ep.responseTimeMs,
      status: ep.statusCode,
    }));
    return { content: [{ type: "text", text: JSON.stringify(summary, null, 2) }] };
  }
);

// ── SLA ──

server.tool(
  "medici_sla",
  "Get SLA report: uptime percentages, MTTR, MTTD for all monitored endpoints",
  {},
  async () => {
    const data = await apiFetch("/api/sla");
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

// ── SalesOrder Diagnostics ──

server.tool(
  "medici_salesorder_diagnostics",
  "Get SalesOffice order pipeline diagnostics: running orders, failed orders, throughput, hotel coverage",
  {},
  async () => {
    const data = await apiFetch("/api/salesorder/diagnostics");
    const summary = {
      summary: data.summary,
      runningOrders: data.runningOrders?.length || 0,
      failedOrders: data.failedOrders?.length || 0,
      zeroMappingOrders: data.zeroMappingOrders?.length || 0,
    };
    return { content: [{ type: "text", text: JSON.stringify(summary, null, 2) }] };
  }
);

// ── DB Health ──

server.tool(
  "medici_db_health",
  "Get database health report: connections, size, long-running queries, deadlocks, index fragmentation",
  {},
  async () => {
    const data = await apiFetch("/api/db-health");
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

// ── Innstant Booking Verify ──

server.tool(
  "medici_verify_innstant_booking",
  "Verify a specific booking exists in Innstant by its content booking ID",
  { bookingId: z.number().describe("The contentBookingID to verify") },
  async ({ bookingId }) => {
    const data = await apiFetch(`/api/reconciliation/innstant/${bookingId}`);
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

// ── Notifications ──

server.tool(
  "medici_send_test_notification",
  "Send a test notification through all configured channels (Telegram, Email, Slack, WhatsApp)",
  {},
  async () => {
    const resp = await fetch(`${BASE_URL}/api/notifications/test`, { method: "POST" });
    const data = await resp.json();
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

// ── FailSafe Scan ──

server.tool(
  "medici_failsafe_scan",
  "Run a fail-safe scan: checks 8 business rules for anomalies (price drift, spend spikes, duplicate bookings, etc.)",
  {},
  async () => {
    const data = await apiFetch("/api/failsafe/scan");
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

// ── Deep Verification ──

server.tool(
  "medici_deep_verify",
  "Run deep cross-system verification: checks every booking in Medici against Innstant API and Zenith. Detects anomalies: missing bookings, price mismatches, status conflicts, ghost cancellations, duplicates, expiring unsold rooms.",
  { hours: z.number().optional().describe("Lookback hours (default 48)") },
  async ({ hours }) => {
    const h = hours || 48;
    const data = await apiFetch(`/api/verify/deep?hours=${h}`);
    return { content: [{ type: "text", text: JSON.stringify({
      totalBookings: data.totalBookings,
      totalReservations: data.totalReservations,
      innstantVerified: data.innstantVerifiedOk,
      totalAnomalies: data.totalAnomalies,
      criticalAnomalies: data.criticalAnomalies,
      anomalies: data.anomalies?.slice(0, 15),
      durationMs: data.durationMs
    }, null, 2) }] };
  }
);

server.tool(
  "medici_anomalies",
  "Get list of detected anomalies from the last deep verification run",
  {},
  async () => {
    const data = await apiFetch("/api/verify/anomalies");
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

// ── System Monitor (full health scan) ──

server.tool(
  "medici_monitor_full",
  "Run full system monitor scan: WebJob health, table row counts, mapping quality, skills health, orders status, Zenith SOAP probe, cancellation health, cancel error analysis. Includes trend analysis and alert escalation.",
  {},
  async () => {
    const data = await apiFetch("/api/monitor/full");
    const summary = {
      timestamp: data.timestamp,
      alertCount: data.alerts?.length || 0,
      alerts: data.alerts?.map((a) => ({
        severity: a.severity,
        component: a.component,
        message: a.message,
      })),
      trend: data.trend
        ? {
            overallTrend: data.trend.overallTrend,
            healthPct: data.trend.healthPct,
            totalRuns: data.trend.totalRuns,
          }
        : null,
      checks: Object.keys(data.results || {}),
    };
    return {
      content: [{ type: "text", text: JSON.stringify(summary, null, 2) }],
    };
  }
);

server.tool(
  "medici_monitor_check",
  "Run a specific system monitor check. Available checks: webjob, tables, mapping, skills, orders, zenith, cancellation, cancel_errors",
  {
    check: z
      .string()
      .describe(
        "Check name: webjob, tables, mapping, skills, orders, zenith, cancellation, cancel_errors, buyrooms, reservations, price_override_pipeline, data_freshness, booking_sales"
      ),
  },
  async ({ check }) => {
    const data = await apiFetch(`/api/monitor/check/${check}`);
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

server.tool(
  "medici_monitor_trend",
  "Get system health trend analysis: overall trend (STABLE/DEGRADING/IMPROVING), health percentage, component breakdown with consecutive CRITICAL counts",
  { hours: z.number().optional().describe("Lookback hours (default 24)") },
  async ({ hours }) => {
    const h = hours || 24;
    const data = await apiFetch(`/api/monitor/trend?hours=${h}`);
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

server.tool(
  "medici_monitor_sales",
  "Get booking sales health: buy/sell conversion, P&L, expiring rooms, opportunities. Covers all 10 skills.",
  {},
  async () => {
    const data = await apiFetch("/api/monitor/check/booking_sales");
    return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
  }
);

// ── Start Server ──

const transport = new StdioServerTransport();
await server.connect(transport);
