"""
System Monitor Skill - Full monitoring of Medici booking engine

Monitors all systems, skills, WebJobs, tables, and processes.
Provides health checks, alerts, and detailed diagnostics.

Usage:
    python system_monitor.py --connection-string "..." --full
    python system_monitor.py --connection-string "..." --check webjob
    python system_monitor.py --connection-string "..." --check tables
    python system_monitor.py --connection-string "..." --check skills
    python system_monitor.py --connection-string "..." --check mapping
    python system_monitor.py --connection-string "..." --check orders
    python system_monitor.py --connection-string "..." --check zenith
    python system_monitor.py --connection-string "..." --alert-only
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta

import pyodbc
import requests

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

ZENITH_URL = os.environ.get("ZENITH_URL", "https://hotel.tools/service/Medici%20new")
ZENITH_USERNAME = os.environ.get("ZENITH_USERNAME", "APIMedici:Medici Live")
ZENITH_PASSWORD = os.environ.get("ZENITH_PASSWORD", "")

ALERT_THRESHOLDS = {
    "webjob_stale_minutes": 30,
    "mapping_miss_rate_per_hour": 10,
    "order_detail_gap_pct": 5,
    "override_failure_pct": 20,
    "table_growth_alert_rows": 10000,
    "scan_cycle_max_hours": 24,
}

# Escalation: after N consecutive CRITICAL alerts of the same type → EMERGENCY
ESCALATION_CONSECUTIVE_THRESHOLD = 3


# ═══════════════════════════════════════════════════════════════
# HISTORY DB (SQLite)
# ═══════════════════════════════════════════════════════════════

HISTORY_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor_history.db")


def _init_history_db(db_path=None):
    """Create history tables for trend tracking."""
    import sqlite3
    path = db_path or HISTORY_DB_PATH
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS run_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'completed',
            alert_count INTEGER DEFAULT 0,
            results_json TEXT,
            alerts_json  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_rh_ts ON run_history(timestamp);

        CREATE TABLE IF NOT EXISTS alert_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id      INTEGER,
            timestamp   TEXT NOT NULL,
            severity    TEXT NOT NULL,
            component   TEXT NOT NULL,
            message     TEXT,
            FOREIGN KEY (run_id) REFERENCES run_history(id)
        );
        CREATE INDEX IF NOT EXISTS idx_ah_component_ts ON alert_history(component, timestamp);
        CREATE INDEX IF NOT EXISTS idx_ah_severity ON alert_history(severity);
    """)
    conn.close()
    return path


def _save_to_history(results, alerts, db_path=None):
    """Save a run to SQLite history."""
    import sqlite3
    path = db_path or HISTORY_DB_PATH
    _init_history_db(path)
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    cursor.execute(
        """INSERT INTO run_history (timestamp, status, alert_count, results_json, alerts_json)
           VALUES (?, ?, ?, ?, ?)""",
        (now, "completed", len(alerts),
         json.dumps(results, default=str),
         json.dumps(alerts, default=str)),
    )
    run_id = cursor.lastrowid

    for alert in alerts:
        cursor.execute(
            """INSERT INTO alert_history (run_id, timestamp, severity, component, message)
               VALUES (?, ?, ?, ?, ?)""",
            (run_id, now, alert.get("severity", "INFO"),
             alert.get("component", "unknown"), alert.get("message", "")),
        )

    conn.commit()
    conn.close()
    return run_id


def get_trend_analysis(hours=24, db_path=None):
    """Analyze trends from history: recurring alerts, getting worse/better.

    Returns dict with trend data for each component.
    """
    import sqlite3
    path = db_path or HISTORY_DB_PATH
    _init_history_db(path)
    conn = sqlite3.connect(path)
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()

    # Alert frequency by component
    rows = conn.execute(
        """SELECT component, severity, COUNT(*) as cnt
           FROM alert_history WHERE timestamp > ?
           GROUP BY component, severity
           ORDER BY cnt DESC""",
        (cutoff,),
    ).fetchall()

    component_stats = {}
    for component, severity, count in rows:
        if component not in component_stats:
            component_stats[component] = {"total": 0, "by_severity": {}}
        component_stats[component]["total"] += count
        component_stats[component]["by_severity"][severity] = count

    # Consecutive CRITICAL alerts per component (for escalation)
    for component in component_stats:
        recent = conn.execute(
            """SELECT severity FROM alert_history
               WHERE component = ? AND timestamp > ?
               ORDER BY timestamp DESC LIMIT 10""",
            (component, cutoff),
        ).fetchall()

        consecutive_critical = 0
        for (sev,) in recent:
            if sev == "CRITICAL":
                consecutive_critical += 1
            else:
                break
        component_stats[component]["consecutive_critical"] = consecutive_critical

    # Run count and healthy ratio
    runs = conn.execute(
        "SELECT COUNT(*), SUM(CASE WHEN alert_count = 0 THEN 1 ELSE 0 END) FROM run_history WHERE timestamp > ?",
        (cutoff,),
    ).fetchone()
    total_runs = runs[0] or 0
    healthy_runs = runs[1] or 0

    # First vs second half comparison (is it getting worse?)
    mid = (datetime.now() - timedelta(hours=hours / 2)).isoformat()
    first_half = conn.execute(
        "SELECT COUNT(*) FROM alert_history WHERE timestamp > ? AND timestamp <= ?",
        (cutoff, mid),
    ).fetchone()[0]
    second_half = conn.execute(
        "SELECT COUNT(*) FROM alert_history WHERE timestamp > ?",
        (mid,),
    ).fetchone()[0]

    if first_half > 0 and second_half > first_half * 1.5:
        overall_trend = "DEGRADING"
    elif second_half == 0 and total_runs > 2:
        overall_trend = "IMPROVING"
    else:
        overall_trend = "STABLE"

    conn.close()

    return {
        "period_hours": hours,
        "total_runs": total_runs,
        "healthy_runs": healthy_runs,
        "health_pct": round(healthy_runs / total_runs * 100, 1) if total_runs > 0 else 0,
        "overall_trend": overall_trend,
        "first_half_alerts": first_half,
        "second_half_alerts": second_half,
        "components": component_stats,
    }


# ═══════════════════════════════════════════════════════════════
# MONITOR CLASS
# ═══════════════════════════════════════════════════════════════

class SystemMonitor:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.conn = pyodbc.connect(connection_string, timeout=30)
        self.conn.timeout = 60  # query timeout
        self.cursor = self.conn.cursor()
        self.results = {}
        self.alerts = []

    def _reconnect_if_needed(self):
        """Reconnect if connection was lost."""
        try:
            self.cursor.execute("SELECT 1")
        except (pyodbc.Error, Exception):
            logger.warning("DB connection lost, reconnecting...")
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = pyodbc.connect(self.connection_string, timeout=30)
            self.conn.timeout = 60
            self.cursor = self.conn.cursor()

    def _alert(self, severity, component, message):
        # Check for escalation: if same component had consecutive CRITICALs → EMERGENCY
        if severity == "CRITICAL":
            try:
                trend = get_trend_analysis(hours=6)
                comp_stats = trend.get("components", {}).get(component, {})
                consecutive = comp_stats.get("consecutive_critical", 0)
                if consecutive >= ESCALATION_CONSECUTIVE_THRESHOLD:
                    severity = "EMERGENCY"
                    message = f"[ESCALATED x{consecutive + 1}] {message}"
                    logger.warning("Escalation: %s has %d consecutive CRITICAL alerts → EMERGENCY", component, consecutive + 1)
            except Exception as e:
                logger.debug("Escalation check failed (non-critical): %s", e)

        self.alerts.append({
            "severity": severity,
            "component": component,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        })

    # ── 1. WEBJOB HEALTH ─────────────────────────────────────

    def check_webjob(self):
        """Check WebJob processing health"""
        checks = {}

        # Last activity
        self.cursor.execute("SELECT TOP 1 SalesOfficeOrderId, DateCreated FROM [SalesOffice.Log] ORDER BY Id DESC")
        row = self.cursor.fetchone()
        if row:
            last_order = row[0]
            last_time = row[1]
            minutes_ago = (datetime.now() - last_time).total_seconds() / 60
            checks["last_log"] = {
                "order_id": last_order,
                "time": str(last_time)[:19],
                "minutes_ago": round(minutes_ago, 1),
            }
            if minutes_ago > ALERT_THRESHOLDS["webjob_stale_minutes"]:
                self._alert("CRITICAL", "WebJob", f"No activity for {minutes_ago:.0f} minutes (last: Order {last_order})")
        else:
            checks["last_log"] = None
            self._alert("CRITICAL", "WebJob", "No log entries found")

        # In Progress orders
        self.cursor.execute("""
            SELECT o.Id, o.DestinationId, h.[Name], o.WebJobStatus
            FROM [SalesOffice.Orders] o
            LEFT JOIN Med_Hotels h ON h.HotelId = CAST(o.DestinationId AS INT)
            WHERE o.WebJobStatus LIKE '%In Progress%' AND o.Id > 26
        """)
        in_progress = []
        for r in self.cursor.fetchall():
            name = str(r[2]) if r[2] else f"Hotel {r[1]}"
            in_progress.append({"order_id": r[0], "hotel": name})
        checks["in_progress"] = in_progress

        # Pending orders
        self.cursor.execute("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE WebJobStatus IS NULL AND IsActive=1")
        pending = self.cursor.fetchone()[0]
        checks["pending_orders"] = pending

        # Failed orders (last 24h)
        self.cursor.execute("""
            SELECT COUNT(*) FROM [SalesOffice.Orders]
            WHERE WebJobStatus LIKE '%Failed%' AND IsActive=1
        """)
        failed = self.cursor.fetchone()[0]
        checks["failed_orders"] = failed
        if failed > 0:
            self._alert("WARNING", "WebJob", f"{failed} failed orders")

        # Scan cycle estimate
        self.cursor.execute("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE IsActive=1")
        total_active = self.cursor.fetchone()[0]
        cycle_hours = total_active * 30 / 3600
        checks["active_orders"] = total_active
        checks["estimated_cycle_hours"] = round(cycle_hours, 1)
        if cycle_hours > ALERT_THRESHOLDS["scan_cycle_max_hours"]:
            self._alert("WARNING", "WebJob", f"Scan cycle estimated {cycle_hours:.0f}h (threshold: {ALERT_THRESHOLDS['scan_cycle_max_hours']}h)")

        self.results["webjob"] = checks
        return checks

    # ── 2. TABLE HEALTH ───────────────────────────────────────

    def check_tables(self):
        """Check all SalesOffice tables"""
        tables = {}

        table_queries = {
            "SalesOffice.Orders": "SELECT COUNT(*), SUM(CASE WHEN IsActive=1 THEN 1 ELSE 0 END) FROM [SalesOffice.Orders]",
            "SalesOffice.Details": "SELECT COUNT(*), SUM(CASE WHEN IsDeleted=0 THEN 1 ELSE 0 END) FROM [SalesOffice.Details]",
            "SalesOffice.MappingMisses": "SELECT COUNT(*), SUM(CASE WHEN Status='new' THEN 1 ELSE 0 END) FROM [SalesOffice.MappingMisses]",
            "SalesOffice.PriceOverride": "SELECT COUNT(*), SUM(CASE WHEN IsActive=1 THEN 1 ELSE 0 END) FROM [SalesOffice.PriceOverride]",
            "SalesOffice.Log": "SELECT COUNT(*), NULL FROM [SalesOffice.Log]",
            "Med_Hotels_ratebycat": "SELECT COUNT(*), NULL FROM Med_Hotels_ratebycat",
            "BackOfficeOPT": "SELECT COUNT(*), SUM(CASE WHEN Status=1 THEN 1 ELSE 0 END) FROM BackOfficeOPT",
            "MED_Book": "SELECT COUNT(*), SUM(CASE WHEN IsActive=1 THEN 1 ELSE 0 END) FROM MED_Book",
            "MED_CancelBook": "SELECT COUNT(*), NULL FROM MED_CancelBook",
            "MED_CancelBookError": "SELECT COUNT(*), NULL FROM MED_CancelBookError",
        }

        for table, query in table_queries.items():
            try:
                self.cursor.execute(query)
                row = self.cursor.fetchone()
                tables[table] = {"total": row[0], "active": row[1]}
            except Exception as e:
                tables[table] = {"error": str(e)[:80]}
                self._alert("ERROR", "Tables", f"Cannot query {table}: {str(e)[:60]}")

        self.results["tables"] = tables
        return tables

    # ── 3. MAPPING HEALTH ─────────────────────────────────────

    def check_mapping(self):
        """Check mapping quality across all hotels"""
        checks = {}

        # Hotels with active orders
        self.cursor.execute("""
            SELECT DISTINCT o.DestinationId, h.[Name]
            FROM [SalesOffice.Orders] o
            LEFT JOIN Med_Hotels h ON h.HotelId = CAST(o.DestinationId AS INT)
            WHERE o.IsActive = 1
        """)
        active_hotels = self.cursor.fetchall()
        checks["active_hotels"] = len(active_hotels)

        # ratebycat coverage
        self.cursor.execute("""
            SELECT COUNT(DISTINCT HotelId) FROM Med_Hotels_ratebycat
        """)
        hotels_with_rbc = self.cursor.fetchone()[0]
        checks["hotels_with_ratebycat"] = hotels_with_rbc

        # Hotels with BB
        self.cursor.execute("""
            SELECT COUNT(DISTINCT HotelId) FROM Med_Hotels_ratebycat WHERE BoardId = 2
        """)
        hotels_with_bb = self.cursor.fetchone()[0]
        checks["hotels_with_bb"] = hotels_with_bb

        # Open mapping misses
        self.cursor.execute("""
            SELECT HotelId, RoomCategory, RoomBoard, COUNT(*) as Hits
            FROM [SalesOffice.MappingMisses]
            WHERE Status = 'new'
            GROUP BY HotelId, RoomCategory, RoomBoard
        """)
        misses = []
        for r in self.cursor.fetchall():
            misses.append({"hotel_id": r[0], "category": r[1], "board": r[2], "hits": r[3]})
        checks["open_misses"] = misses
        if len(misses) > 0:
            self._alert("INFO", "Mapping", f"{len(misses)} open mapping misses")

        # Miss rate (last hour)
        self.cursor.execute("""
            SELECT COUNT(*) FROM [SalesOffice.MappingMisses]
            WHERE SeenAt >= DATEADD(HOUR, -1, GETDATE())
        """)
        miss_rate = self.cursor.fetchone()[0]
        checks["miss_rate_last_hour"] = miss_rate
        if miss_rate > ALERT_THRESHOLDS["mapping_miss_rate_per_hour"]:
            self._alert("WARNING", "Mapping", f"High miss rate: {miss_rate}/hour")

        # ORDER = DETAIL check per hotel
        order_detail_gaps = []
        for hid_str, hname in active_hotels:
            hid = int(hid_str)
            name = str(hname) if hname else f"Hotel {hid}"

            self.cursor.execute("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE DestinationId=? AND IsActive=1", hid_str)
            total = self.cursor.fetchone()[0]

            self.cursor.execute("""
                SELECT COUNT(*) FROM [SalesOffice.Orders] o
                WHERE o.DestinationId=? AND o.IsActive=1
                AND EXISTS (SELECT 1 FROM [SalesOffice.Details] d WHERE d.SalesOfficeOrderId=o.Id AND d.IsDeleted=0)
            """, hid_str)
            with_det = self.cursor.fetchone()[0]

            self.cursor.execute("""
                SELECT COUNT(*) FROM [SalesOffice.Orders]
                WHERE DestinationId=? AND IsActive=1
                AND (WebJobStatus LIKE '%Api Rooms: 0%' OR WebJobStatus LIKE '%Api: 0%')
            """, hid_str)
            api_zero = self.cursor.fetchone()[0]

            self.cursor.execute("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE DestinationId=? AND IsActive=1 AND WebJobStatus IS NULL", hid_str)
            null_count = self.cursor.fetchone()[0]

            gap = total - with_det - api_zero - null_count
            if gap > 0:
                order_detail_gaps.append({"hotel": name[:30], "hotel_id": hid, "gap": gap, "total": total})

        checks["order_detail_gaps"] = order_detail_gaps
        if order_detail_gaps:
            self._alert("WARNING", "Mapping", f"{len(order_detail_gaps)} hotels with ORDER!=DETAIL gaps")

        self.results["mapping"] = checks
        return checks

    # ── 4. SKILLS HEALTH ──────────────────────────────────────

    def check_skills(self):
        """Check health of all skills"""
        checks = {}

        # autofix_worker - last run
        checks["autofix_worker"] = self._check_skill_audit(
            "salesoffice-mapping-gap-skill/autofix-report",
            "autofix_worker"
        )

        # Price Override - active overrides
        self.cursor.execute("""
            SELECT COUNT(*),
                SUM(CASE WHEN IsActive=1 AND PushStatus IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN IsActive=1 AND PushStatus='success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN PushStatus='failed' THEN 1 ELSE 0 END)
            FROM [SalesOffice.PriceOverride]
        """)
        r = self.cursor.fetchone()
        checks["price_override"] = {
            "total": r[0], "pending": r[1] or 0, "pushed": r[2] or 0, "failed": r[3] or 0,
        }
        if r[3] and r[0] and r[3] / r[0] * 100 > ALERT_THRESHOLDS["override_failure_pct"]:
            self._alert("WARNING", "PriceOverride", f"{r[3]} failed overrides ({r[3]/r[0]*100:.0f}%)")

        # Insert Opp - recent activity
        self.cursor.execute("""
            SELECT COUNT(*),
                SUM(CASE WHEN Status=1 THEN 1 ELSE 0 END)
            FROM BackOfficeOPT
            WHERE DateInsert >= DATEADD(DAY, -1, GETDATE())
        """)
        r = self.cursor.fetchone()
        checks["insert_opp"] = {"last_24h": r[0], "active": r[1] or 0}

        self.results["skills"] = checks
        return checks

    def _check_skill_audit(self, report_dir, skill_name):
        """Check last audit file for a skill"""
        base = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), report_dir)
        if not os.path.exists(base):
            return {"status": "no_report_dir"}
        files = sorted([f for f in os.listdir(base) if f.endswith(".json")], reverse=True)
        if not files:
            return {"status": "no_audit_files"}
        latest = files[0]
        mtime = os.path.getmtime(os.path.join(base, latest))
        age_min = (time.time() - mtime) / 60
        return {"last_file": latest, "age_minutes": round(age_min, 1)}

    # ── 5. ORDERS HEALTH ─────────────────────────────────────

    def check_orders(self):
        """Check SalesOffice orders health"""
        checks = {}

        # Active hotels and orders
        self.cursor.execute("""
            SELECT COUNT(DISTINCT DestinationId), COUNT(*)
            FROM [SalesOffice.Orders] WHERE IsActive = 1
        """)
        r = self.cursor.fetchone()
        checks["active_hotels"] = r[0]
        checks["active_orders"] = r[1]

        # Status breakdown
        self.cursor.execute("""
            SELECT
                SUM(CASE WHEN WebJobStatus IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN WebJobStatus LIKE '%Completed%' THEN 1 ELSE 0 END),
                SUM(CASE WHEN WebJobStatus LIKE '%In Progress%' THEN 1 ELSE 0 END),
                SUM(CASE WHEN WebJobStatus LIKE '%Failed%' THEN 1 ELSE 0 END),
                SUM(CASE WHEN WebJobStatus LIKE '%DateRange%' THEN 1 ELSE 0 END)
            FROM [SalesOffice.Orders] WHERE IsActive = 1
        """)
        r = self.cursor.fetchone()
        checks["status"] = {
            "null_pending": r[0] or 0, "completed": r[1] or 0,
            "in_progress": r[2] or 0, "failed": r[3] or 0,
            "date_error": r[4] or 0,
        }

        # Details breakdown
        self.cursor.execute("""
            SELECT
                SUM(CASE WHEN RoomBoard='RO' AND IsDeleted=0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN RoomBoard='BB' AND IsDeleted=0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN IsDeleted=0 THEN 1 ELSE 0 END)
            FROM [SalesOffice.Details]
        """)
        r = self.cursor.fetchone()
        checks["details"] = {"ro": r[0] or 0, "bb": r[1] or 0, "total": r[2] or 0}

        # Archive tables
        for archive in ["SalesOffice.Orders_Deleted_20260319", "SalesOffice.Details_Deleted_20260319"]:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM [{archive}]")
                checks[f"archive_{archive.split('_')[0].split('.')[-1].lower()}"] = self.cursor.fetchone()[0]
            except (pyodbc.Error, Exception) as e:
                logger.debug("Archive table %s not found: %s", archive, e)

        self.results["orders"] = checks
        return checks

    # ── 6. ZENITH HEALTH ──────────────────────────────────────

    def check_zenith(self):
        """Check Zenith API connectivity"""
        checks = {}

        # Probe with a known venue
        soap = f'''<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
  <SOAP-ENV:Header><wsse:Security soap:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><wsse:UsernameToken><wsse:Username>{ZENITH_USERNAME}</wsse:Username><wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{ZENITH_PASSWORD}</wsse:Password></wsse:UsernameToken></wsse:Security></SOAP-ENV:Header>
  <SOAP-ENV:Body><OTA_HotelAvailNotifRQ xmlns="http://www.opentravel.org/OTA/2003/05" Version="1.0" TimeStamp="{datetime.now().isoformat()}" EchoToken="monitor"><AvailStatusMessages HotelCode="5093"><AvailStatusMessage BookingLimit="0"><StatusApplicationControl Start="2026-12-31" End="2026-12-31" InvTypeCode="Stnd" RatePlanCode="12062"/><RestrictionStatus Status="Open" /></AvailStatusMessage></AvailStatusMessages></OTA_HotelAvailNotifRQ></SOAP-ENV:Body></SOAP-ENV:Envelope>'''

        try:
            start = time.time()
            resp = requests.post(ZENITH_URL, data=soap, headers={"Content-Type": "text/xml"}, timeout=15)
            latency = (time.time() - start) * 1000
            success = resp.status_code == 200 and "Error" not in resp.text
            checks["status"] = "OK" if success else "ERROR"
            checks["latency_ms"] = round(latency, 0)
            checks["http_status"] = resp.status_code
            if not success:
                self._alert("CRITICAL", "Zenith", f"Zenith API error: HTTP {resp.status_code}")
        except Exception as e:
            checks["status"] = "UNREACHABLE"
            checks["error"] = str(e)[:100]
            self._alert("CRITICAL", "Zenith", f"Zenith unreachable: {str(e)[:60]}")

        self.results["zenith"] = checks
        return checks

    # ── 7. AUTO-CANCELLATION HEALTH ───────────────────────────

    def check_cancellation(self):
        """Check auto-cancellation system"""
        checks = {}

        # Active bookings approaching CX deadline
        self.cursor.execute("""
            SELECT COUNT(*) FROM MED_Book
            WHERE IsActive = 1 AND CancellationTo <= DATEADD(DAY, 5, GETDATE())
        """)
        upcoming = self.cursor.fetchone()[0]
        checks["bookings_near_cx_deadline"] = upcoming
        if upcoming > 10:
            self._alert("INFO", "Cancellation", f"{upcoming} bookings within 5 days of CX deadline")

        # Recent cancellations (24h)
        self.cursor.execute("""
            SELECT COUNT(*) FROM MED_CancelBook
            WHERE DateInsert >= DATEADD(DAY, -1, GETDATE())
        """)
        checks["cancellations_24h"] = self.cursor.fetchone()[0]

        # Cancel errors (24h)
        self.cursor.execute("""
            SELECT COUNT(*) FROM MED_CancelBookError
            WHERE DateInsert >= DATEADD(DAY, -1, GETDATE())
        """)
        errors = self.cursor.fetchone()[0]
        checks["cancel_errors_24h"] = errors
        if errors > 0:
            self._alert("WARNING", "Cancellation", f"{errors} cancellation errors in last 24h")

        # Total active bookings
        self.cursor.execute("SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1")
        checks["active_bookings"] = self.cursor.fetchone()[0]

        self.results["cancellation"] = checks
        return checks

    # ── 8. CANCEL ERROR ANALYSIS ─────────────────────────────

    def check_cancel_errors(self):
        """Deep analysis of MED_CancelBookError — breakdown by type, hotel, trend."""
        checks = {}

        # Total errors
        self.cursor.execute("SELECT COUNT(*) FROM MED_CancelBookError")
        checks["total_errors"] = self.cursor.fetchone()[0]

        # Top error types (by error message pattern)
        self.cursor.execute("""
            SELECT TOP 10
                LEFT(ErrorMessage, 80) as error_type,
                COUNT(*) as cnt
            FROM MED_CancelBookError
            GROUP BY LEFT(ErrorMessage, 80)
            ORDER BY cnt DESC
        """)
        checks["top_error_types"] = [
            {"error": r[0], "count": r[1]} for r in self.cursor.fetchall()
        ]

        # Per-hotel error rates (last 30 days)
        self.cursor.execute("""
            SELECT
                cb.HotelId,
                h.[Name],
                COUNT(*) as errors
            FROM MED_CancelBookError cb
            LEFT JOIN Med_Hotels h ON h.HotelId = cb.HotelId
            WHERE cb.DateInsert >= DATEADD(DAY, -30, GETDATE())
            GROUP BY cb.HotelId, h.[Name]
            ORDER BY errors DESC
        """)
        checks["per_hotel_30d"] = [
            {"hotel_id": r[0], "hotel": str(r[1]) if r[1] else f"Hotel {r[0]}", "errors": r[2]}
            for r in self.cursor.fetchall()
        ]

        # Trend: last 7 days daily counts
        self.cursor.execute("""
            SELECT
                CAST(DateInsert AS DATE) as day,
                COUNT(*) as cnt
            FROM MED_CancelBookError
            WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())
            GROUP BY CAST(DateInsert AS DATE)
            ORDER BY day DESC
        """)
        daily = [{"date": str(r[0]), "count": r[1]} for r in self.cursor.fetchall()]
        checks["daily_trend_7d"] = daily

        # Is it trending up?
        if len(daily) >= 3:
            recent_avg = sum(d["count"] for d in daily[:3]) / 3
            older_avg = sum(d["count"] for d in daily[3:]) / max(1, len(daily) - 3)
            if older_avg > 0 and recent_avg > older_avg * 1.5:
                checks["trend"] = "INCREASING"
                self._alert("WARNING", "CancelErrors",
                            f"Cancel errors trending up: {recent_avg:.0f}/day recent vs {older_avg:.0f}/day older")
            elif recent_avg < older_avg * 0.5:
                checks["trend"] = "DECREASING"
            else:
                checks["trend"] = "STABLE"
        else:
            checks["trend"] = "INSUFFICIENT_DATA"

        self.results["cancel_errors"] = checks
        return checks

    # ── FULL REPORT ───────────────────────────────────────────

    def run_full(self):
        """Run all checks"""
        self._reconnect_if_needed()
        self.check_webjob()
        self.check_tables()
        self.check_mapping()
        self.check_skills()
        self.check_orders()
        self.check_zenith()
        self.check_cancellation()
        self.check_cancel_errors()
        return self.results

    # ── OUTPUT ────────────────────────────────────────────────

    def print_report(self, alert_only=False):
        """Print formatted report"""
        if alert_only:
            if not self.alerts:
                print("NO ALERTS - all systems healthy")
            else:
                print(f"=== {len(self.alerts)} ALERTS ===")
                for a in self.alerts:
                    print(f"  [{a['severity']}] {a['component']}: {a['message']}")
            return

        print("=" * 60)
        print(f"  SYSTEM MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)

        # Alerts first
        if self.alerts:
            print(f"\n{'!'*60}")
            print(f"  {len(self.alerts)} ALERT(S)")
            print(f"{'!'*60}")
            for a in self.alerts:
                icon = {"CRITICAL": "!!!", "WARNING": " ! ", "ERROR": "ERR", "INFO": " i "}.get(a["severity"], "???")
                print(f"  [{icon}] {a['component']}: {a['message']}")

        # WebJob
        if "webjob" in self.results:
            wj = self.results["webjob"]
            print(f"\n--- WebJob ---")
            if wj.get("last_log"):
                print(f"  Last activity: {wj['last_log']['minutes_ago']:.0f}m ago (Order {wj['last_log']['order_id']})")
            print(f"  Pending: {wj.get('pending_orders', '?')} | Failed: {wj.get('failed_orders', '?')}")
            print(f"  Active orders: {wj.get('active_orders', '?')} | Cycle: ~{wj.get('estimated_cycle_hours', '?')}h")

        # Tables
        if "tables" in self.results:
            print(f"\n--- Tables ---")
            for name, info in self.results["tables"].items():
                if "error" in info:
                    print(f"  {name:40} ERROR: {info['error']}")
                else:
                    active = f" (active={info['active']})" if info['active'] is not None else ""
                    print(f"  {name:40} {info['total']:>7} rows{active}")

        # Mapping
        if "mapping" in self.results:
            m = self.results["mapping"]
            print(f"\n--- Mapping ---")
            print(f"  Hotels: {m.get('active_hotels', '?')} active | {m.get('hotels_with_bb', '?')} with BB")
            print(f"  Miss rate: {m.get('miss_rate_last_hour', '?')}/hour")
            gaps = m.get("order_detail_gaps", [])
            if gaps:
                print(f"  ORDER!=DETAIL gaps: {len(gaps)} hotels")
                for g in gaps[:5]:
                    print(f"    {g['hotel']}: {g['gap']} gaps / {g['total']} orders")
            else:
                print(f"  ORDER=DETAIL: ALL PASS")

        # Skills
        if "skills" in self.results:
            s = self.results["skills"]
            print(f"\n--- Skills ---")
            po = s.get("price_override", {})
            print(f"  PriceOverride: {po.get('total', 0)} total | {po.get('pushed', 0)} pushed | {po.get('failed', 0)} failed")
            io = s.get("insert_opp", {})
            print(f"  InsertOpp: {io.get('last_24h', 0)} last 24h | {io.get('active', 0)} active")

        # Orders
        if "orders" in self.results:
            o = self.results["orders"]
            print(f"\n--- Orders & Details ---")
            print(f"  Hotels: {o.get('active_hotels', '?')} | Orders: {o.get('active_orders', '?')}")
            d = o.get("details", {})
            print(f"  Details: RO={d.get('ro', '?')} BB={d.get('bb', '?')} Total={d.get('total', '?')}")

        # Zenith
        if "zenith" in self.results:
            z = self.results["zenith"]
            print(f"\n--- Zenith API ---")
            print(f"  Status: {z.get('status', '?')} | Latency: {z.get('latency_ms', '?')}ms")

        # Cancellation
        if "cancellation" in self.results:
            c = self.results["cancellation"]
            print(f"\n--- Auto-Cancellation ---")
            print(f"  Active bookings: {c.get('active_bookings', '?')}")
            print(f"  Near CX deadline: {c.get('bookings_near_cx_deadline', '?')}")
            print(f"  Cancelled 24h: {c.get('cancellations_24h', '?')} | Errors: {c.get('cancel_errors_24h', '?')}")

        # Cancel error analysis
        if "cancel_errors" in self.results:
            ce = self.results["cancel_errors"]
            print(f"\n--- Cancel Error Analysis ---")
            print(f"  Total errors: {ce.get('total_errors', '?')} | Trend: {ce.get('trend', '?')}")
            top = ce.get("top_error_types", [])
            if top:
                print(f"  Top error types:")
                for t in top[:5]:
                    print(f"    {t['count']:>5} × {t['error']}")
            per_hotel = ce.get("per_hotel_30d", [])
            if per_hotel:
                print(f"  Per-hotel (30d):")
                for h in per_hotel[:5]:
                    print(f"    {h['hotel']:30} {h['errors']} errors")

        print(f"\n{'=' * 60}")
        print(f"  {len(self.alerts)} alerts | {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'=' * 60}")

    def save_report(self, output_dir):
        os.makedirs(output_dir, exist_ok=True)
        report = {
            "timestamp": datetime.now().isoformat(),
            "results": self.results,
            "alerts": self.alerts,
        }
        path = os.path.join(output_dir, f"monitor-{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        # Also save to SQLite history for trend tracking
        try:
            _save_to_history(self.results, self.alerts)
        except Exception as e:
            logger.warning("Failed to save to history DB: %s", e)

        return path

    def close(self):
        self.conn.close()


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="System Monitor")
    parser.add_argument("--connection-string", required=False)
    parser.add_argument("--full", action="store_true", help="Run all checks")
    parser.add_argument("--check", choices=["webjob", "tables", "skills", "mapping", "orders", "zenith", "cancellation", "cancel_errors"])
    parser.add_argument("--alert-only", action="store_true", help="Show only alerts")
    parser.add_argument("--trend", action="store_true", help="Show trend analysis from history (no DB required)")
    parser.add_argument("--trend-hours", type=int, default=24, help="Hours of history for trend analysis")
    parser.add_argument("--out", default="monitor-report")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    # Trend-only mode — no DB connection needed
    if args.trend:
        trend = get_trend_analysis(hours=args.trend_hours)
        if args.json:
            print(json.dumps(trend, indent=2, default=str))
        else:
            print(f"{'=' * 60}")
            print(f"  TREND ANALYSIS — Last {trend['period_hours']}h")
            print(f"{'=' * 60}")
            print(f"  Runs: {trend['total_runs']} | Healthy: {trend['healthy_runs']} ({trend['health_pct']}%)")
            print(f"  Overall trend: {trend['overall_trend']}")
            print(f"  Alerts: first half={trend['first_half_alerts']}, second half={trend['second_half_alerts']}")
            if trend["components"]:
                print(f"\n--- Components ---")
                for comp, stats in trend["components"].items():
                    consec = stats.get("consecutive_critical", 0)
                    esc = " *** ESCALATION ***" if consec >= ESCALATION_CONSECUTIVE_THRESHOLD else ""
                    print(f"  {comp}: {stats['total']} alerts {stats['by_severity']} (consec CRITICAL: {consec}){esc}")
            print(f"{'=' * 60}")
        return

    if not args.connection_string:
        parser.error("--connection-string is required unless using --trend")

    monitor = SystemMonitor(args.connection_string)

    try:
        if args.full or args.alert_only:
            monitor.run_full()
        elif args.check:
            getattr(monitor, f"check_{args.check}")()
        else:
            monitor.run_full()

        if args.json:
            output = {"results": monitor.results, "alerts": monitor.alerts}
            # Include trend data in JSON output
            try:
                output["trend"] = get_trend_analysis(hours=24)
            except Exception:
                pass
            print(json.dumps(output, indent=2, default=str))
        else:
            monitor.print_report(alert_only=args.alert_only)

        report_path = monitor.save_report(args.out)

    finally:
        monitor.close()


if __name__ == "__main__":
    main()
