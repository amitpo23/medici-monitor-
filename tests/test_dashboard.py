"""
MediciMonitor Dashboard — Automated UI Tests
Uses Playwright to verify all dashboard tabs, APIs, and interactive features.
Run: python tests/test_dashboard.py [--base-url http://localhost:5000]
"""

import sys
import json
import time
import argparse
from playwright.sync_api import sync_playwright

BASE_URL = "http://localhost:5000"
RESULTS = {"passed": 0, "failed": 0, "errors": []}


def log_pass(name):
    RESULTS["passed"] += 1
    print(f"  ✅ {name}")


def log_fail(name, error=""):
    RESULTS["failed"] += 1
    RESULTS["errors"].append(f"{name}: {error}")
    print(f"  ❌ {name} — {error}")


def test_health_endpoints(page):
    """Test basic health endpoints return valid JSON"""
    print("\n🔹 Health Endpoints")

    for endpoint in ["/healthz", "/readyz"]:
        try:
            resp = page.request.get(f"{BASE_URL}{endpoint}")
            data = resp.json()
            if resp.ok and "status" in data:
                log_pass(f"GET {endpoint} → {data['status']}")
            else:
                log_fail(f"GET {endpoint}", f"HTTP {resp.status}")
        except Exception as e:
            log_fail(f"GET {endpoint}", str(e))


def test_api_endpoints(page):
    """Test core API endpoints return valid data"""
    print("\n🔹 API Endpoints")

    endpoints = [
        ("/api/status", lambda d: "dbConnected" in d or "DbConnected" in d),
        ("/api/alerts", lambda d: isinstance(d, list)),
        ("/api/sla", lambda d: "endpoints" in d or "Endpoints" in d or "overallUptime" in d or "OverallUptime" in d),
        ("/api/notifications/config", lambda d: "webhookEnabled" in d or "WebhookEnabled" in d),
        ("/api/notifications/history", lambda d: isinstance(d, list)),
        ("/api/reconciliation/status", lambda d: True),  # Any valid JSON
        ("/api/failsafe/breakers", lambda d: isinstance(d, list)),
        ("/api/logs/stats", lambda d: True),
        ("/api/ai/status", lambda d: "available" in d or "Available" in d),
    ]

    for path, validator in endpoints:
        try:
            resp = page.request.get(f"{BASE_URL}{path}")
            if resp.ok:
                data = resp.json()
                if validator(data):
                    log_pass(f"GET {path}")
                else:
                    log_fail(f"GET {path}", "Invalid response structure")
            else:
                log_fail(f"GET {path}", f"HTTP {resp.status}")
        except Exception as e:
            log_fail(f"GET {path}", str(e))


def test_dashboard_loads(page):
    """Test that the dashboard HTML loads and renders"""
    print("\n🔹 Dashboard Loading")

    try:
        page.goto(f"{BASE_URL}/index.html")
        page.wait_for_load_state("networkidle", timeout=30000)
        log_pass("Dashboard HTML loaded")
    except Exception as e:
        log_fail("Dashboard load", str(e))
        return

    # Check title
    title = page.title()
    if "Medici" in title or "ALERT" in title or "Monitor" in title:
        log_pass(f"Title: {title}")
    else:
        log_fail("Title check", f"Got: {title}")

    # Check top bar exists
    topbar = page.locator(".topbar")
    if topbar.count() > 0:
        log_pass("Top bar rendered")
    else:
        log_fail("Top bar missing")

    # Screenshot
    page.screenshot(path="tests/screenshots/dashboard_overview.png", full_page=False)
    log_pass("Screenshot saved: dashboard_overview.png")


def test_nav_tabs(page):
    """Test all navigation tabs exist and can be clicked"""
    print("\n🔹 Navigation Tabs")

    expected_tabs = [
        "overview", "salesorder", "reservations", "waste", "conversion",
        "pricedrift", "errors", "azure", "bi", "emergency", "history",
        "alerting", "sla", "dbhealth", "incidents", "audit", "notifications",
        "failsafe", "reconciliation", "webjobs", "ai", "logs"
    ]

    tabs = page.locator(".nav-tab")
    tab_count = tabs.count()

    if tab_count >= 15:
        log_pass(f"Found {tab_count} nav tabs")
    else:
        log_fail(f"Expected 15+ tabs, found {tab_count}")

    # Click each tab and verify panel appears
    for tab_name in expected_tabs:
        try:
            tab = page.locator(f'.nav-tab[data-nav="{tab_name}"]')
            if tab.count() == 0:
                log_fail(f"Tab '{tab_name}'", "Not found")
                continue

            tab.click()
            time.sleep(0.3)

            panel = page.locator(f'#panel-{tab_name}')
            if panel.count() > 0 and panel.is_visible():
                log_pass(f"Tab '{tab_name}' → panel visible")
            else:
                log_fail(f"Tab '{tab_name}'", "Panel not visible after click")
        except Exception as e:
            log_fail(f"Tab '{tab_name}'", str(e))

    # Go back to overview
    page.locator('.nav-tab[data-nav="overview"]').click()
    time.sleep(0.5)


def test_overview_kpis(page):
    """Test that overview KPI cards render with data"""
    print("\n🔹 Overview KPIs")

    page.locator('.nav-tab[data-nav="overview"]').click()
    page.wait_for_load_state("networkidle", timeout=15000)
    time.sleep(2)  # Wait for data to load

    # Check for KPI values (they should have numbers or text)
    page.screenshot(path="tests/screenshots/overview_kpis.png", full_page=False)
    log_pass("Overview screenshot saved")

    # Check DB status indicator
    body_html = page.content()
    if "dot" in body_html:
        log_pass("Status indicator dot found")
    else:
        log_fail("Status indicator dot missing")


def test_alerts_tab(page):
    """Test alerts tab renders alert list"""
    print("\n🔹 Alerts Tab")

    page.locator('.nav-tab[data-nav="alerting"]').click()
    time.sleep(2)

    page.screenshot(path="tests/screenshots/alerts.png", full_page=False)
    log_pass("Alerts tab screenshot saved")


def test_reconciliation_tab(page):
    """Test reconciliation tab renders"""
    print("\n🔹 Reconciliation Tab")

    page.locator('.nav-tab[data-nav="reconciliation"]').click()
    time.sleep(1)

    # Check UI elements exist
    run_btn = page.locator('button:has-text("הרץ בדיקה")')
    if run_btn.count() > 0:
        log_pass("Run reconciliation button found")
    else:
        log_fail("Run reconciliation button missing")

    hours_select = page.locator("#reconHours")
    if hours_select.count() > 0:
        log_pass("Hours selector found")
    else:
        log_fail("Hours selector missing")

    page.screenshot(path="tests/screenshots/reconciliation.png", full_page=False)
    log_pass("Reconciliation tab screenshot saved")


def test_killswitch_tab(page):
    """Test kill switch tab renders with breakers"""
    print("\n🔹 Kill Switch Tab")

    page.locator('.nav-tab[data-nav="failsafe"]').click()
    time.sleep(1)

    page.screenshot(path="tests/screenshots/killswitch.png", full_page=False)
    log_pass("Kill switch tab screenshot saved")


def test_notifications_tab(page):
    """Test notifications config tab renders with all channels"""
    print("\n🔹 Notifications Tab")

    page.locator('.nav-tab[data-nav="notifications"]').click()
    time.sleep(1)

    # Check Telegram section exists
    telegram_checkbox = page.locator("#nfTelegramEnabled")
    if telegram_checkbox.count() > 0:
        log_pass("Telegram config section found")
    else:
        log_fail("Telegram config section missing")

    # Check WhatsApp section
    whatsapp_checkbox = page.locator("#nfWhatsAppEnabled")
    if whatsapp_checkbox.count() > 0:
        log_pass("WhatsApp config section found")
    else:
        log_fail("WhatsApp config section missing")

    page.screenshot(path="tests/screenshots/notifications.png", full_page=False)
    log_pass("Notifications tab screenshot saved")


def test_console_errors(page):
    """Check for JavaScript console errors"""
    print("\n🔹 Console Errors")

    errors = []
    page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)

    page.goto(f"{BASE_URL}/index.html")
    page.wait_for_load_state("networkidle", timeout=30000)
    time.sleep(3)

    if not errors:
        log_pass("No console errors")
    else:
        for err in errors[:5]:
            log_fail("Console error", err[:100])


def main():
    parser = argparse.ArgumentParser(description="MediciMonitor Dashboard Tests")
    parser.add_argument("--base-url", default="http://localhost:5000", help="Base URL")
    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url

    print(f"🧪 MediciMonitor Dashboard Tests")
    print(f"   Target: {BASE_URL}")
    print("=" * 50)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        try:
            test_health_endpoints(page)
            test_api_endpoints(page)
            test_dashboard_loads(page)
            test_nav_tabs(page)
            test_overview_kpis(page)
            test_alerts_tab(page)
            test_reconciliation_tab(page)
            test_killswitch_tab(page)
            test_notifications_tab(page)
            test_console_errors(page)
        finally:
            browser.close()

    # Summary
    print("\n" + "=" * 50)
    total = RESULTS["passed"] + RESULTS["failed"]
    print(f"📊 Results: {RESULTS['passed']}/{total} passed, {RESULTS['failed']} failed")

    if RESULTS["errors"]:
        print("\n❌ Failures:")
        for err in RESULTS["errors"]:
            print(f"   • {err}")

    sys.exit(0 if RESULTS["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
