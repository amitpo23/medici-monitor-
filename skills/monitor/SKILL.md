# System Monitor Skill

## Purpose
Full monitoring of the Medici booking engine - WebJobs, tables, mapping, skills, Zenith, and auto-cancellation. Provides health checks, alerts, and diagnostics.

## Commands

```bash
# Full system report
python system_monitor.py --connection-string "..." --full

# Alerts only (quick check)
python system_monitor.py --connection-string "..." --alert-only

# Specific check
python system_monitor.py --connection-string "..." --check webjob
python system_monitor.py --connection-string "..." --check tables
python system_monitor.py --connection-string "..." --check mapping
python system_monitor.py --connection-string "..." --check skills
python system_monitor.py --connection-string "..." --check orders
python system_monitor.py --connection-string "..." --check zenith
python system_monitor.py --connection-string "..." --check cancellation

# JSON output (for automation)
python system_monitor.py --connection-string "..." --full --json
```

## What It Monitors

### 1. WebJob Health
- Last activity time (alert if > 30 min stale)
- In Progress orders
- Pending / Failed orders count
- Scan cycle time estimate

### 2. Table Health
- Row counts for all SalesOffice tables
- Active vs total for Orders, Details, PriceOverride
- MappingMisses status
- Archive tables

### 3. Mapping Quality
- Hotels with BB coverage
- Open mapping misses (count + rate per hour)
- ORDER = DETAIL verification per hotel
- Gap detection

### 4. Skills Health
- autofix_worker last run time
- PriceOverride: pending/pushed/failed counts
- InsertOpp: recent activity + active count

### 5. Orders & Details
- Active hotels and orders count
- Status breakdown (null/completed/in-progress/failed)
- RO vs BB details count

### 6. Zenith API
- Connectivity probe (SOAP request)
- Response latency
- HTTP status

### 7. Auto-Cancellation
- Active bookings count
- Bookings near CX deadline (5 days)
- Cancellations in last 24h
- Cancellation errors in last 24h

## Alert Thresholds

| Alert | Threshold | Severity |
|-------|-----------|----------|
| WebJob stale | > 30 minutes | CRITICAL |
| Zenith unreachable | Connection failure | CRITICAL |
| Failed orders | > 0 | WARNING |
| Mapping miss rate | > 10/hour | WARNING |
| Override failure rate | > 20% | WARNING |
| Scan cycle time | > 24 hours | WARNING |
| Cancel errors | > 0 in 24h | WARNING |
| ORDER != DETAIL gaps | Any hotel | WARNING |
| Near CX deadline | > 10 bookings | INFO |
| Open mapping misses | Any | INFO |

## Output

Console report + JSON file saved to `monitor-report/`.

## Integration

```python
from system_monitor import SystemMonitor

monitor = SystemMonitor(connection_string)
monitor.run_full()

# Check alerts
if monitor.alerts:
    for alert in monitor.alerts:
        send_notification(alert)

# Get specific data
results = monitor.results
webjob_ok = results["webjob"]["last_log"]["minutes_ago"] < 30

monitor.close()
```
