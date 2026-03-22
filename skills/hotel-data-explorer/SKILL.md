# Hotel Data Explorer Skill

## Purpose
Single skill for full access to all hotel data: prices, scans, price changes, bookings, reservations, cancellations, mapping, logs, and overrides. Read-only access to all tables in medici-db.

## Commands

### Hotel Info
```bash
python data_explorer.py --hotel 22034              # by ID
python data_explorer.py --hotel "Hilton Bentley"   # by name
python data_explorer.py --hotels                   # list all
```

### Current Prices
```bash
python data_explorer.py --prices --hotel 22034
python data_explorer.py --prices --all
python data_explorer.py --prices --board BB
python data_explorer.py --prices --category standard
python data_explorer.py --prices --all --csv all_prices.csv
```

### Price Changes
```bash
python data_explorer.py --price-changes --hotel 22034 --hours 24
python data_explorer.py --price-changes --all --hours 48
```

### Scan Status
```bash
python data_explorer.py --scans --hotel 22034
python data_explorer.py --scans --all
```

### Bookings (Active Rooms)
```bash
python data_explorer.py --bookings
python data_explorer.py --bookings --hotel 22034
```

### Reservations (Sold Rooms)
```bash
python data_explorer.py --reservations
python data_explorer.py --reservations --hotel 22034
```

### Cancellations
```bash
python data_explorer.py --cancellations --hours 48
python data_explorer.py --cancel-errors
```

### Mapping
```bash
python data_explorer.py --mapping --hotel 22034
python data_explorer.py --mapping-misses
```

### Logs
```bash
python data_explorer.py --logs --hotel 22034 --hours 24
python data_explorer.py --logs --order 1268
```

### Price Overrides
```bash
python data_explorer.py --overrides
```

### Custom SQL (read-only)
```bash
python data_explorer.py --sql "SELECT TOP 10 * FROM MED_Book ORDER BY id DESC"
python data_explorer.py --sql "SELECT COUNT(*) FROM [SalesOffice.Details] WHERE IsDeleted=0"
```

### Full Hotel Report
```bash
python data_explorer.py --report --hotel 22034
python data_explorer.py --report --hotel 22034 --json
```

### Export to CSV
```bash
python data_explorer.py --prices --all --csv prices.csv
python data_explorer.py --bookings --csv bookings.csv
python data_explorer.py --price-changes --hours 48 --csv changes.csv
```

## Data Sources

| Command | Table(s) |
|---------|----------|
| `--hotels` | Med_Hotels + SalesOffice.Orders + Details + MED_Book |
| `--prices` | SalesOffice.Details + Orders |
| `--price-changes` | SalesOffice.Log (Message LIKE '%RoomPrice%') |
| `--scans` | SalesOffice.Orders (WebJobStatus) |
| `--bookings` | MED_Book |
| `--reservations` | MED_Reservation + Med_ReservationCustomerName |
| `--cancellations` | MED_CancelBook + MED_Book |
| `--cancel-errors` | MED_CancelBookError |
| `--mapping` | Med_Hotels_ratebycat + MED_Board + MED_RoomCategory |
| `--mapping-misses` | SalesOffice.MappingMisses |
| `--logs` | SalesOffice.Log |
| `--overrides` | SalesOffice.PriceOverride + Details |
| `--report` | All of the above for one hotel |

## Safety

- **Read-only:** INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, EXEC are blocked in `--sql`
- **No writes** to any table
- **No Zenith pushes** - data viewing only

## Integration with Price Predictor

```python
from data_explorer import HotelDataExplorer

explorer = HotelDataExplorer(connection_string)

# Get all current prices
prices = explorer.get_prices()

# Get price changes in last 24h
changes = explorer.get_price_changes(hours=24)

# Full hotel data for ML model
report = explorer.hotel_report(hotel_id=22034)

# Custom query
data = explorer.run_sql("SELECT * FROM MED_Book WHERE HotelId=22034 AND IsActive=1")

explorer.close()
```
