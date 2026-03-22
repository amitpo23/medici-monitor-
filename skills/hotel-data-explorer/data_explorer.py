"""
Hotel Data Explorer - Full access to all hotel data, prices, scans, and logs

One skill for all data research: hotel prices, scan results, price changes,
booking logs, cancellation history, mapping status, and WebService health.

Usage:
    # Hotel info
    python data_explorer.py --hotel 22034
    python data_explorer.py --hotel "Hilton Bentley"
    python data_explorer.py --hotels                     # list all

    # Current prices
    python data_explorer.py --prices --hotel 22034
    python data_explorer.py --prices --all
    python data_explorer.py --prices --board BB --category standard

    # Price history / changes
    python data_explorer.py --price-changes --hotel 22034 --hours 24
    python data_explorer.py --price-changes --all --hours 48

    # Scan status
    python data_explorer.py --scans --hotel 22034
    python data_explorer.py --scans --all

    # Bookings (active rooms)
    python data_explorer.py --bookings
    python data_explorer.py --bookings --hotel 22034

    # Reservations (sold rooms)
    python data_explorer.py --reservations
    python data_explorer.py --reservations --hotel 22034

    # Cancellations
    python data_explorer.py --cancellations --hours 48
    python data_explorer.py --cancel-errors

    # Mapping status
    python data_explorer.py --mapping --hotel 22034
    python data_explorer.py --mapping-misses

    # Logs
    python data_explorer.py --logs --hotel 22034 --hours 24
    python data_explorer.py --logs --order 1268

    # Overrides
    python data_explorer.py --overrides

    # Custom SQL (read-only)
    python data_explorer.py --sql "SELECT TOP 10 * FROM MED_Book ORDER BY id DESC"

    # Full hotel report
    python data_explorer.py --report --hotel 22034

    # Export to CSV
    python data_explorer.py --prices --all --csv prices.csv
"""

import argparse
import csv as csv_module
import json
import os
import re
import sys
from datetime import datetime, timedelta

import pyodbc


class HotelDataExplorer:
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()

    # ── HOTEL INFO ────────────────────────────────────────────

    def get_hotel(self, hotel_id=None, hotel_name=None):
        """Get hotel details by ID or name"""
        if hotel_id:
            self.cursor.execute("""
                SELECT HotelId, [Name], Innstant_ZenithId, isActive
                FROM Med_Hotels WHERE HotelId = ?
            """, hotel_id)
        elif hotel_name:
            self.cursor.execute("""
                SELECT HotelId, [Name], Innstant_ZenithId, isActive
                FROM Med_Hotels WHERE [Name] LIKE ?
            """, f"%{hotel_name}%")
        row = self.cursor.fetchone()
        if not row:
            return None
        return {"hotel_id": row[0], "name": row[1], "venue_id": row[2], "active": row[3]}

    def list_hotels(self):
        """List all hotels with active orders"""
        self.cursor.execute("""
            SELECT h.HotelId, h.[Name], h.Innstant_ZenithId, h.isActive,
                (SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE DestinationId = CAST(h.HotelId AS NVARCHAR) AND IsActive=1) as Orders,
                (SELECT COUNT(*) FROM [SalesOffice.Details] WHERE HotelId = h.HotelId AND IsDeleted=0) as Details,
                (SELECT COUNT(*) FROM MED_Book WHERE HotelId = h.HotelId AND IsActive=1) as ActiveBookings
            FROM Med_Hotels h
            WHERE h.isActive = 1
            ORDER BY h.[Name]
        """)
        return [{"hotel_id": r[0], "name": r[1], "venue_id": r[2],
                 "orders": r[4], "details": r[5], "bookings": r[6]}
                for r in self.cursor.fetchall()]

    # ── PRICES ────────────────────────────────────────────────

    def get_prices(self, hotel_id=None, board=None, category=None):
        """Get current prices from SalesOffice.Details"""
        sql = """
            SELECT d.Id, d.HotelId, h.[Name], d.RoomCategory, d.RoomBoard,
                   d.RoomPrice, o.DateFrom, o.DateTo, d.DateCreated, d.DateUpdated
            FROM [SalesOffice.Details] d
            JOIN [SalesOffice.Orders] o ON o.Id = d.SalesOfficeOrderId
            LEFT JOIN Med_Hotels h ON h.HotelId = d.HotelId
            WHERE d.IsDeleted = 0
        """
        params = []
        if hotel_id:
            sql += " AND d.HotelId = ?"
            params.append(hotel_id)
        if board:
            sql += " AND d.RoomBoard = ?"
            params.append(board)
        if category:
            sql += " AND LOWER(d.RoomCategory) = LOWER(?)"
            params.append(category)
        sql += " ORDER BY d.HotelId, o.DateFrom, d.RoomCategory, d.RoomBoard"
        self.cursor.execute(sql, *params)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    # ── PRICE CHANGES (LOG) ──────────────────────────────────

    def get_price_changes(self, hotel_id=None, hours=24):
        """Get price change logs"""
        sql = """
            SELECT l.Id, l.SalesOfficeOrderId, l.DateCreated,
                   l.Message, o.DestinationId, h.[Name]
            FROM [SalesOffice.Log] l
            JOIN [SalesOffice.Orders] o ON o.Id = l.SalesOfficeOrderId
            LEFT JOIN Med_Hotels h ON h.HotelId = CAST(o.DestinationId AS INT)
            WHERE l.DateCreated >= DATEADD(HOUR, ?, GETDATE())
              AND l.Message LIKE '%RoomPrice%'
        """
        params = [-hours]
        if hotel_id:
            sql += " AND CAST(o.DestinationId AS INT) = ?"
            params.append(hotel_id)
        sql += " ORDER BY l.DateCreated DESC"
        self.cursor.execute(sql, *params)
        results = []
        for r in self.cursor.fetchall():
            msg = str(r[3]) if r[3] else ""
            m = re.search(r'DbRoomPrice:\s*([\d.]+)\s*->\s*API RoomPrice:\s*([\d.]+)', msg)
            old_price = float(m.group(1)) if m else None
            new_price = float(m.group(2)) if m else None
            results.append({
                "log_id": r[0], "order_id": r[1], "time": str(r[2])[:19],
                "hotel_id": r[4], "hotel": str(r[5])[:30] if r[5] else "?",
                "old_price": old_price, "new_price": new_price,
                "change": round(new_price - old_price, 2) if old_price and new_price else None,
            })
        return results

    # ── SCAN STATUS ───────────────────────────────────────────

    def get_scans(self, hotel_id=None):
        """Get scan status per hotel"""
        sql = """
            SELECT o.DestinationId, h.[Name],
                COUNT(*) as Orders,
                SUM(CASE WHEN o.WebJobStatus IS NULL THEN 1 ELSE 0 END) as Pending,
                SUM(CASE WHEN o.WebJobStatus LIKE '%Completed%' THEN 1 ELSE 0 END) as Completed,
                SUM(CASE WHEN o.WebJobStatus LIKE '%Failed%' THEN 1 ELSE 0 END) as Failed
            FROM [SalesOffice.Orders] o
            LEFT JOIN Med_Hotels h ON h.HotelId = CAST(o.DestinationId AS INT)
            WHERE o.IsActive = 1
        """
        if hotel_id:
            sql += f" AND o.DestinationId = '{hotel_id}'"
        sql += " GROUP BY o.DestinationId, h.[Name] ORDER BY h.[Name]"
        self.cursor.execute(sql)
        return [{"hotel_id": r[0], "hotel": r[1], "orders": r[2],
                 "pending": r[3], "completed": r[4], "failed": r[5]}
                for r in self.cursor.fetchall()]

    # ── BOOKINGS ──────────────────────────────────────────────

    def get_bookings(self, hotel_id=None, active_only=True):
        """Get MED_Book records"""
        sql = """
            SELECT b.id, b.PreBookId, b.HotelId, h.[Name],
                   b.startDate, b.endDate, b.price, b.lastPrice, b.DateLastPrice,
                   b.IsActive, b.IsSold, b.SoldId,
                   b.CancellationType, b.CancellationTo,
                   b.source, b.providers, b.supplierReference,
                   b.contentBookingID, b.DateInsert
            FROM MED_Book b
            LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            WHERE 1=1
        """
        params = []
        if hotel_id:
            sql += " AND b.HotelId = ?"
            params.append(hotel_id)
        if active_only:
            sql += " AND b.IsActive = 1"
        sql += " ORDER BY b.id DESC"
        self.cursor.execute(sql, *params)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    # ── RESERVATIONS ──────────────────────────────────────────

    def get_reservations(self, hotel_id=None, limit=50):
        """Get reservation records"""
        sql = """
            SELECT r.Id, r.ResStatus, r.HotelCode, r.Datefrom, r.Dateto,
                   r.AmountAfterTax, r.RatePlanCode, r.RoomTypeCode,
                   r.AdultCount, r.ChildrenCount, r.UniqueId,
                   r.IsApproved, r.IsCanceled, r.DateInsert,
                   cn.GivenName, cn.Surname
            FROM MED_Reservation r
            LEFT JOIN Med_ReservationCustomerName cn ON cn.ReservationId = r.Id AND cn.IsLeader = 1
        """
        if hotel_id:
            # Get venue ID
            hotel = self.get_hotel(hotel_id=hotel_id)
            if hotel:
                sql += f" WHERE r.HotelCode = '{hotel['venue_id']}'"
        sql += f" ORDER BY r.Id DESC OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
        self.cursor.execute(sql)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    # ── CANCELLATIONS ─────────────────────────────────────────

    def get_cancellations(self, hours=48):
        """Get recent cancellations"""
        self.cursor.execute("""
            SELECT c.Id, c.PreBookId, c.contentBookingID,
                   c.CancellationReason, c.CancellationDate, c.DateInsert,
                   b.HotelId, h.[Name], b.price, b.startDate, b.endDate
            FROM MED_CancelBook c
            LEFT JOIN MED_Book b ON b.PreBookId = c.PreBookId
            LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            WHERE c.DateInsert >= DATEADD(HOUR, ?, GETDATE())
            ORDER BY c.Id DESC
        """, -hours)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    def get_cancel_errors(self, hours=168):
        """Get cancellation errors"""
        self.cursor.execute("""
            SELECT Id, DateInsert, PreBookId, contentBookingID, Error
            FROM MED_CancelBookError
            WHERE DateInsert >= DATEADD(HOUR, ?, GETDATE())
            ORDER BY Id DESC
        """, -hours)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    # ── MAPPING ───────────────────────────────────────────────

    def get_mapping(self, hotel_id):
        """Get ratebycat mapping for a hotel"""
        self.cursor.execute("""
            SELECT r.HotelId, brd.BoardCode, cat.[Name] as Category,
                   r.RatePlanCode, r.InvTypeCode
            FROM Med_Hotels_ratebycat r
            LEFT JOIN MED_Board brd ON brd.BoardId = r.BoardId
            LEFT JOIN MED_RoomCategory cat ON cat.CategoryId = r.CategoryId
            WHERE r.HotelId = ?
            ORDER BY r.BoardId, r.CategoryId
        """, hotel_id)
        return [{"board": r[1], "category": r[2], "rpc": r[3], "itc": r[4]}
                for r in self.cursor.fetchall()]

    def get_mapping_misses(self):
        """Get open mapping misses"""
        self.cursor.execute("""
            SELECT mm.HotelId, h.[Name], mm.RoomCategory, mm.RoomBoard,
                   COUNT(*) as Hits, mm.Status, MAX(mm.SeenAt) as LastSeen
            FROM [SalesOffice.MappingMisses] mm
            LEFT JOIN Med_Hotels h ON h.HotelId = mm.HotelId
            WHERE mm.Status = 'new'
            GROUP BY mm.HotelId, h.[Name], mm.RoomCategory, mm.RoomBoard, mm.Status
            ORDER BY COUNT(*) DESC
        """)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    # ── LOGS ──────────────────────────────────────────────────

    def get_logs(self, hotel_id=None, order_id=None, hours=24, limit=100):
        """Get SalesOffice logs"""
        sql = """
            SELECT l.Id, l.SalesOfficeOrderId, l.SalesOfficeDetailId,
                   l.DateCreated, l.Message, l.ActionId, l.ActionResultId
            FROM [SalesOffice.Log] l
        """
        params = []
        conditions = []

        if order_id:
            conditions.append("l.SalesOfficeOrderId = ?")
            params.append(order_id)
        elif hotel_id:
            conditions.append("""l.SalesOfficeOrderId IN (
                SELECT Id FROM [SalesOffice.Orders] WHERE DestinationId = ?)""")
            params.append(str(hotel_id))

        conditions.append("l.DateCreated >= DATEADD(HOUR, ?, GETDATE())")
        params.append(-hours)

        sql += " WHERE " + " AND ".join(conditions)
        sql += f" ORDER BY l.Id DESC OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
        self.cursor.execute(sql, *params)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    # ── OVERRIDES ─────────────────────────────────────────────

    def get_overrides(self):
        """Get price overrides"""
        self.cursor.execute("""
            SELECT po.Id, po.DetailId, po.OriginalPrice, po.OverridePrice,
                   po.CreatedBy, po.CreatedAt, po.PushedAt, po.PushStatus, po.IsActive,
                   d.HotelId, h.[Name], d.RoomCategory, d.RoomBoard
            FROM [SalesOffice.PriceOverride] po
            LEFT JOIN [SalesOffice.Details] d ON d.Id = po.DetailId
            LEFT JOIN Med_Hotels h ON h.HotelId = d.HotelId
            ORDER BY po.Id DESC
        """)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    # ── CUSTOM SQL ────────────────────────────────────────────

    def run_sql(self, query):
        """Run read-only SQL query"""
        # Safety: block write operations
        q_upper = query.strip().upper()
        for blocked in ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "EXEC"]:
            if q_upper.startswith(blocked):
                return {"error": f"Write operations blocked. Query starts with {blocked}"}

        self.cursor.execute(query)
        cols = [desc[0] for desc in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    # ── FULL HOTEL REPORT ─────────────────────────────────────

    def hotel_report(self, hotel_id):
        """Complete report for a single hotel"""
        hotel = self.get_hotel(hotel_id=hotel_id)
        if not hotel:
            return {"error": f"Hotel {hotel_id} not found"}

        return {
            "hotel": hotel,
            "mapping": self.get_mapping(hotel_id),
            "scans": self.get_scans(hotel_id),
            "prices": self.get_prices(hotel_id),
            "price_changes_24h": self.get_price_changes(hotel_id, hours=24),
            "bookings": self.get_bookings(hotel_id),
            "reservations": self.get_reservations(hotel_id, limit=10),
            "logs_24h": self.get_logs(hotel_id=hotel_id, hours=24, limit=20),
        }

    # ── OUTPUT ────────────────────────────────────────────────

    def print_results(self, data, title="Results"):
        """Print formatted results"""
        if isinstance(data, dict) and "error" in data:
            print(f"ERROR: {data['error']}")
            return

        if isinstance(data, list):
            if not data:
                print(f"{title}: No results")
                return
            print(f"{title}: {len(data)} rows")
            # Print as table
            if data:
                cols = list(data[0].keys())
                # Truncate wide columns
                for row in data:
                    for k, v in row.items():
                        if isinstance(v, str) and len(v) > 50:
                            row[k] = v[:47] + "..."
                        if isinstance(v, datetime):
                            row[k] = str(v)[:19]
                # Print header
                header = " | ".join(f"{c[:15]:>15}" for c in cols)
                print(header)
                print("-" * len(header))
                for row in data[:50]:
                    line = " | ".join(f"{str(row.get(c, ''))[:15]:>15}" for c in cols)
                    print(line)
                if len(data) > 50:
                    print(f"... and {len(data) - 50} more rows")
        elif isinstance(data, dict):
            print(json.dumps(data, indent=2, default=str))

    def export_csv(self, data, path):
        """Export results to CSV"""
        if not data or not isinstance(data, list):
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv_module.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Exported {len(data)} rows to {path}")

    def close(self):
        self.conn.close()


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Hotel Data Explorer")
    parser.add_argument("--connection-string", required=True)

    # What to query
    parser.add_argument("--hotel", help="Hotel ID or name")
    parser.add_argument("--hotels", action="store_true", help="List all hotels")
    parser.add_argument("--prices", action="store_true", help="Current prices")
    parser.add_argument("--price-changes", action="store_true", help="Price change history")
    parser.add_argument("--scans", action="store_true", help="Scan status")
    parser.add_argument("--bookings", action="store_true", help="Active bookings")
    parser.add_argument("--reservations", action="store_true", help="Reservations")
    parser.add_argument("--cancellations", action="store_true", help="Recent cancellations")
    parser.add_argument("--cancel-errors", action="store_true", help="Cancellation errors")
    parser.add_argument("--mapping", action="store_true", help="Mapping status")
    parser.add_argument("--mapping-misses", action="store_true", help="Open mapping misses")
    parser.add_argument("--logs", action="store_true", help="SalesOffice logs")
    parser.add_argument("--overrides", action="store_true", help="Price overrides")
    parser.add_argument("--report", action="store_true", help="Full hotel report")
    parser.add_argument("--sql", help="Custom read-only SQL query")

    # Filters
    parser.add_argument("--all", action="store_true", help="All hotels")
    parser.add_argument("--board", help="Filter by board (RO/BB)")
    parser.add_argument("--category", help="Filter by category")
    parser.add_argument("--hours", type=int, default=24, help="Hours to look back")
    parser.add_argument("--order", type=int, help="Order ID for logs")

    # Output
    parser.add_argument("--csv", help="Export to CSV file")
    parser.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()
    explorer = HotelDataExplorer(args.connection_string)

    try:
        # Resolve hotel ID
        hotel_id = None
        if args.hotel:
            try:
                hotel_id = int(args.hotel)
            except ValueError:
                hotel = explorer.get_hotel(hotel_name=args.hotel)
                if hotel:
                    hotel_id = hotel["hotel_id"]
                    print(f"Found: {hotel['name']} (ID={hotel_id})")
                else:
                    print(f"Hotel not found: {args.hotel}")
                    return

        # Execute query
        data = None
        title = "Results"

        if args.hotels:
            data = explorer.list_hotels()
            title = "Hotels"
        elif args.prices:
            data = explorer.get_prices(hotel_id, args.board, args.category)
            title = "Prices"
        elif args.price_changes:
            data = explorer.get_price_changes(hotel_id, args.hours)
            title = f"Price Changes (last {args.hours}h)"
        elif args.scans:
            data = explorer.get_scans(hotel_id)
            title = "Scan Status"
        elif args.bookings:
            data = explorer.get_bookings(hotel_id)
            title = "Active Bookings"
        elif args.reservations:
            data = explorer.get_reservations(hotel_id)
            title = "Reservations"
        elif args.cancellations:
            data = explorer.get_cancellations(args.hours)
            title = f"Cancellations (last {args.hours}h)"
        elif args.cancel_errors:
            data = explorer.get_cancel_errors()
            title = "Cancel Errors"
        elif args.mapping and hotel_id:
            data = explorer.get_mapping(hotel_id)
            title = "Mapping"
        elif args.mapping_misses:
            data = explorer.get_mapping_misses()
            title = "Mapping Misses"
        elif args.logs:
            data = explorer.get_logs(hotel_id=hotel_id, order_id=args.order, hours=args.hours)
            title = "Logs"
        elif args.overrides:
            data = explorer.get_overrides()
            title = "Price Overrides"
        elif args.report and hotel_id:
            data = explorer.hotel_report(hotel_id)
            title = f"Full Report: Hotel {hotel_id}"
        elif args.sql:
            data = explorer.run_sql(args.sql)
            title = "SQL Query"
        else:
            parser.print_help()
            return

        # Output
        if args.csv and isinstance(data, list):
            explorer.export_csv(data, args.csv)
        elif args.json:
            print(json.dumps(data, indent=2, default=str))
        else:
            explorer.print_results(data, title)

    finally:
        explorer.close()


if __name__ == "__main__":
    main()
