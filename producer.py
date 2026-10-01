"""
Sample e-commerce event producer for Kafka.

Generates *related* events for six topics (orders, catalogue, inventory,
shipments, payments, customer service). IDs are drawn from shared pools, so a
payment/shipment/ticket always refers to an order that really exists, which
makes downstream joins possible.

Usage:
    python producer.py                      # 200 events, 0.1-0.6s apart
    python producer.py --events 1000
    python producer.py --forever --min-delay 0.05 --max-delay 0.3
    python producer.py --dry-run --events 20   # no Kafka needed, just prints
"""
import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

import config

CATEGORIES = ["Electronics", "Fashion", "Home", "Grocery"]
CARRIERS = ["BlueDart", "Delhivery", "Ekart"]
PAY_METHODS = ["UPI", "CARD", "NETBANKING"]
WAREHOUSES = ["WH-DEL-01", "WH-DEL-02", "WH-MUM-01", "WH-BLR-01", "WH-KOL-01"]
REASONS = ["DAMAGED_ITEM", "LATE_DELIVERY", "WRONG_ITEM", "REFUND_REQUEST"]


def now_dt():
    return datetime.now(timezone.utc)


def iso(dt=None):
    return (dt or now_dt()).strftime("%Y-%m-%dT%H:%M:%SZ")


class World:
    """Holds the shared state so generated events stay consistent."""

    def __init__(self, n_products=60, n_customers=200):
        self.customers = [f"CUST-{10000 + i}" for i in range(n_customers)]
        self.products = {
            f"PROD-{1000 + i}": {
                "category": random.choice(CATEGORIES),
                "price": round(random.uniform(199, 4999), 2),
                "stock": {wh: random.randint(50, 500) for wh in WAREHOUSES},
            }
            for i in range(n_products)
        }
        self.orders = {}       # order_id -> dict(state)
        self.shipments = {}    # order_id -> shipment_id
        self.order_seq = 80000
        self.ship_seq = 70000
        self.txn_seq = 990000
        self.ticket_seq = 4000

    # ------------------------------------------------------------ ORDERS
    def order_event(self):
        # 75% new order, 25% update/cancel of an existing active order
        active = [o for o, d in self.orders.items() if d["status"] in ("PLACED", "CONFIRMED")]
        if active and random.random() < 0.25:
            oid = random.choice(active)
            o = self.orders[oid]
            if random.random() < 0.2:
                o["status"], etype = "CANCELLED", "ORDER_CANCELLED"
            else:
                o["status"], etype = "CONFIRMED", "ORDER_UPDATED"
        else:
            self.order_seq += 1
            oid = f"ORD-{self.order_seq}"
            pid = random.choice(list(self.products))
            o = self.orders[oid] = {
                "customer_id": random.choice(self.customers),
                "product_id": pid,
                "quantity": random.randint(1, 5),
                "unit_price": self.products[pid]["price"],
                "status": "PLACED",
                "paid": False,
            }
            etype = "ORDER_CREATED"
        return oid, {
            "event_type": etype,
            "order_id": oid,
            "customer_id": o["customer_id"],
            "product_id": o["product_id"],
            "quantity": o["quantity"],
            "unit_price": o["unit_price"],
            "currency": "INR",
            "status": o["status"],
            "timestamp": iso(),
        }

    # --------------------------------------------------------- CATALOGUE
    def catalogue_event(self):
        pid = random.choice(list(self.products))
        p = self.products[pid]
        p["price"] = round(max(99, p["price"] * random.uniform(0.9, 1.1)), 2)
        return pid, {
            "event_type": "PRICE_UPDATE",
            "product_id": pid,
            "category": p["category"],
            "price": p["price"],
            "discount_pct": random.choice([0, 5, 10, 15, 20]),
            "currency": "INR",
            "timestamp": iso(),
        }

    # --------------------------------------------------------- INVENTORY
    def inventory_event(self):
        # Prefer a product that was actually ordered so stock moves with orders
        if self.orders and random.random() < 0.6:
            o = self.orders[random.choice(list(self.orders))]
            pid, change = o["product_id"], -o["quantity"]
        else:
            pid, change = random.choice(list(self.products)), random.randint(10, 100)
        wh = random.choice(WAREHOUSES)
        stock = self.products[pid]["stock"]
        stock[wh] = max(0, stock[wh] + change)
        return f"{wh}:{pid}", {
            "event_type": "STOCK_UPDATE",
            "warehouse_id": wh,
            "product_id": pid,
            "stock_qty": stock[wh],
            "change": change,
            "timestamp": iso(),
        }

    # ----------------------------------------------------------- PAYMENT
    def payment_event(self):
        if not self.orders:
            return self.order_event()  # nothing to pay for yet
        oid = random.choice(list(self.orders))
        o = self.orders[oid]
        self.txn_seq += 1
        if o["paid"] and random.random() < 0.15:
            etype, status = "REFUND", "SUCCESS"
        elif random.random() < 0.12:
            etype, status = "PAYMENT_FAILED", "FAILED"
        else:
            etype, status = "PAYMENT_SUCCESS", "SUCCESS"
            o["paid"] = True
        return oid, {
            "event_type": etype,
            "transaction_id": f"TXN-{self.txn_seq}",
            "order_id": oid,
            "amount": round(o["quantity"] * o["unit_price"], 2),
            "currency": "INR",
            "method": random.choice(PAY_METHODS),
            "status": status,
            "timestamp": iso(),
        }

    # ---------------------------------------------------------- SHIPMENT
    def shipment_event(self):
        paid = [o for o, d in self.orders.items() if d["paid"] and d["status"] != "CANCELLED"]
        if not paid:
            return self.payment_event()
        oid = random.choice(paid)
        if oid not in self.shipments:
            self.ship_seq += 1
            self.shipments[oid] = f"SHP-{self.ship_seq}"
            etype, status = "SHIPMENT_DISPATCHED", "IN_TRANSIT"
        else:
            etype = random.choice(["IN_TRANSIT", "DELIVERED", "DELAYED"])
            status = "IN_TRANSIT" if etype == "IN_TRANSIT" else etype
        return oid, {
            "event_type": etype,
            "shipment_id": self.shipments[oid],
            "order_id": oid,
            "carrier": random.choice(CARRIERS),
            "status": status,
            "eta": iso(now_dt() + timedelta(days=random.randint(1, 5))),
            "timestamp": iso(),
        }

    # ---------------------------------------------------- CUSTOMER SERVICE
    def service_event(self):
        if not self.orders:
            return self.order_event()
        oid = random.choice(list(self.orders))
        self.ticket_seq += 1
        etype = random.choice(["TICKET_CREATED", "TICKET_UPDATED", "TICKET_CLOSED"])
        status = {"TICKET_CREATED": "OPEN",
                  "TICKET_UPDATED": "IN_PROGRESS",
                  "TICKET_CLOSED": "RESOLVED"}[etype]
        tid = f"TCK-{self.ticket_seq}"
        return tid, {
            "event_type": etype,
            "ticket_id": tid,
            "order_id": oid,
            "reason": random.choice(REASONS),
            "status": status,
            "timestamp": iso(),
        }


def build_generators(world):
    # (topic, generator, weight) -> orders most frequent, tickets least
    return [
        ("order-events", world.order_event, 30),
        ("payment-events", world.payment_event, 22),
        ("inventory-events", world.inventory_event, 18),
        ("shipment-events", world.shipment_event, 14),
        ("catalogue-events", world.catalogue_event, 10),
        ("customer-service-events", world.service_event, 6),
    ]


def make_producer(bootstrap):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=5,
    )


def main():
    ap = argparse.ArgumentParser(description="E-commerce Kafka event producer")
    ap.add_argument("--events", type=int, default=200, help="number of events to send")
    ap.add_argument("--forever", action="store_true", help="run until Ctrl+C")
    ap.add_argument("--min-delay", type=float, default=0.1)
    ap.add_argument("--max-delay", type=float, default=0.6)
    ap.add_argument("--bootstrap", nargs="+", default=config.BOOTSTRAP_SERVERS)
    ap.add_argument("--seed", type=int, default=None, help="reproducible data")
    ap.add_argument("--dry-run", action="store_true", help="print only, no Kafka")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    world = World()
    gens = build_generators(world)
    topics, fns, weights = zip(*gens)

    producer = None if args.dry_run else make_producer(args.bootstrap)
    counts = {t: 0 for t in config.TOPICS}
    sent = 0

    def on_error(exc):
        print(f"!! delivery failed: {exc}")

    try:
        while args.forever or sent < args.events:
            idx = random.choices(range(len(topics)), weights=weights)[0]
            topic, (key, event) = topics[idx], fns[idx]()
            if producer:
                producer.send(topic, key=key, value=event).add_errback(on_error)
            counts[topic] += 1
            sent += 1
            print(f"-> {topic:<24} key={key:<18} {json.dumps(event)}")
            time.sleep(random.uniform(args.min_delay, args.max_delay))
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
        print(f"\nSent {sent} events:")
        for t, c in counts.items():
            print(f"  {t:<24} {c}")


if __name__ == "__main__":
    main()
