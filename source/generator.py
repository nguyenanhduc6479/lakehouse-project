"""Continuously simulate shop activity in source-db (schema `shop`) to feed the CDC pipeline.

Every tick picks one weighted action — create an order (one transaction: order + 1-5 lines +
total_amount update), move an order's status forward, create / update / lock a user, or delete an
order (cascades to its lines) — so Debezium sees inserts, updates and deletes on all three tables.

Connection comes from the standard libpq env vars (PGHOST, PGPORT, PGUSER, PGPASSWORD,
PGDATABASE), set by the `generator` service in compose/source.yml. Run by hand:
    docker compose --profile generator up -d generator     # start (container)
    docker compose stop generator                          # stop
or directly on the host (repo root, WSL; source-db is published on localhost:5433):
    set -a && . ./.env && set +a
    PGHOST=localhost PGPORT=5433 PGUSER="$SOURCE_POSTGRES_USER" \
    PGPASSWORD="$SOURCE_POSTGRES_PASSWORD" PGDATABASE="$SOURCE_POSTGRES_DB" \
    venv/bin/python source/generator.py --rate 2           # Ctrl+C to stop
Options: --rate, --max-events, --seed-users, --seed, --log-level (see --help); full guide in
docs/services.md ("Running the generator").
Pure helpers (line totals, status transitions, order lines, emails) have no DB access and are
unit-tested in tests/test_generator.py.
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import time
import unicodedata
from decimal import ROUND_HALF_UP, Decimal

import psycopg
from faker import Faker
from psycopg import errors

logger = logging.getLogger("generator")

CITIES = [
    "Hà Nội", "TP. Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ",
    "Huế", "Nha Trang", "Biên Hòa", "Vũng Tàu", "Quy Nhơn",
]  # fmt: skip

PAYMENT_METHODS = ["cod", "card", "bank_transfer", "e_wallet"]
PAYMENT_WEIGHTS = [35, 25, 20, 20]

# (sku, product_name, category, unit_price in VND)
PRODUCTS = [
    ("SKU-PHN-001", "Smartphone A15", "electronics", Decimal("4990000")),
    ("SKU-PHN-002", "Smartphone X Pro", "electronics", Decimal("18990000")),
    ("SKU-LAP-001", "Laptop Air 13", "electronics", Decimal("21490000")),
    ("SKU-ACC-001", "Wireless Mouse", "accessories", Decimal("249000")),
    ("SKU-ACC-002", "Mechanical Keyboard", "accessories", Decimal("1290000")),
    ("SKU-ACC-003", "USB-C Charger 65W", "accessories", Decimal("590000")),
    ("SKU-ACC-004", "Bluetooth Earbuds", "accessories", Decimal("890000")),
    ("SKU-HOM-001", "Rice Cooker 1.8L", "home", Decimal("1150000")),
    ("SKU-HOM-002", "Electric Kettle", "home", Decimal("390000")),
    ("SKU-HOM-003", "Air Fryer 5L", "home", Decimal("2190000")),
    ("SKU-FAS-001", "Cotton T-Shirt", "fashion", Decimal("199000")),
    ("SKU-FAS-002", "Running Shoes", "fashion", Decimal("1490000")),
    ("SKU-FAS-003", "Backpack 20L", "fashion", Decimal("650000")),
    ("SKU-BOK-001", "Data Engineering Book", "books", Decimal("320000")),
    ("SKU-BOK-002", "Vietnamese Cookbook", "books", Decimal("180000")),
    ("SKU-GRO-001", "Coffee Beans 500g", "grocery", Decimal("210000")),
    ("SKU-GRO-002", "Green Tea 100 bags", "grocery", Decimal("95000")),
]

# Forward-only order lifecycle, matching the CHECK constraint on shop.orders.status.
# status -> [(next status, weight)]; statuses missing here are final.
TRANSITIONS: dict[str, list[tuple[str, int]]] = {
    "pending": [("paid", 8), ("cancelled", 2)],
    "paid": [("shipped", 9), ("refunded", 1)],
    "shipped": [("completed", 1)],
    "completed": [("refunded", 1)],
}
# Which status to pick an order from when advancing: mostly open orders, rarely a refund.
ADVANCE_FROM = ["pending", "paid", "shipped", "completed"]
ADVANCE_FROM_WEIGHTS = [40, 30, 27, 3]

# action -> weight per tick
ACTIONS = {
    "create_order": 45,
    "advance_order": 30,
    "update_user": 10,
    "create_user": 8,
    "delete_order": 4,
    "toggle_user_lock": 3,
}

MONEY = Decimal("0.01")


# ---------------------------------------------------------------------------- pure helpers


def line_total(quantity: int, unit_price: Decimal, discount_amount: Decimal) -> Decimal:
    """quantity * unit_price - discount_amount, rounded to cents, never negative."""
    total = Decimal(quantity) * unit_price - discount_amount
    return max(total, Decimal(0)).quantize(MONEY, rounding=ROUND_HALF_UP)


def next_status(status: str, rng: random.Random) -> str | None:
    """Next status of an order, or None when `status` is final."""
    options = TRANSITIONS.get(status)
    if not options:
        return None
    statuses = [s for s, _ in options]
    weights = [w for _, w in options]
    return rng.choices(statuses, weights=weights)[0]


def build_order_lines(rng: random.Random) -> list[dict]:
    """1-5 lines with distinct products, numbered from 1; ~30% of lines get a 5-20% discount."""
    products = rng.sample(PRODUCTS, k=rng.randint(1, 5))
    lines = []
    for number, (sku, name, category, price) in enumerate(products, start=1):
        quantity = rng.choices([1, 2, 3, 4], weights=[70, 20, 7, 3])[0]
        discount = Decimal(0)
        if rng.random() < 0.3:
            pct = Decimal(rng.choice([5, 10, 15, 20])) / 100
            # round to 1,000 VND like a real shop
            discount = (Decimal(quantity) * price * pct / 1000).quantize(Decimal(1)) * 1000
        lines.append(
            {
                "line_number": number,
                "sku": sku,
                "product_name": name,
                "category": category,
                "quantity": quantity,
                "unit_price": price,
                "discount_amount": discount.quantize(MONEY),
                "line_total": line_total(quantity, price, discount),
            }
        )
    return lines


def ascii_slug(text: str) -> str:
    """'Nguyễn Văn Đức' -> 'nguyen.van.duc' (lowercase ASCII, dots between words)."""
    text = text.replace("đ", "d").replace("Đ", "D")
    plain = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    words = ["".join(ch for ch in w if ch.isalnum()) for w in plain.lower().split()]
    return ".".join(w for w in words if w) or "user"


def make_email(full_name: str, rng: random.Random) -> str:
    """Unique-enough email from the name plus a random 4-digit suffix."""
    return f"{ascii_slug(full_name)}.{rng.randint(1000, 9999)}@example.com"


def make_phone(rng: random.Random) -> str:
    """Vietnamese mobile number, 10 digits."""
    return rng.choice(["090", "091", "093", "097", "098", "086", "088"]) + "".join(
        str(rng.randint(0, 9)) for _ in range(7)
    )


# ---------------------------------------------------------------------------- DB actions


class Generator:
    def __init__(self, conn: psycopg.Connection, rng: random.Random, fake: Faker) -> None:
        self.conn = conn
        self.rng = rng
        self.fake = fake

    def create_user(self) -> str:
        # Faker 40.44 vi_VN: first_name() only ever returns "John"/"Jane";
        # first_name_male()/first_name_female() return Vietnamese names.
        if self.rng.random() < 0.5:
            first_name = self.fake.first_name_male()
        else:
            first_name = self.fake.first_name_female()
        full_name = f"{self.fake.last_name()} {self.fake.middle_name()} {first_name}"
        for _ in range(5):  # retry on the (unlikely) email collision
            try:
                with self.conn.transaction():
                    user_id = self.conn.execute(
                        "INSERT INTO shop.users (email, full_name, phone, city) "
                        "VALUES (%s, %s, %s, %s) RETURNING user_id",
                        (make_email(full_name, self.rng), full_name, make_phone(self.rng),
                         self.rng.choice(CITIES)),
                    ).fetchone()[0]  # fmt: skip
                return f"user {user_id} created ({full_name})"
            except errors.UniqueViolation:
                continue
        return "user not created (email collisions)"

    def create_order(self) -> str:
        with self.conn.transaction():
            user = self.conn.execute(
                "SELECT user_id, city FROM shop.users WHERE status = 'active' "
                "ORDER BY random() LIMIT 1"
            ).fetchone()
            if user is None:
                return self.create_user()
            user_id, city = user
            shipping_city = city if self.rng.random() < 0.8 else self.rng.choice(CITIES)
            payment = self.rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
            order_id = self.conn.execute(
                "INSERT INTO shop.orders (user_id, payment_method, shipping_city) "
                "VALUES (%s, %s, %s) RETURNING order_id",
                (user_id, payment, shipping_city),
            ).fetchone()[0]
            lines = build_order_lines(self.rng)
            with self.conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO shop.order_details (order_id, line_number, sku, product_name, "
                    "category, quantity, unit_price, discount_amount, line_total) VALUES "
                    "(%(order_id)s, %(line_number)s, %(sku)s, %(product_name)s, %(category)s, "
                    "%(quantity)s, %(unit_price)s, %(discount_amount)s, %(line_total)s)",
                    [{**line, "order_id": order_id} for line in lines],
                )
            total = sum((line["line_total"] for line in lines), Decimal(0))
            # same transaction: insert + lines + total update arrive together (design §3.3)
            self.conn.execute(
                "UPDATE shop.orders SET total_amount = %s WHERE order_id = %s", (total, order_id)
            )
        return f"order {order_id} created for user {user_id}: {len(lines)} lines, {total:,.0f} VND"

    def advance_order(self) -> str:
        status = self.rng.choices(ADVANCE_FROM, weights=ADVANCE_FROM_WEIGHTS)[0]
        with self.conn.transaction():
            row = self.conn.execute(
                "SELECT order_id FROM shop.orders WHERE status = %s "
                "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED",
                (status,),
            ).fetchone()
            if row is None:
                return f"no {status} order to advance"
            new_status = next_status(status, self.rng)
            self.conn.execute(
                "UPDATE shop.orders SET status = %s WHERE order_id = %s", (new_status, row[0])
            )
        return f"order {row[0]} {status} -> {new_status}"

    def update_user(self) -> str:
        with self.conn.transaction():
            row = self.conn.execute(
                "SELECT user_id FROM shop.users ORDER BY random() LIMIT 1"
            ).fetchone()
            if row is None:
                return "no user to update"
            if self.rng.random() < 0.5:
                field, value = "city", self.rng.choice(CITIES)
            else:
                field, value = "phone", make_phone(self.rng)
            self.conn.execute(
                f"UPDATE shop.users SET {field} = %s WHERE user_id = %s",  # field is a constant
                (value, row[0]),
            )
        return f"user {row[0]} {field} -> {value}"

    def toggle_user_lock(self) -> str:
        with self.conn.transaction():
            row = self.conn.execute(
                "SELECT user_id, status FROM shop.users WHERE status IN ('active', 'locked') "
                "ORDER BY random() LIMIT 1"
            ).fetchone()
            if row is None:
                return "no user to lock/unlock"
            new_status = "locked" if row[1] == "active" else "active"
            self.conn.execute(
                "UPDATE shop.users SET status = %s WHERE user_id = %s", (new_status, row[0])
            )
        return f"user {row[0]} {row[1]} -> {new_status}"

    def delete_order(self) -> str:
        with self.conn.transaction():
            row = self.conn.execute(
                "DELETE FROM shop.orders WHERE order_id = "
                "(SELECT order_id FROM shop.orders ORDER BY random() LIMIT 1) "
                "RETURNING order_id, (SELECT count(*) FROM shop.order_details d "
                "WHERE d.order_id = shop.orders.order_id)"
            ).fetchone()
        if row is None:
            return "no order to delete"
        return f"order {row[0]} deleted (cascade: {row[1]} lines)"

    def seed_users(self, minimum: int) -> None:
        count = self.conn.execute("SELECT count(*) FROM shop.users").fetchone()[0]
        for _ in range(max(0, minimum - count)):
            self.create_user()
        if count < minimum:
            logger.info("seeded %d users (had %d)", minimum - count, count)

    def tick(self) -> str:
        action = self.rng.choices(list(ACTIONS), weights=list(ACTIONS.values()))[0]
        return getattr(self, action)()


# ---------------------------------------------------------------------------- main loop

_stop = False


def _request_stop(signum: int, _frame: object) -> None:
    # As PID 1 in a container, Python ignores SIGTERM unless a handler is installed, so
    # `docker stop` would wait 10 s and SIGKILL us. Finish the current tick and exit instead.
    global _stop
    _stop = True
    logger.info("signal %d received, stopping after the current event", signum)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rate", type=float, default=2.0, help="events per second (default 2)")
    parser.add_argument(
        "--seed-users", type=int, default=20, help="create users until at least this many exist"
    )
    parser.add_argument(
        "--max-events", type=int, default=0, help="stop after N events (0 = run forever)"
    )
    parser.add_argument("--seed", type=int, default=None, help="random seed (reproducible runs)")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING (default INFO)")
    args = parser.parse_args(argv)
    if args.rate <= 0:
        parser.error("--rate must be > 0")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    rng = random.Random(args.seed)
    fake = Faker("vi_VN")
    if args.seed is not None:
        fake.seed_instance(args.seed)
    interval = 1.0 / args.rate
    events = 0
    logger.info("starting: %.2f events/s, seed users %d", args.rate, args.seed_users)

    while not _stop:
        try:
            # Empty conninfo: everything comes from the PG* environment variables.
            # autocommit=True is essential: without it the first statement outside a
            # `with conn.transaction()` block opens an implicit transaction that is never
            # committed, every later transaction() becomes a mere SAVEPOINT inside it, and nothing
            # is visible to other sessions (or to Debezium) until the generator stops.
            with psycopg.connect("", autocommit=True, application_name="generator") as conn:
                gen = Generator(conn, rng, fake)
                gen.seed_users(args.seed_users)
                next_at = time.monotonic()
                while not _stop:
                    logger.info(gen.tick())
                    events += 1
                    if args.max_events and events >= args.max_events:
                        logger.info("reached --max-events %d, stopping", args.max_events)
                        return 0
                    next_at += interval
                    time.sleep(max(0.0, next_at - time.monotonic()))
        except psycopg.OperationalError as exc:
            if _stop:
                break
            logger.warning("database unavailable (%s), retrying in 5 s", str(exc).strip())
            time.sleep(5)
    logger.info("stopped after %d events", events)
    return 0


if __name__ == "__main__":
    sys.exit(main())
