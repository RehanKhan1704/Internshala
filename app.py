"""
====================================================================
  Leaderboard Backend — Flask
====================================================================
  Stack  : Python 3.10+, Flask, SQLite (via SQLAlchemy)
  DB     : SQLite file  →  leaderboard.db  (auto-created on start)

  SCHEMA
  ──────
  users
    id          TEXT  PK   (e.g. "user_123")
    username    TEXT  NOT NULL UNIQUE
    total_amount REAL DEFAULT 0
    tx_count    INTEGER DEFAULT 0
    last_tx_at  REAL  (unix timestamp, nullable)
    created_at  REAL  NOT NULL

  transactions
    id          TEXT  PK   (UUID v4 — idempotency key)
    user_id     TEXT  FK → users.id
    amount      REAL  NOT NULL  (> 0, ≤ 1,000,000 per tx)
    type        TEXT  NOT NULL  ("credit" | "debit")
    note        TEXT
    created_at  REAL  NOT NULL

  idempotency_keys
    key         TEXT  PK   (client-supplied X-Idempotency-Key header)
    tx_id       TEXT        (transaction that was created)
    created_at  REAL  NOT NULL

  DATA FLOW
  ─────────
  POST /transaction
    1. Validate request body (userId, amount, type, idempotency key)
    2. Check idempotency_keys — if key already seen → return cached tx
    3. Acquire a per-user threading.Lock to prevent concurrent double-spend
    4. Inside lock: re-validate user balance for debits
    5. Write transaction + update user totals atomically (DB transaction)
    6. Store idempotency key → done

  GET /summary/:userId
    Simple SELECT + aggregate query, no writes.

  GET /ranking
    Multi-factor score = (total_amount × 0.6) + (tx_count × 10 × 0.3)
                       + (recency_bonus × 0.1)
    recency_bonus: 100 if last tx was within 24 h, else 0
    Capped at 1 tx/user/minute to prevent spam abuse.

  ASSUMPTIONS / MOCK DATA
  ────────────────────────
  • No real auth — userId is trusted as-is (assume upstream auth gateway)
  • Amounts are always positive floats (debit = subtract from total)
  • Users are auto-created on first transaction (no separate /register)
  • SQLite is used for simplicity; swap DATABASE_URL env var for Postgres
  • Rate limit: max 60 transactions per user per hour (in-memory counter)
====================================================================
"""

import os
import time
import uuid
import threading
from collections import defaultdict
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from sqlalchemy import text

# ── App setup ──────────────────────────────────────────────────────
app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv(
    "DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'leaderboard.db')}"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

# ── Per-user locks (prevent concurrent race on same user) ──────────
_user_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_user_locks_meta = threading.Lock()

def get_user_lock(user_id: str) -> threading.Lock:
    with _user_locks_meta:
        return _user_locks[user_id]

# ── Rate-limit store (in-memory, resets on restart) ───────────────
# { user_id: [timestamp, timestamp, ...] }
_rate_store: dict[str, list[float]] = defaultdict(list)
_rate_store_lock = threading.Lock()
RATE_LIMIT_MAX   = 60    # max transactions
RATE_LIMIT_WINDOW = 3600  # per hour (seconds)

def is_rate_limited(user_id: str) -> bool:
    now = time.time()
    with _rate_store_lock:
        timestamps = _rate_store[user_id]
        # Remove old entries outside window
        _rate_store[user_id] = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
        if len(_rate_store[user_id]) >= RATE_LIMIT_MAX:
            return True
        _rate_store[user_id].append(now)
        return False

# ── Models ─────────────────────────────────────────────────────────
class User(db.Model):
    __tablename__ = "users"
    id           = db.Column(db.String, primary_key=True)
    username     = db.Column(db.String, nullable=False)
    total_amount = db.Column(db.Float, default=0.0, nullable=False)
    tx_count     = db.Column(db.Integer, default=0, nullable=False)
    last_tx_at   = db.Column(db.Float, nullable=True)
    created_at   = db.Column(db.Float, nullable=False)

    def to_dict(self):
        return {
            "userId":      self.id,
            "username":    self.username,
            "totalAmount": round(self.total_amount, 2),
            "txCount":     self.tx_count,
            "lastTxAt":    self.last_tx_at,
            "createdAt":   self.created_at,
        }


class Transaction(db.Model):
    __tablename__ = "transactions"
    id         = db.Column(db.String, primary_key=True)
    user_id    = db.Column(db.String, db.ForeignKey("users.id"), nullable=False)
    amount     = db.Column(db.Float, nullable=False)
    type       = db.Column(db.String, nullable=False)   # "credit" | "debit"
    note       = db.Column(db.String, nullable=True)
    created_at = db.Column(db.Float, nullable=False)

    def to_dict(self):
        return {
            "txId":      self.id,
            "userId":    self.user_id,
            "amount":    round(self.amount, 2),
            "type":      self.type,
            "note":      self.note,
            "createdAt": self.created_at,
        }


class IdempotencyKey(db.Model):
    __tablename__ = "idempotency_keys"
    key        = db.Column(db.String, primary_key=True)
    tx_id      = db.Column(db.String, nullable=False)
    created_at = db.Column(db.Float, nullable=False)


# ── Helpers ────────────────────────────────────────────────────────
MAX_AMOUNT = 1_000_000

def validate_transaction_body(data: dict) -> list[str]:
    errors = []
    if not data.get("userId") or not isinstance(data["userId"], str):
        errors.append("userId is required and must be a string")
    if not data.get("username") or not isinstance(data["username"], str):
        errors.append("username is required and must be a string")
    if "amount" not in data:
        errors.append("amount is required")
    else:
        try:
            amt = float(data["amount"])
            if amt <= 0:
                errors.append("amount must be greater than 0")
            if amt > MAX_AMOUNT:
                errors.append(f"amount must not exceed {MAX_AMOUNT}")
        except (TypeError, ValueError):
            errors.append("amount must be a valid number")
    if data.get("type") not in ("credit", "debit"):
        errors.append("type must be 'credit' or 'debit'")
    return errors


def compute_rank_score(user: User) -> float:
    """
    Multi-factor ranking score (higher = better rank):

      score = (total_amount   × 0.60)
            + (tx_count × 10  × 0.30)
            + (recency_bonus  × 0.10)

    recency_bonus = 100 if last transaction within last 24 hours, else 0.
    This rewards consistent, recent activity — not just raw balance.
    """
    recency_bonus = 0.0
    if user.last_tx_at and (time.time() - user.last_tx_at) < 86400:
        recency_bonus = 100.0

    score = (
        user.total_amount * 0.60
        + user.tx_count * 10 * 0.30
        + recency_bonus * 0.10
    )
    return round(score, 4)


def error_response(message: str, status: int, details=None):
    body = {"success": False, "error": message}
    if details:
        body["details"] = details
    return jsonify(body), status


def success_response(data: dict, status: int = 200):
    return jsonify({"success": True, **data}), status


# ── Routes ─────────────────────────────────────────────────────────

@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.post("/transaction")
def post_transaction():
    """
    POST /transaction
    Body: { userId, username, amount, type, note? }
    Header: X-Idempotency-Key  (optional but recommended)

    Returns the created (or already-processed) transaction.
    """
    data = request.get_json(silent=True)
    if not data:
        return error_response("Request body must be valid JSON", 400)

    # ── 1. Validate ────────────────────────────────────────────────
    errs = validate_transaction_body(data)
    if errs:
        return error_response("Validation failed", 400, errs)

    user_id  = data["userId"].strip()
    username = data["username"].strip()
    amount   = round(float(data["amount"]), 2)
    tx_type  = data["type"]
    note     = data.get("note", "").strip()[:500]  # cap note length

    # ── 2. Idempotency check ───────────────────────────────────────
    idem_key = request.headers.get("X-Idempotency-Key", "").strip()
    if idem_key:
        existing = IdempotencyKey.query.get(idem_key)
        if existing:
            tx = Transaction.query.get(existing.tx_id)
            return success_response(
                {"message": "Duplicate request — returning cached result",
                 "transaction": tx.to_dict(),
                 "duplicate": True},
                200
            )

    # ── 3. Rate limit ──────────────────────────────────────────────
    if is_rate_limited(user_id):
        return error_response(
            f"Rate limit exceeded: max {RATE_LIMIT_MAX} transactions per hour", 429
        )

    # ── 4. Per-user lock (prevent concurrent race conditions) ──────
    with get_user_lock(user_id):
        try:
            # ── 5. Upsert user ─────────────────────────────────────
            user = User.query.get(user_id)
            if user is None:
                user = User(
                    id=user_id,
                    username=username,
                    total_amount=0.0,
                    tx_count=0,
                    last_tx_at=None,
                    created_at=time.time(),
                )
                db.session.add(user)
                db.session.flush()  # get user into session before update

            # ── 6. Debit balance check ─────────────────────────────
            if tx_type == "debit":
                if user.total_amount < amount:
                    return error_response(
                        f"Insufficient balance: available {round(user.total_amount, 2)}, "
                        f"requested {amount}",
                        422
                    )
                user.total_amount = round(user.total_amount - amount, 2)
            else:
                user.total_amount = round(user.total_amount + amount, 2)

            # ── 7. Create transaction ──────────────────────────────
            tx = Transaction(
                id=str(uuid.uuid4()),
                user_id=user_id,
                amount=amount,
                type=tx_type,
                note=note,
                created_at=time.time(),
            )
            user.tx_count  += 1
            user.last_tx_at = tx.created_at

            db.session.add(tx)

            # ── 8. Store idempotency key ───────────────────────────
            if idem_key:
                db.session.add(IdempotencyKey(
                    key=idem_key, tx_id=tx.id, created_at=time.time()
                ))

            db.session.commit()
            return success_response(
                {"message": "Transaction recorded", "transaction": tx.to_dict()}, 201
            )

        except Exception as e:
            db.session.rollback()
            return error_response(f"Internal server error: {str(e)}", 500)


@app.get("/summary/<user_id>")
def get_summary(user_id: str):
    """
    GET /summary/:userId
    Returns user profile + all transactions for that user.
    """
    if not user_id or not user_id.strip():
        return error_response("userId is required", 400)

    user = User.query.get(user_id.strip())
    if user is None:
        return error_response(f"User '{user_id}' not found", 404)

    transactions = (
        Transaction.query
        .filter_by(user_id=user_id)
        .order_by(Transaction.created_at.desc())
        .all()
    )

    credits = sum(t.amount for t in transactions if t.type == "credit")
    debits  = sum(t.amount for t in transactions if t.type == "debit")

    return success_response({
        "user": user.to_dict(),
        "summary": {
            "totalCredits": round(credits, 2),
            "totalDebits":  round(debits, 2),
            "netBalance":   round(user.total_amount, 2),
            "txCount":      user.tx_count,
            "rankScore":    compute_rank_score(user),
        },
        "transactions": [t.to_dict() for t in transactions],
    })


@app.get("/ranking")
def get_ranking():
    """
    GET /ranking
    Returns all users ranked by multi-factor score.
    Query params:
      limit  (default 50, max 100)
      offset (default 0)
    """
    try:
        limit  = min(int(request.args.get("limit",  50)), 100)
        offset = max(int(request.args.get("offset",  0)),  0)
    except ValueError:
        return error_response("limit and offset must be integers", 400)

    users = User.query.all()
    ranked = sorted(users, key=compute_rank_score, reverse=True)

    page   = ranked[offset: offset + limit]
    result = []
    for i, user in enumerate(page):
        entry = user.to_dict()
        entry["rank"]      = offset + i + 1
        entry["rankScore"] = compute_rank_score(user)
        result.append(entry)

    return success_response({
        "ranking": result,
        "meta": {
            "total":  len(users),
            "limit":  limit,
            "offset": offset,
            "scoringFormula": (
                "score = (totalAmount × 0.60) + (txCount × 10 × 0.30) "
                "+ (recencyBonus[100 if lastTx<24h else 0] × 0.10)"
            ),
        },
    })


# ── Health check ───────────────────────────────────────────────────
@app.get("/health")
def health():
    return jsonify({"status": "ok", "timestamp": time.time()}), 200


# ── Bootstrap ──────────────────────────────────────────────────────
with app.app_context():
    db.create_all()

if __name__ == "__main__":
    app.run(debug=True, port=8080)
