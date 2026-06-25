# Leaderboard System — Flask Backend + Frontend

A transaction-based leaderboard system with API design, data consistency,
duplicate prevention, concurrency safety, and multi-factor ranking.

---

## Tech Stack

| Layer      | Technology                          |
|------------|-------------------------------------|
| Backend    | Python 3.11, Flask 3.0              |
| Database   | SQLite via SQLAlchemy (swap-ready)  |
| Frontend   | Vanilla HTML/CSS/JS (served by Flask) |
| Deploy     | Render           |

---

## Running Locally

```bash
pip install -r requirements.txt
python app.py
# → http://localhost:8080
```

---

## API Reference

### POST /transaction
```
Body (JSON):
  userId    string  required
  username  string  required
  amount    float   required  (> 0, ≤ 1,000,000)
  type      string  required  ("credit" | "debit")
  note      string  optional  (max 500 chars)

Header (optional):
  X-Idempotency-Key   string   Resend same key safely — won't double-process

Responses:
  201  Transaction created
  200  Duplicate key — cached result returned
  400  Validation error
  422  Insufficient balance (debit)
  429  Rate limit exceeded (60 tx/hour/user)
  500  Internal server error
```

### GET /summary/:userId
```
Returns:
  user        — profile (id, username, totalAmount, txCount)
  summary     — credits, debits, netBalance, rankScore
  transactions — full history, newest first
```

### GET /ranking
```
Query params:
  limit   int  (default 50, max 100)
  offset  int  (default 0)

Returns:
  ranking  — array of users with rank and rankScore
  meta     — total count, pagination info, scoring formula
```

---

## Database Schema

```
users
  id           TEXT  PK
  username     TEXT  NOT NULL
  total_amount REAL  DEFAULT 0
  tx_count     INT   DEFAULT 0
  last_tx_at   REAL  nullable (unix timestamp)
  created_at   REAL  NOT NULL

transactions
  id           TEXT  PK (UUID v4)
  user_id      TEXT  FK → users.id
  amount       REAL  NOT NULL
  type         TEXT  "credit" | "debit"
  note         TEXT  nullable
  created_at   REAL  NOT NULL

idempotency_keys
  key          TEXT  PK
  tx_id        TEXT  FK → transactions.id
  created_at   REAL  NOT NULL
```

---

## Design Decisions

### 1. Idempotency
Every POST /transaction accepts an optional `X-Idempotency-Key` header.
If the same key is seen twice, the cached transaction is returned — the
DB write is skipped entirely. This makes retries safe.

### 2. Concurrency Safety
A `threading.Lock` is held per `userId` during the entire write path
(balance check → write → commit). Two simultaneous requests for the
same user are serialised — the second waits until the first commits.
This prevents double-spend on debit transactions.

### 3. Atomic DB writes
All writes (upsert user, insert transaction, store idempotency key) happen
in a single SQLAlchemy session with `db.session.commit()`. On any failure
the session is rolled back — no partial state is ever persisted.

### 4. Ranking Formula
```
score = (total_amount × 0.60)
      + (tx_count × 10 × 0.30)
      + (recency_bonus × 0.10)

recency_bonus = 100 if last_tx_at < 24h ago, else 0
```
This rewards three things:
- Large total volume (60% weight) — main driver
- Consistent activity (30%) — many small transactions still rank well
- Recent activity (10%) — active users ranked above dormant ones with the same balance

### 5. Abuse Prevention
- Max 60 transactions per user per hour (in-memory sliding window)
- Amount capped at 1,000,000 per transaction
- Note field capped at 500 characters
- Debit balance check (cannot go negative)
- Idempotency prevents replay/duplicate abuse

### 6. Auto User Creation
Users are created automatically on first transaction — no separate
/register endpoint needed.

---

## Assumptions & Mock Data

- No authentication — userId is trusted as supplied (upstream auth assumed)
- SQLite is used for portability; swap DATABASE_URL env var to PostgreSQL URI for production
- In-memory rate limit store resets on server restart (acceptable for assignment scope)
- Amounts are always positive; debit = subtract from running total
- Users cannot have a negative total_amount balance

---

## Deployment (Render)

1. Push to GitHub
2. New Web Service → connect repo
3. Build: `pip install -r requirements.txt`
4. Start: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 4`
5. Done — frontend is served at `/` by Flask itself
