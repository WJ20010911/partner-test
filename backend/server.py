#!/usr/bin/env python3
"""Partner Test Server - Zero external dependencies.
Uses only Python standard library (http.server + sqlite3 + hashlib + hmac).
"""
import http.server
import json
import sqlite3
import hashlib
import hmac
import base64
import uuid
import os
import re
import random
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("PORT", "8000"))
DB_PATH = os.path.join(os.path.dirname(__file__), "partner_test.db")
SECRET_KEY = os.environ.get("SECRET_KEY", "partner-test-secret-change-in-prod")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "123123")
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
QUESTION_COUNT_PER_TEST = 12
TOKEN_EXPIRY_HOURS = 72

MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".ico": "image/x-icon",
    ".svg": "image/svg+xml",
}

# ── Database ──────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'contributor',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tags (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS questions (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            options TEXT NOT NULL,
            dimension TEXT,
            weight REAL DEFAULT 1.0,
            time_limit INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            submitter_id TEXT REFERENCES users(id),
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS question_tags (
            question_id TEXT REFERENCES questions(id),
            tag_id TEXT REFERENCES tags(id),
            PRIMARY KEY (question_id, tag_id)
        );
        CREATE TABLE IF NOT EXISTS test_records (
            id TEXT PRIMARY KEY,
            answers TEXT NOT NULL,
            surface_score REAL NOT NULL,
            real_score REAL NOT NULL,
            token TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS question_skips (
            id TEXT PRIMARY KEY,
            question_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS admin_logs (
            id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            detail TEXT NOT NULL DEFAULT '',
            ip TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
    """)
    # Add banned column to users if not exists
    try:
        conn.execute("ALTER TABLE users ADD COLUMN banned INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    # Add username column to users if not exists
    try:
        conn.execute("ALTER TABLE users ADD COLUMN username TEXT NOT NULL DEFAULT ''")
    except Exception:
        pass
    # Ensure test_uploader user exists for test-login feature
    conn.execute(
        "INSERT OR IGNORE INTO users (id, email, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?)",
        ("test_uploader", "test@local.dev", "", "user", datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))
    )
    conn.commit()
    conn.close()

# ── Auth helpers ──────────────────────────────────────────

def hash_password(password):
    salt = os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return base64.b64encode(salt).decode() + ":" + base64.b64encode(h).decode()

def verify_password(password, stored):
    try:
        salt_b64, hash_b64 = stored.split(":")
        salt = base64.b64decode(salt_b64)
        stored_hash = base64.b64decode(hash_b64)
        h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
        return hmac.compare_digest(h, stored_hash)
    except Exception:
        return False

def make_token(user_id, role):
    exp = int((datetime.now(datetime.UTC) + timedelta(hours=TOKEN_EXPIRY_HOURS)).timestamp())
    payload = json.dumps({"uid": user_id, "role": role, "exp": exp}, separators=(",", ":")).encode()
    sig = hmac.new(SECRET_KEY.encode(), payload, hashlib.sha256).hexdigest()
    data = json.dumps({"p": payload.decode(), "sig": sig})
    return base64.urlsafe_b64encode(data.encode()).decode()

def decode_token(token):
    try:
        data = json.loads(base64.urlsafe_b64decode(token.encode()).decode())
        payload = json.loads(data["p"])
        expected = hmac.new(SECRET_KEY.encode(), data["p"].encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, data["sig"]):
            return None
        if payload["exp"] < datetime.now(datetime.UTC).timestamp():
            return None
        return payload
    except Exception:
        return None

def get_current_user(headers):
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:]
    return decode_token(token)

def require_auth(headers):
    user = get_current_user(headers)
    if not user:
        return None, error_response("Unauthorized", 401)
    return user, None

def require_admin(headers):
    user = get_current_user(headers)
    if not user:
        return None, error_response("Unauthorized", 401)
    if user.get("role") != "admin":
        return None, error_response("Admin only", 403)
    return user, None

# ── Crypto for test results ──────────────────────────────

def generate_token(record_id, real_score):
    ts = int(datetime.now(datetime.UTC).timestamp())
    payload = f"{record_id}|{real_score}|{ts}"
    sig = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    data = json.dumps({"rid": record_id, "rs": real_score, "t": ts, "sig": sig})
    return base64.urlsafe_b64encode(data.encode()).decode()

def decode_record_token(token):
    try:
        data = json.loads(base64.urlsafe_b64decode(token.encode()).decode())
        payload = f"{data['rid']}|{data['rs']}|{data['t']}"
        expected = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(expected, data["sig"]):
            return data
    except Exception:
        pass
    return None

def generate_short_code(real_score):
    """Simple base64-encoded short string for screenshot verification."""
    data = json.dumps({"s": real_score}, separators=(",", ":"))
    return base64.urlsafe_b64encode(data.encode()).decode()

# ── JSON response helpers ─────────────────────────────────

def json_response(data, status=200):
    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    return (status, body, "application/json; charset=utf-8")

def error_response(msg, status=400):
    return json_response({"detail": msg}, status)

def read_body(handler):
    length = int(handler.headers.get("Content-Length", 0))
    if length == 0:
        return None
    return json.loads(handler.rfile.read(length).decode("utf-8"))

# ── API Handlers ──────────────────────────────────────────

def handle_register(body):
    email = body.get("email", "").strip()
    password = body.get("password", "")
    username = body.get("username", "").strip() or email.split("@")[0]
    if not email or not password:
        return error_response("Email and password required")
    if len(password) < 6:
        return error_response("Password too short")
    conn = get_db()
    try:
        existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if existing:
            return error_response("Email already registered", 400)
        uid = uuid.uuid4().hex[:8]
        conn.execute(
            "INSERT INTO users (id, email, password_hash, role, created_at, username) VALUES (?, ?, ?, ?, ?, ?)",
            (uid, email, hash_password(password), "contributor", datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'), username)
        )
        conn.commit()
        token = make_token(uid, "contributor")
        return json_response({"access_token": token, "token_type": "bearer"})
    finally:
        conn.close()

def handle_login(body):
    email = body.get("email", "").strip()
    password = body.get("password", "")
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not row or not verify_password(password, row["password_hash"]):
            return error_response("Invalid credentials", 401)
        token = make_token(row["id"], row["role"])
        return json_response({"access_token": token, "token_type": "bearer", "username": row["username"] or ""})
    finally:
        conn.close()

def handle_me(headers):
    user, err = require_auth(headers)
    if err:
        return err
    conn = get_db()
    try:
        row = conn.execute("SELECT id, email, role, created_at, username FROM users WHERE id = ?", (user["uid"],)).fetchone()
        if not row:
            return error_response("User not found", 404)
        return json_response({
            "id": row["id"], "email": row["email"],
            "role": row["role"], "created_at": row["created_at"],
            "username": row["username"] or ""
        })
    finally:
        conn.close()

def handle_test_login(headers, body):
    conn = get_db()
    try:
        existing = conn.execute("SELECT id FROM users WHERE id = ?", ("test_uploader",)).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO users (id, email, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?)",
                ("test_uploader", "test@local.dev", "", "user", datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))
            )
            conn.commit()
    finally:
        conn.close()
    token = make_token("test_uploader", "user")
    return json_response({"access_token": token, "token_type": "bearer", "test_mode": True})

def handle_get_questions(headers, query):
    count = int(query.get("count", [QUESTION_COUNT_PER_TEST])[0])
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM questions WHERE status = 'approved' ORDER BY RANDOM() LIMIT ?",
            (count,)
        ).fetchall()
        result = []
        for r in rows:
            tags = [t["name"] for t in conn.execute(
                "SELECT t.name FROM tags t JOIN question_tags qt ON t.id = qt.tag_id WHERE qt.question_id = ?",
                (r["id"],)
            ).fetchall()]
            result.append({
                "id": r["id"],
                "content": r["content"],
                "options": json.loads(r["options"]),
                "dimension": r["dimension"],
                "weight": r["weight"],
                "time_limit": r["time_limit"],
                "status": r["status"],
                "tags": tags,
                "created_at": r["created_at"],
            })
        return json_response(result)
    finally:
        conn.close()

def handle_create_question(headers, body):
    user, err = require_auth(headers)
    if err:
        return err
    content = body.get("content", "").strip()
    options = body.get("options", [])
    tags = body.get("tags", [])
    dimension = body.get("dimension")
    weight = body.get("weight", 1.0)
    time_limit = body.get("time_limit", 0)
    if not content or len(options) < 2:
        return error_response("Content and at least 2 options required")
    conn = get_db()
    try:
        qid = uuid.uuid4().hex[:8]
        conn.execute(
            "INSERT INTO questions (id, content, options, dimension, weight, time_limit, status, submitter_id, created_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
            (qid, content, json.dumps(options, ensure_ascii=False), dimension, weight, time_limit, user["uid"], datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        for tag_name in tags:
            tag_name = tag_name.strip()
            if not tag_name:
                continue
            existing = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
            if existing:
                tid = existing["id"]
            else:
                tid = uuid.uuid4().hex[:8]
                conn.execute("INSERT INTO tags (id, name) VALUES (?, ?)", (tid, tag_name))
            conn.execute("INSERT OR IGNORE INTO question_tags (question_id, tag_id) VALUES (?, ?)", (qid, tid))
        conn.commit()
        return json_response({"id": qid, "status": "pending"}, 201)
    finally:
        conn.close()

def handle_pending_questions(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM questions WHERE status = 'pending' ORDER BY created_at DESC").fetchall()
        result = []
        for r in rows:
            tags = [t["name"] for t in conn.execute(
                "SELECT t.name FROM tags t JOIN question_tags qt ON t.id = qt.tag_id WHERE qt.question_id = ?",
                (r["id"],)
            ).fetchall()]
            result.append({
                "id": r["id"],
                "content": r["content"],
                "options": json.loads(r["options"]),
                "dimension": r["dimension"],
                "weight": r["weight"],
                "time_limit": r["time_limit"],
                "status": r["status"],
                "submitter_id": r["submitter_id"],
                "tags": tags,
                "created_at": r["created_at"],
            })
        return json_response(result)
    finally:
        conn.close()

def handle_review_question(headers, qid, body):
    user, err = require_admin(headers)
    if err:
        return err
    status = body.get("status")
    if status not in ("approved", "rejected"):
        return error_response("Invalid status")
    conn = get_db()
    try:
        row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
        if not row:
            return error_response("Question not found", 404)
        conn.execute("UPDATE questions SET status = ? WHERE id = ?", (status, qid))
        conn.commit()
        return json_response({"id": qid, "status": status})
    finally:
        conn.close()

def handle_submit_test(headers, body):
    answers = body.get("answers", [])
    if not answers:
        return error_response("No answers provided")
    real_score = 0.0
    conn = get_db()
    try:
        for ans in answers:
            qid = ans.get("question_id")
            idx = ans.get("selected_index", -1)
            behavior = ans.get("behavior", "normal")
            time_taken = ans.get("time_taken", 0)
            row = conn.execute("SELECT * FROM questions WHERE id = ? AND status = 'approved'", (qid,)).fetchone()
            if not row:
                continue
            w = row["weight"]
            time_limit = row["time_limit"]
            if behavior == "complaint":
                real_score += 15.0 * w
                sid = uuid.uuid4().hex[:8]
                conn.execute(
                    "INSERT INTO question_skips (id, question_id, reason, created_at) VALUES (?, ?, 'complaint', ?)",
                    (sid, qid, datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))
                )
            elif time_limit > 0 and time_taken > time_limit + 2:
                real_score += 10.0 * w
            else:
                options = json.loads(row["options"])
                if 0 <= idx < len(options):
                    real_score += options[idx].get("score", 0) * w
        real_score = round(real_score, 1)
        surface_score = round(max(0.0, 100.0 - real_score), 1)
        rid = uuid.uuid4().hex[:8]
        token = generate_token(rid, real_score)
        conn.execute(
            "INSERT INTO test_records (id, answers, surface_score, real_score, token, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (rid, json.dumps(answers, ensure_ascii=False), surface_score, real_score, token, datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        conn.commit()
        return json_response({
            "record_id": rid,
            "surface_score": surface_score,
            "short_code": generate_short_code(real_score),
            "real_token": token,
            "created_at": datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
    finally:
        conn.close()

def handle_verify(headers, record_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM test_records WHERE id = ?", (record_id,)).fetchone()
        if not row:
            return error_response("Record not found", 404)
        decoded = decode_record_token(row["token"])
        is_authentic = decoded is not None and decoded["rs"] == row["real_score"]
        return json_response({
            "record_id": row["id"],
            "real_score": row["real_score"],
            "surface_score": row["surface_score"],
            "is_authentic": is_authentic,
            "created_at": row["created_at"],
        })
    finally:
        conn.close()

def handle_verify_by_token(headers, query):
    token = query.get("token", [None])[0]
    if not token:
        return error_response("Token required", 400)
    decoded = decode_record_token(token)
    if not decoded:
        return json_response({"is_authentic": False})
    # Token is valid — look up record for created_at
    conn = get_db()
    try:
        row = conn.execute("SELECT created_at FROM test_records WHERE id = ?", (decoded["rid"],)).fetchone()
        return json_response({
            "is_authentic": True,
            "real_score": decoded["rs"],
            "created_at": row["created_at"] if row else None,
        })
    finally:
        conn.close()

def handle_get_record(headers, record_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM test_records WHERE id = ?", (record_id,)).fetchone()
        if not row:
            return error_response("Record not found", 404)
        return json_response({
            "record_id": row["id"],
            "real_score": row["real_score"],
            "surface_score": row["surface_score"],
            "short_code": generate_short_code(row["real_score"]),
            "token": row["token"],
            "created_at": row["created_at"],
        })
    finally:
        conn.close()

def handle_get_replacement(headers, query):
    exclude = query.get("exclude", [""])[0]
    exclude_ids = [e for e in exclude.split(",") if e] if exclude else []
    conn = get_db()
    try:
        if exclude_ids:
            ph = ",".join("?" for _ in exclude_ids)
            row = conn.execute(f"SELECT * FROM questions WHERE status = 'approved' AND id NOT IN ({ph}) ORDER BY RANDOM() LIMIT 1", exclude_ids).fetchone()
        else:
            row = conn.execute("SELECT * FROM questions WHERE status = 'approved' ORDER BY RANDOM() LIMIT 1").fetchone()
        if not row:
            return error_response("No replacement available", 404)
        tags = [t["name"] for t in conn.execute(
            "SELECT t.name FROM tags t JOIN question_tags qt ON t.id = qt.tag_id WHERE qt.question_id = ?",
            (row["id"],)
        ).fetchall()]
        return json_response({
            "id": row["id"],
            "content": row["content"],
            "options": json.loads(row["options"]),
            "dimension": row["dimension"],
            "weight": row["weight"],
            "tags": tags,
        })
    finally:
        conn.close()

def handle_record_skip(headers, body):
    question_id = body.get("question_id", "")
    reason = body.get("reason", "skip")
    if reason not in ("skip", "complaint"):
        return error_response("Invalid reason", 400)
    conn = get_db()
    try:
        sid = uuid.uuid4().hex[:8]
        conn.execute(
            "INSERT INTO question_skips (id, question_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (sid, question_id, reason, datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        conn.commit()
        return json_response({"id": sid})
    finally:
        conn.close()

def handle_get_complaints(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        # Get all unique question_ids that have complaints, with counts
        rows = conn.execute(
            "SELECT question_id, COUNT(*) as cnt, q.content as q_content "
            "FROM question_skips qs JOIN questions q ON qs.question_id = q.id "
            "WHERE qs.reason = 'complaint' "
            "GROUP BY question_id "
            "ORDER BY cnt DESC"
        ).fetchall()

        # Count how many times each question appeared in tests
        # test_records.answers is a JSON array of {question_id, ...}
        answer_counts = {}
        all_records = conn.execute("SELECT answers FROM test_records").fetchall()
        for rec in all_records:
            seen = set()
            try:
                ans_list = json.loads(rec["answers"])
                for a in ans_list:
                    qid = a.get("question_id")
                    if qid:
                        seen.add(qid)
            except (json.JSONDecodeError, TypeError):
                pass
            for qid in seen:
                answer_counts[qid] = answer_counts.get(qid, 0) + 1

        result = []
        for r in rows:
            qid = r["question_id"]
            complaint_cnt = r["cnt"]
            answered_cnt = answer_counts.get(qid, 0)
            ratio = round(complaint_cnt / answered_cnt, 4) if answered_cnt > 0 else 0
            result.append({
                "question_id": qid,
                "question_content": r["q_content"],
                "complaint_count": complaint_cnt,
                "answered_count": answered_cnt,
                "ratio": ratio,
            })
        return json_response(result)
    finally:
        conn.close()

def handle_delete_question(headers, qid):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
        if not row:
            return error_response("Question not found", 404)
        conn.execute("DELETE FROM question_tags WHERE question_id = ?", (qid,))
        conn.execute("DELETE FROM question_skips WHERE question_id = ?", (qid,))
        conn.execute("DELETE FROM questions WHERE id = ?", (qid,))
        conn.commit()
        log_admin_action(headers, "delete_question", qid)
        return json_response({"id": qid, "status": "deleted"})
    finally:
        conn.close()

# ── Admin delete with password ──────────────────────

def handle_admin_delete_question(headers, body):
    user, err = require_admin(headers)
    if err:
        return err
    password = body.get("password", "")
    if not verify_admin_password(password):
        return error_response("管理员密码错误", 401)
    qid = body.get("id", "")
    if not qid:
        return error_response("No ID provided", 400)
    conn = get_db()
    try:
        row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
        if not row:
            return error_response("Question not found", 404)
        conn.execute("DELETE FROM question_tags WHERE question_id = ?", (qid,))
        conn.execute("DELETE FROM question_skips WHERE question_id = ?", (qid,))
        conn.execute("DELETE FROM questions WHERE id = ?", (qid,))
        conn.commit()
        log_admin_action(headers, "delete_question", qid)
        return json_response({"id": qid, "status": "deleted"})
    finally:
        conn.close()

def handle_admin_batch_delete(headers, body):
    user, err = require_admin(headers)
    if err:
        return err
    password = body.get("password", "")
    if not verify_admin_password(password):
        return error_response("管理员密码错误", 401)
    ids = body.get("ids", [])
    if not ids:
        return error_response("No IDs provided", 400)
    conn = get_db()
    try:
        deleted = 0
        for qid in ids:
            row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
            if not row:
                continue
            conn.execute("DELETE FROM question_tags WHERE question_id = ?", (qid,))
            conn.execute("DELETE FROM question_skips WHERE question_id = ?", (qid,))
            conn.execute("DELETE FROM questions WHERE id = ?", (qid,))
            deleted += 1
        conn.commit()
        log_admin_action(headers, "batch_delete_questions", f"Deleted {deleted} questions")
        return json_response({"deleted": deleted})
    finally:
        conn.close()

# ── Contributors ─────────────────────────────────────

def handle_contributors(headers):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT DISTINCT u.id, u.username, u.email FROM users u "
            "INNER JOIN questions q ON q.submitter_id = u.id "
            "WHERE q.status = 'approved' "
            "ORDER BY u.username ASC"
        ).fetchall()
        result = []
        for r in rows:
            result.append({
                "username": r["username"] or r["email"].split("@")[0],
                "email": r["email"]
            })
        return json_response(result)
    finally:
        conn.close()

# ── Admin all questions ─────────────────────────────

def handle_all_questions(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM questions ORDER BY created_at DESC").fetchall()
        result = []
        for r in rows:
            tags = [t["name"] for t in conn.execute(
                "SELECT t.name FROM tags t JOIN question_tags qt ON t.id = qt.tag_id WHERE qt.question_id = ?",
                (r["id"],)
            ).fetchall()]
            # Get submitter info
            submitter = None
            if r["submitter_id"]:
                u = conn.execute("SELECT username, email FROM users WHERE id = ?", (r["submitter_id"],)).fetchone()
                if u:
                    submitter = u["username"] or u["email"].split("@")[0]
            result.append({
                "id": r["id"],
                "content": r["content"],
                "options": json.loads(r["options"]),
                "dimension": r["dimension"],
                "weight": r["weight"],
                "time_limit": r["time_limit"],
                "status": r["status"],
                "submitter": submitter,
                "tags": tags,
                "created_at": r["created_at"],
            })
        return json_response(result)
    finally:
        conn.close()

# ── Admin question edit ──────────────────────────────

def handle_edit_question(headers, qid, body):
    # Verify admin password again before allowing edit
    user, err = require_admin(headers)
    if err:
        return err
    password = body.get("password", "")
    if not verify_admin_password(password):
        return error_response("Admin password required", 401)
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM questions WHERE id = ?", (qid,)).fetchone()
        if not row:
            return error_response("Question not found", 404)
        content = body.get("content", row["content"])
        options = body.get("options", json.loads(row["options"]))
        dimension = body.get("dimension", row["dimension"])
        weight = body.get("weight", row["weight"])
        time_limit = body.get("time_limit", row["time_limit"])
        conn.execute(
            "UPDATE questions SET content=?, options=?, dimension=?, weight=?, time_limit=? WHERE id=?",
            (content, json.dumps(options, ensure_ascii=False), dimension, weight, time_limit, qid)
        )
        # Handle tags update
        if "tags" in body:
            conn.execute("DELETE FROM question_tags WHERE question_id = ?", (qid,))
            for tag_name in body["tags"]:
                tag_name = tag_name.strip()
                if not tag_name:
                    continue
                existing = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
                if existing:
                    tid = existing["id"]
                else:
                    tid = uuid.uuid4().hex[:8]
                    conn.execute("INSERT INTO tags (id, name) VALUES (?, ?)", (tid, tag_name))
                conn.execute("INSERT OR IGNORE INTO question_tags (question_id, tag_id) VALUES (?, ?)", (qid, tid))
        conn.commit()
        return json_response({"id": qid, "status": "updated"})
    finally:
        conn.close()

# ── Admin auth & stats ──────────────────────────────

ADMIN_PASSWORD_HASH = os.environ.get("ADMIN_PASSWORD_HASH", "")
# Rate limiting for admin login
_admin_login_attempts = {}  # ip -> [timestamp, ...]
ADMIN_RATE_LIMIT = 5        # max attempts
ADMIN_RATE_WINDOW = 300     # 5 minutes

def check_admin_rate_limit(ip):
    now = datetime.now(datetime.UTC).timestamp()
    attempts = _admin_login_attempts.get(ip, [])
    # Remove expired entries
    attempts = [t for t in attempts if now - t < ADMIN_RATE_WINDOW]
    _admin_login_attempts[ip] = attempts
    return len(attempts) >= ADMIN_RATE_LIMIT

def record_admin_attempt(ip):
    _admin_login_attempts.setdefault(ip, []).append(datetime.now(datetime.UTC).timestamp())

def verify_admin_password(password):
    if not ADMIN_PASSWORD_HASH:
        # Fallback: compare via hash_password
        h = hashlib.pbkdf2_hmac("sha256", password.encode(), b"admin_salt", 100000)
        expected = base64.b64encode(h).decode()
        # First-time init: store the hash
        return expected == base64.b64encode(hashlib.pbkdf2_hmac("sha256", b"123123", b"admin_salt", 100000)).decode()
    return verify_password(password, ADMIN_PASSWORD_HASH)

def handle_admin_auth(headers, body):
    ip = headers.get("X-Forwarded-For", headers.get("Remote-Addr", "unknown"))
    if check_admin_rate_limit(ip):
        return error_response("登录尝试过于频繁，请 5 分钟后再试", 429)
    password = body.get("password", "")
    if not verify_admin_password(password):
        record_admin_attempt(ip)
        return error_response("密码错误", 403)
    token = make_token("admin", "admin")
    return json_response({"access_token": token, "token_type": "bearer"})

def log_admin_action(headers, action, detail=""):
    """Log an admin action to the admin_logs table."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO admin_logs (id, action, detail, ip, created_at) VALUES (?, ?, ?, ?, ?)",
            (uuid.uuid4().hex[:8], action, detail,
             headers.get("X-Forwarded-For", headers.get("Remote-Addr", "unknown")),
             datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        conn.commit()
    finally:
        conn.close()

def handle_admin_stats(headers):
    # Allow either admin token or password header
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM test_records").fetchone()["c"]
        row = conn.execute("SELECT MAX(real_score) as mx, MIN(real_score) as mn, AVG(real_score) as av FROM test_records").fetchone()
        question_count = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
        return json_response({
            "total_tests": total,
            "max_score": round(row["mx"], 1) if row["mx"] is not None else None,
            "min_score": round(row["mn"], 1) if row["mn"] is not None else None,
            "avg_score": round(row["av"], 1) if row["av"] is not None else None,
            "question_count": question_count,
        })
    finally:
        conn.close()

# ── New Admin API Handlers ───────────────────────────

def handle_admin_score_distribution(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT real_score FROM test_records").fetchall()
        buckets = {"0-10":0,"11-20":0,"21-30":0,"31-40":0,"41-50":0,"51-60":0,"61-70":0,"71-80":0,"81-90":0,"91-100":0}
        for r in rows:
            s = r["real_score"]
            if s <= 10: buckets["0-10"] += 1
            elif s <= 20: buckets["11-20"] += 1
            elif s <= 30: buckets["21-30"] += 1
            elif s <= 40: buckets["31-40"] += 1
            elif s <= 50: buckets["41-50"] += 1
            elif s <= 60: buckets["51-60"] += 1
            elif s <= 70: buckets["61-70"] += 1
            elif s <= 80: buckets["71-80"] += 1
            elif s <= 90: buckets["81-90"] += 1
            else: buckets["91-100"] += 1
        return json_response({"buckets": buckets, "total": len(rows)})
    finally:
        conn.close()

def handle_admin_test_trend(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT date(created_at) as day, COUNT(*) as cnt FROM test_records GROUP BY day ORDER BY day ASC LIMIT 30"
        ).fetchall()
        return json_response({"daily": [{"date": r["day"], "count": r["cnt"]} for r in rows]})
    finally:
        conn.close()

def handle_admin_question_stats(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM questions").fetchone()["c"]
        approved = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
        pending = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='pending'").fetchone()["c"]
        rejected = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='rejected'").fetchone()["c"]
        return json_response({"total": total, "approved": approved, "pending": pending, "rejected": rejected})
    finally:
        conn.close()

def handle_admin_users(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT id, email, role, created_at, banned FROM users ORDER BY created_at DESC").fetchall()
        return json_response([dict(r) for r in rows])
    finally:
        conn.close()

def handle_admin_ban_user(headers, uid, body):
    user, err = require_admin(headers)
    if err:
        return err
    banned = body.get("banned", 1)
    conn = get_db()
    try:
        conn.execute("UPDATE users SET banned = ? WHERE id = ?", (banned, uid))
        conn.commit()
        log_admin_action(headers, "ban_user" if banned else "unban_user", uid)
        return json_response({"id": uid, "banned": banned})
    finally:
        conn.close()

def handle_admin_tags(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT t.id, t.name, COUNT(qt.question_id) as q_count "
            "FROM tags t LEFT JOIN question_tags qt ON t.id = qt.tag_id "
            "GROUP BY t.id ORDER BY q_count DESC"
        ).fetchall()
        return json_response([{"id": r["id"], "name": r["name"], "question_count": r["q_count"]} for r in rows])
    finally:
        conn.close()

def handle_admin_export(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM test_records ORDER BY created_at DESC").fetchall()
        records = []
        for r in rows:
            records.append({
                "record_id": r["id"],
                "real_score": r["real_score"],
                "surface_score": r["surface_score"],
                "created_at": r["created_at"],
            })
        return json_response(records)
    finally:
        conn.close()

def handle_admin_logs(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT 100").fetchall()
        return json_response([dict(r) for r in rows])
    finally:
        conn.close()

def handle_admin_change_pwd(headers, body):
    user, err = require_admin(headers)
    if err:
        return err
    old_pw = body.get("old_password", "")
    new_pw = body.get("new_password", "")
    if not new_pw or len(new_pw) < 6:
        return error_response("新密码至少 6 位", 400)
    if not verify_admin_password(old_pw):
        return error_response("旧密码错误", 403)
    # Update env var won't persist; store in DB for persistence
    conn = get_db()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS admin_config (key TEXT PRIMARY KEY, value TEXT)")
        h = hash_password(new_pw)
        conn.execute("INSERT OR REPLACE INTO admin_config (key, value) VALUES ('password_hash', ?)", (h,))
        conn.commit()
        log_admin_action(headers, "change_password")
        return json_response({"status": "ok"})
    finally:
        conn.close()

def handle_admin_get_config(headers):
    user, err = require_admin(headers)
    if err:
        return err
    config = {}
    conn = get_db()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS admin_config (key TEXT PRIMARY KEY, value TEXT)")
        rows = conn.execute("SELECT * FROM admin_config").fetchall()
        for r in rows:
            config[r["key"]] = r["value"]
    finally:
        conn.close()
    return json_response(config)

# ── Router ────────────────────────────────────────────────

API_ROUTES = []

def route(method, pattern):
    def wrapper(func):
        API_ROUTES.append((method, re.compile("^" + pattern + "$"), func))
        return func
    return wrapper

def dispatch_api(method, path, headers, body):
    for m, pat, handler in API_ROUTES:
        if method != m:
            continue
        m = pat.match(path)
        if m:
            return handler(headers, body, *m.groups())
    return error_response("Not Found", 404)

# Register routes
route("POST", r"/api/auth/register")(lambda h, b, *a: handle_register(b))
route("POST", r"/api/auth/login")(lambda h, b, *a: handle_login(b))
route("POST", r"/api/auth/test-login")(lambda h, b, *a: handle_test_login(h, b))
route("GET", r"/api/auth/me")(lambda h, b, *a: handle_me(h))
route("GET", r"/api/questions")(lambda h, b, *a: handle_get_questions(h, parse_qs(urlparse(a[0] if a else "/").query) if False else {}))
# We handle query params differently - the lambda above is a placeholder
route("POST", r"/api/questions")(lambda h, b, *a: handle_create_question(h, b))
route("GET", r"/api/questions/pending")(lambda h, b, *a: handle_pending_questions(h))
route("PATCH", r"/api/questions/([a-f0-9]+)")(lambda h, b, qid: handle_review_question(h, qid, b))
route("POST", r"/api/test/submit")(lambda h, b, *a: handle_submit_test(h, b))
route("GET", r"/api/test/verify/([a-f0-9]+)")(lambda h, b, rid: handle_verify(h, rid))
route("GET", r"/api/test/verify-token")(lambda h, b, *a: None)  # handled via query params in _handle
route("GET", r"/api/questions/replacement")(lambda h, b, *a: None)  # handled via query params in _handle
route("POST", r"/api/test/skip")(lambda h, b, *a: handle_record_skip(h, b))
route("GET", r"/api/questions/complaints")(lambda h, b, *a: handle_get_complaints(h))
route("DELETE", r"/api/questions/([a-f0-9]+)")(lambda h, b, qid: handle_delete_question(h, qid))
route("GET", r"/api/test/([a-f0-9]+)")(lambda h, b, rid: handle_get_record(h, rid))
route("POST", r"/api/admin/auth")(lambda h, b, *a: handle_admin_auth(h, b))
route("GET", r"/api/admin/stats")(lambda h, b, *a: handle_admin_stats(h))
route("GET", r"/api/admin/score-distribution")(lambda h, b, *a: handle_admin_score_distribution(h))
route("GET", r"/api/admin/test-trend")(lambda h, b, *a: handle_admin_test_trend(h))
route("GET", r"/api/admin/question-stats")(lambda h, b, *a: handle_admin_question_stats(h))
route("GET", r"/api/admin/users")(lambda h, b, *a: handle_admin_users(h))
route("PATCH", r"/api/admin/users/([a-f0-9]+)/ban")(lambda h, b, uid: handle_admin_ban_user(h, uid, b))
route("GET", r"/api/admin/tags")(lambda h, b, *a: handle_admin_tags(h))
route("GET", r"/api/admin/export")(lambda h, b, *a: handle_admin_export(h))
route("GET", r"/api/admin/logs")(lambda h, b, *a: handle_admin_logs(h))
route("POST", r"/api/admin/change-password")(lambda h, b, *a: handle_admin_change_pwd(h, b))
route("GET", r"/api/admin/config")(lambda h, b, *a: handle_admin_get_config(h))
route("GET", r"/api/admin/question-counts")(lambda h, b, *a: handle_question_counts(h))
route("POST", r"/api/admin/batch-insert-test")(lambda h, b, *a: handle_batch_insert_test_questions(h, b))
route("GET", r"/api/contributors")(lambda h, b, *a: handle_contributors(h))
route("PATCH", r"/api/questions/([a-f0-9]+)/edit")(lambda h, b, qid: handle_edit_question(h, qid, b))
route("GET", r"/api/questions/all")(lambda h, b, *a: handle_all_questions(h))
route("POST", r"/api/admin/questions/delete")(lambda h, b, *a: handle_admin_delete_question(h, b))
route("POST", r"/api/admin/questions/batch-delete")(lambda h, b, *a: handle_admin_batch_delete(h, b))

# ── Test question pool for quick-fill ────────────────────

TEST_QUESTIONS_POOL = [
    {
        "content": "如果你和伴侣在消费观念上产生分歧，你更倾向于：",
        "dimension": "消费与风控",
        "tags": ["消费观"],
        "time_limit": 15,
        "weight": 1.5,
        "options": [
            {"text": "坐下来沟通，制定双方都能接受的预算方案", "score": 0},
            {"text": "各花各的，互不干涉", "score": 1},
            {"text": "坚持自己的观点，试图说服对方", "score": 2},
            {"text": "冷战直到对方妥协", "score": 3},
        ]
    },
    {
        "content": "加班到很晚回家，发现伴侣已经睡了但给你留了灯和饭菜，你会：",
        "dimension": "情绪与同理心",
        "tags": ["日常关怀"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "心里很暖，第二天当面感谢", "score": 0},
            {"text": "默默吃完，发条微信说晚安", "score": 0},
            {"text": "觉得这是应该的，没什么特别感觉", "score": 2},
            {"text": "嫌饭菜凉了，有点不高兴", "score": 3},
        ]
    },
    {
        "content": "伴侣突然获得一笔意外之财，你会建议他/她：",
        "dimension": "权责对等",
        "tags": ["财务管理"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "先存起来，作为家庭应急基金", "score": 0},
            {"text": "一部分存起来，一部分犒劳自己和家人", "score": 0},
            {"text": "全部用来买一直想要的东西", "score": 2},
            {"text": "拿出来投资高风险项目博一把", "score": 3},
        ]
    },
    {
        "content": "伴侣因为工作压力大对你发了脾气，事后道歉，你会：",
        "dimension": "情绪与同理心",
        "tags": ["包容", "冲突处理"],
        "time_limit": 10,
        "weight": 1.5,
        "options": [
            {"text": "表示理解，安慰对方并一起想办法减压", "score": 0},
            {"text": "接受道歉，但提醒他下次注意方式", "score": 0},
            {"text": "嘴上说没事但心里一直不舒服", "score": 2},
            {"text": "抓住这件事反复翻旧账", "score": 3},
        ]
    },
    {
        "content": "关于双方的社交圈，你认为比较理想的状态是：",
        "dimension": "边界与独立",
        "tags": ["社交边界"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "彼此有自己的社交圈，也融入对方的圈子", "score": 0},
            {"text": "各自保留部分私人朋友，不强行融入", "score": 0},
            {"text": "要求对方的朋友圈必须有自己", "score": 2},
            {"text": "希望对方只和自己玩，不要有太多朋友", "score": 3},
        ]
    },
    {
        "content": "你觉得在亲密关系中，双方应该多久沟通一次「感情状态」？",
        "dimension": "情绪与同理心",
        "tags": ["沟通频率"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "顺其自然，有需要就聊，不刻意", "score": 0},
            {"text": "每周找个固定时间聊一聊近况和感受", "score": 0},
            {"text": "遇到问题才聊，平时不主动提起", "score": 2},
            {"text": "完全不聊，觉得没必要", "score": 3},
        ]
    },
    {
        "content": "看到伴侣和异性同事有说有笑地一起吃饭，你的第一反应是：",
        "dimension": "边界与独立",
        "tags": ["安全感"],
        "time_limit": 10,
        "weight": 1.5,
        "options": [
            {"text": "很正常的工作社交，不会多想", "score": 0},
            {"text": "有一点点在意但选择信任", "score": 0},
            {"text": "回家后旁敲侧击询问对方", "score": 2},
            {"text": "当场走过去打断他们并质问", "score": 3},
        ]
    },
    {
        "content": "你认为什么样的彩礼/嫁妆安排比较合理？",
        "dimension": "权责对等",
        "tags": ["彩礼", "婚姻"],
        "time_limit": 15,
        "weight": 2.0,
        "options": [
            {"text": "双方家庭量力而行，全部给小家庭做启动资金", "score": 0},
            {"text": "走个形式，根据双方条件协商即可", "score": 0},
            {"text": "必须按当地习俗来，不能比别人少", "score": 2},
            {"text": "彩礼是男方诚意的体现，越多越好", "score": 3},
        ]
    },
    {
        "content": "伴侣忘记了你生日，直到当天晚上才想起来，你会：",
        "dimension": "情绪与同理心",
        "tags": ["包容"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "笑着说没关系，但暗示下次要记住", "score": 0},
            {"text": "觉得有点失落但接受道歉", "score": 0},
            {"text": "一整天都闷闷不乐等对方自己发现", "score": 2},
            {"text": "大发脾气，指责对方不在乎自己", "score": 3},
        ]
    },
    {
        "content": "你们的收入差距较大时，你觉得家庭开销应该怎么分担？",
        "dimension": "权责对等",
        "tags": ["经济分担"],
        "time_limit": 15,
        "weight": 1.5,
        "options": [
            {"text": "按收入比例分担，公平合理", "score": 0},
            {"text": "设立共同账户，统一管理收支", "score": 0},
            {"text": "收入高的一方应该多承担", "score": 1},
            {"text": "男人就应该养家，女人赚的钱自己花", "score": 3},
        ]
    },
    {
        "content": "当你情绪低落时，你希望伴侣怎样做？",
        "dimension": "情绪与同理心",
        "tags": ["情感支持"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "安静地陪在身边，等我自己愿意说", "score": 0},
            {"text": "主动关心询问，帮我分析问题", "score": 0},
            {"text": "给我空间暂时不要打扰我", "score": 1},
            {"text": "必须第一时间哄我，不然就是不在乎", "score": 3},
        ]
    },
    {
        "content": "你如何看待婚后的「个人空间」？",
        "dimension": "边界与独立",
        "tags": ["隐私", "空间"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "即使结婚了也需要各自的兴趣爱好和独处时间", "score": 0},
            {"text": "可以各自保留小秘密，但大事要透明", "score": 0},
            {"text": "结婚了就不分彼此，什么都要共享", "score": 2},
            {"text": "对方的一切我都必须知道，包括聊天记录", "score": 3},
        ]
    },
]

def handle_question_counts(headers):
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
        rows = conn.execute("SELECT dimension, COUNT(*) as c FROM questions WHERE status='approved' GROUP BY dimension").fetchall()
        per_dimension = {}
        for r in rows:
            dim = r["dimension"] or "其他"
            per_dimension[dim] = r["c"]
        return json_response({
            "total": total,
            "per_dimension": per_dimension,
        })
    finally:
        conn.close()

def handle_batch_insert_test_questions(headers, body):
    conn = get_db()
    try:
        # Check conditions
        total = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
        if total >= 200:
            return error_response("题库总数已达 200 道上限", 400)
        rows = conn.execute("SELECT dimension, COUNT(*) as c FROM questions WHERE status='approved' GROUP BY dimension").fetchall()
        per_dimension = {}
        for r in rows:
            per_dimension[r["dimension"] or "其他"] = r["c"]
        # Check each dimension from the pool
        dim_counts = {}
        for q in TEST_QUESTIONS_POOL:
            d = q["dimension"]
            dim_counts[d] = dim_counts.get(d, 0) + 1
        for d, cnt in dim_counts.items():
            existing = per_dimension.get(d, 0)
            if existing + cnt > 50:
                return error_response(f"维度「{d}」已接近上限（{existing} 道），无法批量添加 {cnt} 道", 400)
        now = datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        inserted = 0
        for q in TEST_QUESTIONS_POOL:
            qid = uuid.uuid4().hex[:8]
            conn.execute(
                "INSERT INTO questions (id, content, options, dimension, weight, time_limit, status, submitter_id, created_at) VALUES (?, ?, ?, ?, ?, ?, 'approved', 'test_uploader', ?)",
                (qid, q["content"], json.dumps(q["options"], ensure_ascii=False), q["dimension"], q.get("weight", 1.0), q.get("time_limit", 15), now)
            )
            for tag_name in q.get("tags", []):
                tag_name = tag_name.strip()
                if not tag_name:
                    continue
                existing_tag = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
                if existing_tag:
                    tid = existing_tag["id"]
                else:
                    tid = uuid.uuid4().hex[:8]
                    conn.execute("INSERT INTO tags (id, name) VALUES (?, ?)", (tid, tag_name))
                conn.execute("INSERT OR IGNORE INTO question_tags (question_id, tag_id) VALUES (?, ?)", (qid, tid))
            inserted += 1
        conn.commit()
        return json_response({"inserted": inserted, "message": f"成功添加 {inserted} 道测试题目"})
    finally:
        conn.close()

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self._handle("GET")
    def do_POST(self):
        self._handle("POST")
    def do_PATCH(self):
        self._handle("PATCH")
    def do_DELETE(self):
        self._handle("DELETE")
    def do_OPTIONS(self):
        self._cors_headers()
        self.send_response(200)
        self.end_headers()
    
    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
    
    def _handle(self, method):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        
        # API routes
        if path.startswith("/api/"):
            body = None
            if method in ("POST", "PATCH"):
                try:
                    body = read_body(self)
                except Exception:
                    self._send_error(400, "Invalid JSON")
                    return
            
            status, resp_data, content_type = None, None, None
            for m, pat, handler in API_ROUTES:
                if method != m:
                    continue
                match = pat.match(path)
                if match:
                    # Special handling for GET /api/questions with query params
                    if path == "/api/questions" and method == "GET":
                        qs = parse_qs(parsed.query)
                        status, resp_data, content_type = handle_get_questions(self.headers, qs)
                    elif path == "/api/test/verify-token" and method == "GET":
                        qs = parse_qs(parsed.query)
                        status, resp_data, content_type = handle_verify_by_token(self.headers, qs)
                    elif path == "/api/questions/replacement" and method == "GET":
                        qs = parse_qs(parsed.query)
                        status, resp_data, content_type = handle_get_replacement(self.headers, qs)
                    else:
                        args = match.groups()
                        try:
                            status, resp_data, content_type = handler(self.headers, body, *args)
                        except Exception as e:
                            self._send_error(500, f"Internal error: {e}")
                            return
                    break
            
            if status is None:
                self._send_error(404, "Not Found")
            elif isinstance(resp_data, tuple) and len(resp_data) == 3:
                # Already a (status, body, content_type) tuple
                self._send_response(*resp_data)
            else:
                self._send_response(status, resp_data, content_type)
            return
        
        # Static files
        self._serve_static(path)
    
    def _send_response(self, status, body, content_type="application/json; charset=utf-8"):
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    
    def _send_error(self, status, msg):
        body = json.dumps({"detail": msg}, ensure_ascii=False).encode("utf-8")
        self._send_response(status, body)
    
    def _serve_static(self, path):
        if path == "/":
            path = "/index.html"
        filepath = os.path.join(FRONTEND_DIR, path.lstrip("/"))
        filepath = os.path.normpath(filepath)
        # Security: ensure we're still inside frontend dir
        if not filepath.startswith(os.path.normpath(FRONTEND_DIR)):
            self._send_error(403, "Forbidden")
            return
        if not os.path.isfile(filepath):
            self._send_error(404, "File not found")
            return
        ext = os.path.splitext(filepath)[1].lower()
        content_type = MIME_TYPES.get(ext, "application/octet-stream")
        with open(filepath, "rb") as f:
            data = f.read()
        self.send_response(200)
        self._cors_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    
    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]} {args[1]} {args[2]}")

def main():
    init_db()
    # Auto-seed if questions table is empty
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
    conn.close()
    if count == 0:
        print("Questions table empty, auto-seeding...")
        try:
            from seed_questions import QUESTIONS_JSON, rebuild
            rebuild()
        except ImportError:
            print("Warning: seed_questions.py not found, skipping seed.")

    host = os.environ.get("HOST", "0.0.0.0")
    server = http.server.ThreadingHTTPServer((host, PORT), RequestHandler)
    print(f"Server running at http://{host}:{PORT}")
    print(f"Frontend: http://{host}:{PORT}/")
    print(f"API: http://{host}:{PORT}/api/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()

if __name__ == "__main__":
    main()
