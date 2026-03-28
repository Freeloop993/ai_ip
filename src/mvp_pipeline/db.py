import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional


STATUSES = {
    "collected",
    "analyzing",
    "evaluated",
    "producing",
    "pending_review",
    "pending_rework",
    "publishing",
    "published",
    "tracking",
    "failed",
}


RETRY_STATUS = {
    "pending",
    "running",
    "succeeded",
    "dead",
}


def now_dt() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_dt().isoformat()


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS content_item (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    video_id TEXT NOT NULL,
                    video_url TEXT NOT NULL,
                    author TEXT,
                    stats_json TEXT NOT NULL DEFAULT '{}',
                    analysis_json TEXT NOT NULL DEFAULT '{}',
                    production_json TEXT NOT NULL DEFAULT '{}',
                    replicate INTEGER NOT NULL DEFAULT 0,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    run_id TEXT,
                    error_code TEXT,
                    feishu_record_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(platform, video_id)
                );

                CREATE TABLE IF NOT EXISTS task_run (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    content_id INTEGER NOT NULL,
                    agent TEXT NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_code TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(content_id) REFERENCES content_item(id)
                );

                CREATE TABLE IF NOT EXISTS publish_record (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content_id INTEGER NOT NULL,
                    decision TEXT NOT NULL,
                    platform TEXT,
                    publish_url TEXT,
                    external_id TEXT,
                    published_at TEXT,
                    review_feedback TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(content_id) REFERENCES content_item(id)
                );

                CREATE TABLE IF NOT EXISTS metrics_snapshot (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content_id INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    plays INTEGER,
                    likes INTEGER,
                    comments INTEGER,
                    followers INTEGER,
                    captured_at TEXT NOT NULL,
                    FOREIGN KEY(content_id) REFERENCES content_item(id)
                );

                CREATE TABLE IF NOT EXISTS state_transition (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content_id INTEGER NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    source TEXT NOT NULL,
                    run_id TEXT,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(content_id) REFERENCES content_item(id)
                );

                CREATE TABLE IF NOT EXISTS external_event (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    video_id TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS retry_job (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_type TEXT NOT NULL,
                    dedupe_key TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 5,
                    next_run_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    dead_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_task_run_run_id_agent ON task_run(run_id, agent);
                CREATE INDEX IF NOT EXISTS idx_content_status ON content_item(status);
                CREATE INDEX IF NOT EXISTS idx_retry_due ON retry_job(status, next_run_at);
                CREATE INDEX IF NOT EXISTS idx_retry_dedupe ON retry_job(dedupe_key, status);
                """
            )

    def get_content_by_key(self, platform: str, video_id: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM content_item WHERE platform = ? AND video_id = ?",
                (platform, video_id),
            ).fetchone()

    def get_content(self, content_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM content_item WHERE id = ?", (content_id,)).fetchone()

    def get_content_by_feishu_record(self, record_id: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM content_item WHERE feishu_record_id = ?", (record_id,)).fetchone()

    def insert_content(
        self,
        *,
        platform: str,
        video_id: str,
        video_url: str,
        author: str,
        stats: Dict[str, Any],
        source: str,
        status: str,
        run_id: str,
    ) -> int:
        ts = now_iso()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO content_item(
                    platform, video_id, video_url, author, stats_json, source, status,
                    run_id, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    platform,
                    video_id,
                    video_url,
                    author,
                    json.dumps(stats, ensure_ascii=False),
                    source,
                    status,
                    run_id,
                    ts,
                    ts,
                ),
            )
            content_id = int(cursor.lastrowid)
            conn.execute(
                """
                INSERT INTO state_transition(content_id, from_status, to_status, source, run_id, note, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (content_id, None, status, source, run_id, "initial insert", ts),
            )
            return content_id

    def set_content_analysis(self, content_id: int, result: Dict[str, Any], replicate: bool) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE content_item
                SET analysis_json = ?, replicate = ?, updated_at = ?
                WHERE id = ?
                """,
                (json.dumps(result, ensure_ascii=False), 1 if replicate else 0, now_iso(), content_id),
            )

    def set_content_production(self, content_id: int, result: Dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE content_item
                SET production_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (json.dumps(result, ensure_ascii=False), now_iso(), content_id),
            )

    def override_content_production_text(
        self,
        content_id: int,
        *,
        script: Optional[str] = None,
        tts_text: Optional[str] = None,
    ) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT production_json FROM content_item WHERE id = ?", (content_id,)).fetchone()
            if not row:
                return False
            production = json.loads(row["production_json"] or "{}")
            changed = False
            if script is not None and production.get("script") != script:
                production["script"] = script
                changed = True
            if tts_text is not None and production.get("tts_text") != tts_text:
                production["tts_text"] = tts_text
                changed = True
            if changed:
                conn.execute(
                    """
                    UPDATE content_item
                    SET production_json = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (json.dumps(production, ensure_ascii=False), now_iso(), content_id),
                )
            return changed

    def update_status(
        self,
        *,
        content_id: int,
        to_status: str,
        source: str,
        run_id: Optional[str],
        note: str,
        error_code: Optional[str] = None,
    ) -> None:
        if to_status not in STATUSES:
            raise ValueError(f"invalid status: {to_status}")

        ts = now_iso()
        with self.connect() as conn:
            row = conn.execute("SELECT status FROM content_item WHERE id = ?", (content_id,)).fetchone()
            if not row:
                raise ValueError(f"content not found: {content_id}")
            from_status = row["status"]
            conn.execute(
                """
                UPDATE content_item
                SET status = ?, run_id = COALESCE(?, run_id), error_code = ?, updated_at = ?
                WHERE id = ?
                """,
                (to_status, run_id, error_code, ts, content_id),
            )
            conn.execute(
                """
                INSERT INTO state_transition(content_id, from_status, to_status, source, run_id, note, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (content_id, from_status, to_status, source, run_id, note, ts),
            )

    def set_feishu_record(self, content_id: int, record_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE content_item SET feishu_record_id = ?, updated_at = ? WHERE id = ?",
                (record_id, now_iso(), content_id),
            )

    def upsert_task_run(
        self,
        *,
        run_id: str,
        content_id: int,
        agent: str,
        source: str,
        status: str,
        payload: Dict[str, Any],
        result: Optional[Dict[str, Any]] = None,
        error_code: Optional[str] = None,
        finished: bool = False,
    ) -> None:
        ts = now_iso()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT id FROM task_run WHERE run_id = ? AND agent = ?",
                (run_id, agent),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE task_run
                    SET status = ?, error_code = ?, payload_json = ?, result_json = ?,
                        ended_at = CASE WHEN ? THEN ? ELSE ended_at END,
                        updated_at = ?
                    WHERE run_id = ? AND agent = ?
                    """,
                    (
                        status,
                        error_code,
                        json.dumps(payload, ensure_ascii=False),
                        json.dumps(result or {}, ensure_ascii=False),
                        1 if finished else 0,
                        ts,
                        ts,
                        run_id,
                        agent,
                    ),
                )
                return

            conn.execute(
                """
                INSERT INTO task_run(
                    run_id, content_id, agent, source, status, error_code,
                    payload_json, result_json, started_at, ended_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    content_id,
                    agent,
                    source,
                    status,
                    error_code,
                    json.dumps(payload, ensure_ascii=False),
                    json.dumps(result or {}, ensure_ascii=False),
                    ts,
                    ts if finished else None,
                    ts,
                ),
            )

    def has_task_for_content(self, content_id: int, agent: str, statuses: Iterable[str]) -> bool:
        placeholders = ",".join(["?" for _ in statuses])
        params = [content_id, agent, *list(statuses)]
        with self.connect() as conn:
            row = conn.execute(
                f"SELECT 1 FROM task_run WHERE content_id = ? AND agent = ? AND status IN ({placeholders}) LIMIT 1",
                params,
            ).fetchone()
        return row is not None

    def latest_task_result(self, content_id: int, agent: str) -> Dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT result_json FROM task_run
                WHERE content_id = ? AND agent = ?
                ORDER BY id DESC LIMIT 1
                """,
                (content_id, agent),
            ).fetchone()
        if not row:
            return {}
        return json.loads(row["result_json"] or "{}")

    def add_publish_record(
        self,
        *,
        content_id: int,
        decision: str,
        platform: Optional[str],
        publish_url: Optional[str],
        external_id: Optional[str],
        review_feedback: Optional[str],
    ) -> None:
        ts = now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO publish_record(
                    content_id, decision, platform, publish_url, external_id, published_at,
                    review_feedback, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    content_id,
                    decision,
                    platform,
                    publish_url,
                    external_id,
                    ts if decision == "approved" and publish_url else None,
                    review_feedback,
                    ts,
                ),
            )

    def has_published_record(self, content_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM publish_record WHERE content_id = ? AND decision = 'approved' AND publish_url IS NOT NULL LIMIT 1",
                (content_id,),
            ).fetchone()
        return row is not None

    def get_external_event(self, event_id: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM external_event WHERE event_id = ?", (event_id,)).fetchone()

    def insert_external_event(
        self,
        *,
        event_id: str,
        source: str,
        platform: str,
        video_id: str,
        payload: Dict[str, Any],
        status: str,
        note: str = "",
    ) -> None:
        ts = now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO external_event(event_id, source, platform, video_id, payload_json, status, note, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    source,
                    platform,
                    video_id,
                    json.dumps(payload, ensure_ascii=False),
                    status,
                    note,
                    ts,
                    ts,
                ),
            )

    def update_external_event(self, event_id: str, status: str, note: str = "") -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE external_event SET status = ?, note = ?, updated_at = ? WHERE event_id = ?",
                (status, note, now_iso(), event_id),
            )

    def enqueue_retry_job(
        self,
        *,
        job_type: str,
        dedupe_key: str,
        payload: Dict[str, Any],
        max_attempts: int,
        delay_seconds: int = 0,
    ) -> int:
        with self.connect() as conn:
            existing = conn.execute(
                """
                SELECT id FROM retry_job
                WHERE dedupe_key = ? AND status IN ('pending', 'running')
                ORDER BY id DESC LIMIT 1
                """,
                (dedupe_key,),
            ).fetchone()
            if existing:
                return int(existing["id"])

            ts = now_dt()
            next_run = (ts + timedelta(seconds=delay_seconds)).isoformat()
            cur = conn.execute(
                """
                INSERT INTO retry_job(
                    job_type, dedupe_key, payload_json, attempts, max_attempts,
                    next_run_at, status, last_error, created_at, updated_at, dead_at
                ) VALUES(?, ?, ?, 0, ?, ?, 'pending', NULL, ?, ?, NULL)
                """,
                (
                    job_type,
                    dedupe_key,
                    json.dumps(payload, ensure_ascii=False),
                    max_attempts,
                    next_run,
                    ts.isoformat(),
                    ts.isoformat(),
                ),
            )
            return int(cur.lastrowid)

    def list_due_retry_jobs(self, limit: int = 20) -> list[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM retry_job
                WHERE status = 'pending' AND next_run_at <= ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (now_iso(), limit),
            ).fetchall()
        items = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d.pop("payload_json") or "{}")
            items.append(d)
        return items

    def mark_retry_running(self, job_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE retry_job SET status = 'running', updated_at = ? WHERE id = ?",
                (now_iso(), job_id),
            )

    def mark_retry_succeeded(self, job_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE retry_job SET status = 'succeeded', updated_at = ? WHERE id = ?",
                (now_iso(), job_id),
            )

    def mark_retry_failed(self, job_id: int, *, error: str, base_delay_seconds: int) -> None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT attempts, max_attempts FROM retry_job WHERE id = ?",
                (job_id,),
            ).fetchone()
            if not row:
                return
            attempts = int(row["attempts"]) + 1
            max_attempts = int(row["max_attempts"])
            if attempts >= max_attempts:
                conn.execute(
                    """
                    UPDATE retry_job
                    SET attempts = ?, status = 'dead', last_error = ?, dead_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (attempts, error, now_iso(), now_iso(), job_id),
                )
                return

            delay = base_delay_seconds * (2 ** max(0, attempts - 1))
            next_run = (now_dt() + timedelta(seconds=delay)).isoformat()
            conn.execute(
                """
                UPDATE retry_job
                SET attempts = ?, status = 'pending', last_error = ?, next_run_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (attempts, error, next_run, now_iso(), job_id),
            )

    def list_dead_retry_jobs(self) -> list[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM retry_job WHERE status = 'dead' ORDER BY id DESC"
            ).fetchall()
        items = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d.pop("payload_json") or "{}")
            items.append(d)
        return items

    def list_content(self) -> list[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, platform, video_id, video_url, author, status, source, run_id,
                       error_code, feishu_record_id, replicate, created_at, updated_at
                FROM content_item ORDER BY id DESC
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def count_content_by_status(self) -> Dict[str, int]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM content_item
                GROUP BY status
                """
            ).fetchall()
        return {str(r["status"]): int(r["cnt"]) for r in rows}

    def total_content_count(self) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM content_item").fetchone()
        return int(row["cnt"]) if row else 0

    def list_agent_status(self) -> list[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT latest.agent,
                       latest.status AS last_status,
                       latest.updated_at AS last_updated_at,
                       (
                         SELECT COUNT(*)
                         FROM task_run running
                         WHERE running.agent = latest.agent
                           AND running.status = 'running'
                       ) AS running_count
                FROM task_run latest
                JOIN (
                    SELECT agent, MAX(id) AS max_id
                    FROM task_run
                    GROUP BY agent
                ) grouped ON grouped.max_id = latest.id
                ORDER BY latest.agent ASC
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def list_recent_task_runs(self, limit: int = 50) -> list[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, run_id, content_id, agent, source, status, error_code,
                       started_at, ended_at, updated_at
                FROM task_run
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_recent_task_runs_with_json(self, limit: int = 200) -> list[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, run_id, content_id, agent, source, status, error_code,
                       payload_json, result_json, started_at, ended_at, updated_at
                FROM task_run
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        items = []
        for r in rows:
            row = dict(r)
            row["payload"] = json.loads(row.pop("payload_json") or "{}")
            row["result"] = json.loads(row.pop("result_json") or "{}")
            items.append(row)
        return items

    def list_metrics_snapshots(self, limit: int = 200) -> list[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, content_id, platform, plays, likes, comments, followers, captured_at
                FROM metrics_snapshot
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def review_stats(self) -> Dict[str, int]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_reviews,
                    SUM(CASE WHEN decision = 'approved' THEN 1 ELSE 0 END) AS approved_reviews
                FROM publish_record
                WHERE decision IN ('approved', 'rework')
                """
            ).fetchone()
        return {
            "total_reviews": int((row["total_reviews"] if row else 0) or 0),
            "approved_reviews": int((row["approved_reviews"] if row else 0) or 0),
        }

    def list_content_by_status(self, statuses: Iterable[str]) -> list[Dict[str, Any]]:
        statuses = list(statuses)
        if not statuses:
            return []
        placeholders = ",".join(["?" for _ in statuses])
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM content_item WHERE status IN ({placeholders}) ORDER BY id ASC",
                statuses,
            ).fetchall()
        return [dict(r) for r in rows]

    def find_stuck_content(self, statuses: Iterable[str], timeout_minutes: int) -> list[Dict[str, Any]]:
        threshold = (now_dt() - timedelta(minutes=timeout_minutes)).isoformat()
        statuses = list(statuses)
        if not statuses:
            return []
        placeholders = ",".join(["?" for _ in statuses])
        params = [*statuses, threshold]
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM content_item WHERE status IN ({placeholders}) AND updated_at < ? ORDER BY id ASC",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    def get_timeline(self, content_id: int) -> list[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT from_status, to_status, source, run_id, note, created_at
                FROM state_transition
                WHERE content_id = ?
                ORDER BY id ASC
                """,
                (content_id,),
            ).fetchall()
        return [dict(r) for r in rows]
