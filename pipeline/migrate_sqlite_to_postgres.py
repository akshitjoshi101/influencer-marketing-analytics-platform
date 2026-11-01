#may not be used now
import os
import sqlite3
from sqlalchemy import create_engine, text

# ============================================================
# DATABASE CONNECTIONS
# ============================================================

SQLITE_PATH = "instance/myproject.db"
POSTGRES_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres@localhost:5432/influencer_platform",
)

sqlite = sqlite3.connect(SQLITE_PATH)
sqlite.row_factory = sqlite3.Row
postgres = create_engine(POSTGRES_URL)

# ============================================================
# MIGRATE DATA
# ============================================================

with postgres.begin() as db:
    users = sqlite.execute("SELECT * FROM users").fetchall()
    campaigns = sqlite.execute("SELECT * FROM campaigns").fetchall()
    ad_requests = sqlite.execute("SELECT * FROM ad_requests").fetchall()

    for row in users:
        db.execute(
            text("""
                INSERT INTO users
                    (id, username, email, password_hash, role, is_flagged)
                VALUES
                    (:id, :username, :email, :password_hash, :role, :is_flagged)
                ON CONFLICT (id) DO NOTHING
            """),
            dict(row),
        )

    for row in campaigns:
        db.execute(
            text("""
                INSERT INTO campaigns
                    (id, name, description, start_date, end_date,
                     budget, visibility, is_flagged, sponsor_id)
                VALUES
                    (:id, :name, :description, :start_date, :end_date,
                     :budget, :visibility, :is_flagged, :sponsor_id)
                ON CONFLICT (id) DO NOTHING
            """),
            dict(row),
        )

    for row in ad_requests:
        db.execute(
            text("""
                INSERT INTO ad_requests
                    (id, campaign_id, influencer_id, sponsor_id,
                     messages, requirements, payment_amount, status, is_active)
                VALUES
                    (:id, :campaign_id, :influencer_id, :sponsor_id,
                     :messages, :requirements, :payment_amount, :status, :is_active)
                ON CONFLICT (id) DO NOTHING
            """),
            dict(row),
        )

    # ========================================================
    # RESET POSTGRES SEQUENCES
    # ========================================================

    for table in ["users", "campaigns", "ad_requests"]:
        db.execute(
            text(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                f"COALESCE((SELECT MAX(id) FROM {table}), 1), true)"
            )
        )

print("SQLite → PostgreSQL migration completed.")
print(f"Users migrated: {len(users)}")
print(f"Campaigns migrated: {len(campaigns)}")
print(f"Ad requests migrated: {len(ad_requests)}")

sqlite.close()
