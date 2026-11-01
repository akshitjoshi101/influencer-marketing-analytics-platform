import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from io import StringIO
from urllib.parse import quote_plus

# ============================================
# PostgreSQL connection (read from local environment; never hard-code credentials)
import os
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

engine = create_engine(DATABASE_URL)


# ============================================
# CSV file paths
# ============================================

USERS_CSV = r"D:\\old\\MAD1-PROJECT\\data\\raw\\raw_users.csv"
CAMPAIGNS_CSV = r"D:\\old\\MAD1-PROJECT\\data\\raw\\raw_campaigns.csv"
AD_REQUESTS_CSV = r"D:\\old\\MAD1-PROJECT\\data\\raw\\raw_ad_requests.csv"


# ============================================
# Load CSV files
# ============================================

users = pd.read_csv(USERS_CSV)
campaigns = pd.read_csv(CAMPAIGNS_CSV)
ad_requests = pd.read_csv(AD_REQUESTS_CSV)

print("CSV files loaded")
print(f"Users: {len(users)}")
print(f"Campaigns: {len(campaigns)}")
print(f"Ad Requests: {len(ad_requests)}")


# ============================================
# Create raw PostgreSQL tables
# ============================================

with engine.begin() as connection:

    connection.execute(text("""
        DROP TABLE IF EXISTS raw_ad_requests
    """))

    connection.execute(text("""
        DROP TABLE IF EXISTS raw_campaigns
    """))

    connection.execute(text("""
        DROP TABLE IF EXISTS raw_users
    """))

    connection.execute(text("""
        CREATE TABLE raw_users (
            id INTEGER,
            username TEXT,
            email TEXT,
            password_hash TEXT,
            role TEXT,
            is_flagged INTEGER
        )
    """))

    connection.execute(text("""
        CREATE TABLE raw_campaigns (
            id INTEGER,
            name TEXT,
            description TEXT,
            sponsor_id INTEGER,
            start_date TEXT,
            end_date TEXT,
            budget DOUBLE PRECISION,
            visibility TEXT
        )
    """))

    connection.execute(text("""
        CREATE TABLE raw_ad_requests (
            id INTEGER,
            campaign_id INTEGER,
            influencer_id INTEGER,
            sponsor_id INTEGER,
            messages TEXT,
            requirements TEXT,
            payment_amount DOUBLE PRECISION,
            status TEXT,
            is_active INTEGER
        )
    """))

print("Raw tables created")


# ============================================
# PostgreSQL bulk insert function
# ============================================

def load_dataframe(df, table_name, columns):
    """
    Load a pandas DataFrame into PostgreSQL
    using PostgreSQL COPY for fast bulk loading.
    """

    buffer = StringIO()

    df[columns].to_csv(
        buffer,
        index=False,
        header=False,
        na_rep="\\N"
    )

    buffer.seek(0)

    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    cursor = connection.cursor()

    try:

        cursor.copy_expert(
            f"""
            COPY {table_name} ({", ".join(columns)})
            FROM STDIN
            WITH (
                FORMAT CSV,
                NULL '\\N'
            )
            """,
            buffer
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()


# ============================================
# Insert Users
# ============================================

load_dataframe(
    users,
    "raw_users",
    [
        "id",
        "username",
        "email",
        "password_hash",
        "role",
        "is_flagged"
    ]
)

print("Users loaded")


# ============================================
# Insert Campaigns
# ============================================

load_dataframe(
    campaigns,
    "raw_campaigns",
    [
        "id",
        "name",
        "description",
        "sponsor_id",
        "start_date",
        "end_date",
        "budget",
        "visibility"
    ]
)

print("Campaigns loaded")


# ============================================
# Insert Ad Requests
# ============================================

load_dataframe(
    ad_requests,
    "raw_ad_requests",
    [
        "id",
        "campaign_id",
        "influencer_id",
        "sponsor_id",
        "messages",
        "payment_amount",
        "status"    ]
)

print("Ad Requests loaded")


# ============================================
# Verify row counts
# ============================================

with engine.connect() as connection:

    users_count = connection.execute(
        text("SELECT COUNT(*) FROM raw_users")
    ).scalar()

    campaigns_count = connection.execute(
        text("SELECT COUNT(*) FROM raw_campaigns")
    ).scalar()

    ad_requests_count = connection.execute(
        text("SELECT COUNT(*) FROM raw_ad_requests")
    ).scalar()


print()
print("============================================")
print("RAW DATA LOAD COMPLETE")
print("============================================")
print(f"raw_users:        {users_count}")
print(f"raw_campaigns:    {campaigns_count}")
print(f"raw_ad_requests:  {ad_requests_count}")
print("============================================")