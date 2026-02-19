import sqlite3

DB_NAME = "withdraws.db"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS withdraw_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            withdraw_request_id TEXT,
            beneficiary_name TEXT,
            account_number TEXT,
            ifsc_code TEXT,
            amount REAL,
            status INTEGER,
            order_id TEXT,
            payment_method TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("PRAGMA table_info(withdraw_requests)")
    columns = {row[1] for row in cursor.fetchall()}

    if "created_at" not in columns:
        cursor.execute("ALTER TABLE withdraw_requests ADD COLUMN created_at TEXT")
        cursor.execute("""
            UPDATE withdraw_requests
            SET created_at = CURRENT_TIMESTAMP
            WHERE created_at IS NULL
        """)

    if "updated_at" not in columns:
        cursor.execute("ALTER TABLE withdraw_requests ADD COLUMN updated_at TEXT")
        cursor.execute("""
            UPDATE withdraw_requests
            SET updated_at = CURRENT_TIMESTAMP
            WHERE updated_at IS NULL
        """)

    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_withdraw_request_id
        ON withdraw_requests (withdraw_request_id)
    """)

    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_withdraw_requests_updated_at
        AFTER UPDATE ON withdraw_requests
        FOR EACH ROW
        WHEN NEW.updated_at = OLD.updated_at
        BEGIN
            UPDATE withdraw_requests
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
    """)

    # Status 0:Created, 1:Processing, 2:Success, 3:Failed

    conn.commit()
    conn.close()


def insert_withdraw(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO withdraw_requests (
            withdraw_request_id,
            beneficiary_name,
            account_number,
            ifsc_code,
            amount,
            status,
            order_id,
            payment_method,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(withdraw_request_id) DO UPDATE SET
            beneficiary_name = excluded.beneficiary_name,
            account_number = excluded.account_number,
            ifsc_code = excluded.ifsc_code,
            amount = excluded.amount,
            status = excluded.status,
            order_id = excluded.order_id,
            payment_method = excluded.payment_method,
            updated_at = CURRENT_TIMESTAMP
    """, (
        data["withdraw_request_id"],
        data["beneficiary_name"],
        data["account_number"],
        data["ifsc_code"],
        data["amount"],
        data.get("status", 0),
        data.get("order_id", ""),
        data.get("payment_method", "")
    ))

    conn.commit()
    conn.close()
