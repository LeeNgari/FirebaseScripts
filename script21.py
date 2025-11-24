import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta, timezone
import json
import sys
import time
import os
from typing import Any, Dict, Union

# ---------------------------
# CONFIGURATION
# ---------------------------

SERVICE_ACCOUNT_FILE = "./campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json"
LOG_FILE = "coin_transactions_log.json"

# Initialize Firebase once
if not firebase_admin._apps:
    cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ---------------------------
# HELPER FUNCTIONS
# ---------------------------

def log(msg: str) -> None:
    """Print log messages with timestamp."""
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}")
    sys.stdout.flush()

def convert_timestamps(obj: Any) -> Any:
    """
    Recursively convert Firestore Timestamps and datetimes to ISO strings.
    Works for dicts, lists, and single values.
    """
    if isinstance(obj, dict):
        return {k: convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_timestamps(i) for i in obj]
    elif hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    else:
        return obj

# ---------------------------
# MAIN LOGIC
# ---------------------------

def fetch_recent_coin_transactions_streaming() -> None:
    """Fetch coin transactions and log them as they are retrieved."""
    now = datetime.now(timezone.utc)
    seven_days_ago = now - timedelta(days=7)

    log(f"🔍 Fetching transactions from {seven_days_ago.isoformat()} to {now.isoformat()}")

    users_ref = db.collection("users")
    users = list(users_ref.stream())
    log(f"👥 Found {len(users)} users\n")

    # Initialize the log file with an opening bracket for JSON array
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("[\n")

    total_logged = 0
    first_entry = True
    start_time = time.time()

    for i, user_doc in enumerate(users, start=1):
        user_id = user_doc.id
        log(f"➡️  [{i}/{len(users)}] Checking user: {user_id}")

        transactions_ref = users_ref.document(user_id).collection("coinTransactions")
        query = transactions_ref.where("timestamp", ">=", seven_days_ago)
        transactions = list(query.stream())

        if not transactions:
            log("   ⚪ No recent transactions.\n")
            continue

        for tx in transactions:
            raw_data: Dict[str, Any] = tx.to_dict()
            raw_data["userId"] = user_id
            raw_data["transactionId"] = tx.id

            data: Dict[str, Any] = convert_timestamps(raw_data)

            # Safely get timestamp string for log line
            timestamp_str = data["timestamp"] if isinstance(data.get("timestamp"), str) else "No timestamp"
            log(f"   💰 Transaction {tx.id} | {timestamp_str}")
            for k, v in data.items():
                log(f"      {k}: {v}")
            print()

            # Append transaction immediately to JSON file
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                if not first_entry:
                    f.write(",\n")
                json.dump(data, f, ensure_ascii=False, indent=4)
                first_entry = False

            total_logged += 1

    # Close the JSON array properly
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n]\n")

    elapsed = time.time() - start_time
    log(f"✅ Completed fetching in {elapsed:.2f} seconds.")
    log(f"📁 Logged {total_logged} transactions to {LOG_FILE}")


# ---------------------------
# ENTRY POINT
# ---------------------------

if __name__ == "__main__":
    try:
        fetch_recent_coin_transactions_streaming()
        log("🎉 Done.\n")
    except Exception as e:
        log(f"❌ ERROR: {e}")
        sys.exit(1)
