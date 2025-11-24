import json
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

# --- Firestore setup ---
cred = credentials.Certificate("./campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# --- Load transaction logs ---
with open("coin_transactions_log.json", "r") as f:
    transactions = json.load(f)

# --- Filter downloads that deducted coins but not completed/refunded ---
to_refund = [
    tx for tx in transactions
    if tx.get("type") == "download"
    and tx.get("amount") == -1
    and tx.get("status") not in ("completed", "refunded")
]

print(f"🧾 Found {len(to_refund)} incomplete transactions to refund.\n")

for tx in to_refund:
    user_id = tx.get("userId")
    tx_id = tx.get("transactionId")

    if not user_id or not tx_id:
        print(f"⚠️ Skipping transaction missing userId/transactionId: {tx}")
        continue

    user_ref = db.collection("users").document(user_id)
    user_doc = user_ref.get()

    if not user_doc.exists:
        print(f"⚠️ User {user_id} not found — skipping refund.")
        continue

    user_data = user_doc.to_dict()
    old_balance = user_data.get("coins", 0)
    new_balance = old_balance + 1

    # --- Refund the coin ---
    user_ref.update({"coins": new_balance})

    # --- Update the transaction in user's subcollection ---
    tx_ref = user_ref.collection("coinTransactions").document(tx_id)
    tx_ref.update({
        "status": "refunded",
        "completedAt": datetime.now(timezone.utc).isoformat(),
        "timestamp":datetime.now(timezone.utc).isoformat()
    })

    print(f"✅ Refunded 1 coin to {user_data.get('username', user_id)} "
          f"(new balance: {new_balance}) — tx {tx_id}")

print("\n🎉 Refund process complete.")
