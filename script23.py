from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

# --- Firestore setup ---
cred = credentials.Certificate("./campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# --- List of usernames extracted from your log ---
refunded_usernames = [
    "muchiriivana",
    "abzakahmed2003",
    "prettymugisa81",
    "wambui",
    "waithakacolleen",
    "clarapeter319",
    "Hhhjj",
    "missnguthiru",
    "Eni.xx",
    "Xen",
    "angie.read",
    "Tj",
    "Tinaoloo",
    "MK",
    "gabrielkitembo4",
    "Ieunice",
    "hamdiissack30",
]

print(f"🎁 Giving +3 bonus coins to {len(refunded_usernames)} users based on username field.\n")

for username in refunded_usernames:
    # Query by username
    users = db.collection("users").where("username", "==", username).stream()

    found = False
    for user_doc in users:
        found = True
        user_id = user_doc.id
        user_data = user_doc.to_dict()
        old_balance = user_data.get("coins", 0)
        new_balance = old_balance + 5

        # Update coins
        user_doc.reference.update({"coins": new_balance})

        # Add transaction to coinTransactions subcollection
        reward_id = f"bonus_{int(datetime.now().timestamp())}_{user_id[:5]}"
        reward_tx = {
            "transactionId": reward_id,
            "type": "bonus_reward",
            "amount": 3,
            "status": "completed",
            "completedAt":datetime.now(timezone.utc).isoformat(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        user_doc.reference.collection("coinTransactions").document(reward_id).set(reward_tx)
        print(f"✅ Added +3 coins to {username} (userId: {user_id}, new balance: {new_balance})")

    if not found:
        print(f"⚠️ No Firestore user found with username '{username}' — skipping.")

print("\n🎉 Bonus coins successfully distributed to all refunded users.")
