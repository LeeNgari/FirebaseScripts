import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import time

# Initialize Firebase (replace path with your service account key file)
cred = credentials.Certificate("campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")
firebase_admin.initialize_app(cred)

# Get Firestore client
db = firestore.client()

# Reference to users collection
users_ref = db.collection('users')

# Get all users and store their current coin values
users = list(users_ref.stream())
before_coins = {}
for user in users:
    user_data = user.to_dict()
    before_coins[user.id] = user_data.get('coins', 0)

# Counter for modified documents
modified_count = 0

# Update each user's coins
batch = db.batch()
batch_size = 0
max_batch_size = 500  # Firestore has a limit of 500 operations per batch

for user in users:
    user_ref = users_ref.document(user.id)
    
    # Use increment operation to add 10 coins
    batch.update(user_ref, {'coins': firestore.Increment(10)})
    batch_size += 1
    modified_count += 1
    
    # Commit batch when reaching max size
    if batch_size >= max_batch_size:
        batch.commit()
        print(f"Committed batch of {batch_size} updates")
        batch = db.batch()  # Create new batch
        batch_size = 0
        time.sleep(1)  # Small delay to avoid hitting rate limits

# Commit any remaining updates
if batch_size > 0:
    batch.commit()
    print(f"Committed final batch of {batch_size} updates")

print(f"Updated coins for {modified_count} users")

# Get updated user data and display before and after values
print("\nSample of updated users:")
sample_users = users_ref.limit(5).stream()
for user in sample_users:
    user_data = user.to_dict()
    user_id = user.id
    before_value = before_coins.get(user_id, "unknown")
    after_value = user_data.get('coins', 0)
    print(f"User ID: {user_id}, Before coins: {before_value}, After coins: {after_value}")