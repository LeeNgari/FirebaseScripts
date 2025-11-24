import firebase_admin
from firebase_admin import credentials, firestore
import random

# Initialize Firebase
cred = credentials.Certificate('campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json')  # Path to your service account key
firebase_admin.initialize_app(cred)

db = firestore.client()

def add_unique_display_usernames():
    try:
        # Get all users from the users collection
        users_ref = db.collection('users')
        docs = users_ref.stream()
        
        used_display_names = set()
        batch = db.batch()
        batch_count = 0
        max_batch_size = 500  # Firestore batch limit
        
        for doc in docs:
            user_data = doc.to_dict()
            
            # Skip if displayUserName already exists
            if 'displayUserName' in user_data:
                print(f"User {doc.id} already has a displayUserName: {user_data['displayUserName']}")
                continue
            
            # Generate a unique display name
            display_name = None
            attempts = 0
            max_attempts = 10
            
            while attempts < max_attempts:
                attempts += 1
                random_num = random.randint(1, 999999)
                display_name = f"user{random_num}"
                
                if display_name not in used_display_names:
                    break
            
            if attempts >= max_attempts:
                raise Exception(f"Failed to generate unique display name after {max_attempts} attempts")
            
            # Add to used names set
            used_display_names.add(display_name)
            
            # Update the document
            user_ref = users_ref.document(doc.id)
            batch.update(user_ref, {'displayUserName': display_name})
            batch_count += 1
            
            # Commit batch if we reach batch size limit
            if batch_count >= max_batch_size:
                batch.commit()
                print(f"Committed batch of {batch_count} updates")
                batch = db.batch()
                batch_count = 0
        
        # Commit any remaining updates in the batch
        if batch_count > 0:
            batch.commit()
            print(f"Committed final batch of {batch_count} updates")
        
        print("Successfully added display usernames to all users!")
    
    except Exception as e:
        print(f"Error adding display usernames: {e}")

# Run the function
if __name__ == "__main__":
    add_unique_display_usernames()