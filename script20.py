import firebase_admin
from firebase_admin import credentials, firestore
import os

# --- Configuration ---
# Replace with the path to your Firebase service account key file
# It's good practice to store this securely, e.g., using environment variables
SERVICE_ACCOUNT_KEY_PATH = './campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json' 

# Your Firebase project ID (optional, often inferred from the key file)
# PROJECT_ID = 'your-project-id'

COLLECTION_NAME = 'fileHashes'
OLD_FIELD_NAME = 'uploadedAt'
NEW_FIELD_NAME = 'createdAt'

# --- Initialize Firebase ---
try:
    if not os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
        raise FileNotFoundError(
            f"Service account key file not found at: {SERVICE_ACCOUNT_KEY_PATH}\n"
            "Please update SERVICE_ACCOUNT_KEY_PATH with the correct path."
        )

    cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
    # If your project ID isn't inferred correctly, you can specify it:
    # firebase_admin.initialize_app(cred, {'projectId': PROJECT_ID})
    firebase_admin.initialize_app(cred)
    print("Firebase initialized successfully.")
except Exception as e:
    print(f"Error initializing Firebase: {e}")
    print("Please ensure your service account key path is correct and the file is valid.")
    exit()

db = firestore.client()

# --- Update Function ---
def rename_field_in_collection(collection_name, old_field, new_field):
    print(f"\nStarting migration for collection: '{collection_name}'")
    print(f"Renaming field from '{old_field}' to '{new_field}'...")

    updated_count = 0
    skipped_count = 0
    error_count = 0

    docs_ref = db.collection(collection_name)
    
    # Get all documents in the collection
    # For very large collections, consider using pagination (start_after)
    # to avoid memory issues and timeouts.
    docs = docs_ref.stream()

    for doc in docs:
        doc_id = doc.id
        doc_data = doc.to_dict()

        if old_field in doc_data:
            try:
                # Get the value of the old field
                field_value = doc_data[old_field]

                # Create a batch write for atomic updates
                batch = db.batch()

                # Set the new field with the old value
                batch.update(docs_ref.document(doc_id), {new_field: field_value})
                
                # Delete the old field
                batch.update(docs_ref.document(doc_id), {old_field: firestore.DELETE_FIELD})
                
                batch.commit()
                updated_count += 1
                print(f"  Updated document: {doc_id}")
            except Exception as e:
                error_count += 1
                print(f"  Error updating document {doc_id}: {e}")
        else:
            skipped_count += 1
            # print(f"  Skipped document {doc_id}: '{old_field}' not found.")

    print("\n--- Migration Summary ---")
    print(f"Documents updated: {updated_count}")
    print(f"Documents skipped (field not found): {skipped_count}")
    print(f"Documents with errors: {error_count}")
    print("Migration complete.")

# --- Run the migration ---
if __name__ == "__main__":
    # --- IMPORTANT: Backup your database before running this script! ---
    print("\nWARNING: This script will modify your Firestore data.")
    print("It is highly recommended to BACK UP YOUR DATABASE before proceeding.")
    confirmation = input("Type 'yes' to confirm you have backed up your data and wish to proceed: ")

    if confirmation.lower() == 'yes':
        rename_field_in_collection(COLLECTION_NAME, OLD_FIELD_NAME, NEW_FIELD_NAME)
    else:
        print("Migration cancelled by user.")