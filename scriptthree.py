import firebase_admin
from firebase_admin import credentials, firestore
import os

# Path to your Firebase service account key
# Replace with the path to your service account JSON file
cred_path = 'linguo-cbb63-firebase-adminsdk-9earb-832e97d49c.json'

# Initialize Firebase Admin SDK
cred = credentials.Certificate(cred_path)
firebase_admin.initialize_app(cred)

# Get Firestore client
db = firestore.client()

def update_course_documents():
    # Reference to the courses collection
    courses_ref = db.collection('courses')

    # Fetch all documents
    docs = courses_ref.stream()

    # Batch to handle updates
    batch = db.batch()
    update_count = 0

    for doc in docs:
        # Get the document data
        doc_dict = doc.to_dict()

        # Prepare update data
        update_data = {
            'course_name_lowercase': doc_dict.get('course_name', '').lower()
        }

        # Add update to batch
        batch.update(courses_ref.document(doc.id), update_data)
        update_count += 1

        # Firestore batch writes are limited to 500 operations
        if update_count % 500 == 0:
            # Commit the batch
            batch.commit()
            print(f"Committed batch of 500 updates. Total updated: {update_count}")
            # Start a new batch
            batch = db.batch()

    # Commit any remaining updates
    if update_count % 500 != 0:
        batch.commit()

    print(f"Total documents updated: {update_count}")

def main():
    try:
        update_course_documents()
        print("Document update process completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()