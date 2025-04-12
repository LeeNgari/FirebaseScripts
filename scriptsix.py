import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
cred = credentials.Certificate("collectionscript\campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")  # Replace with your service account key path
firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()

# Reference to the lecturers collection
lecturers_ref = db.collection("lecturers")

def update_lecturers():
    try:
        # Fetch all documents in the lecturers collection
        docs = lecturers_ref.stream()

        for doc in docs:
            doc_data = doc.to_dict()
            lecturer_name = doc_data.get("name", "")

            # Generate lowercaseName and searchableFields
            lowercase_name = lecturer_name.lower()
            searchable_fields = lowercase_name.split(" ")

            # Update the document with new fields
            doc.reference.update({
                "lowercaseName": lowercase_name,
                "searchableFields": searchable_fields
            })

            print(f"Updated document {doc.id} with lowercaseName: {lowercase_name} and searchableFields: {searchable_fields}")

        print("All documents updated successfully!")

    except Exception as e:
        print(f"Error updating documents: {e}")

# Run the update function
update_lecturers()