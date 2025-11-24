import firebase_admin
from firebase_admin import credentials, firestore
import json
import os

# Path to your Firebase service account key
SERVICE_ACCOUNT_PATH = "./campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json"

# Output JSON filename
OUTPUT_FILE = "filehashed_data.json"

def initialize_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)

def fetch_filehashed_documents():
    db = firestore.client()
    collection_ref = db.collection("fileHashes")
    docs = collection_ref.stream()

    data = []

    for doc in docs:
        doc_dict = doc.to_dict()
        doc_dict["documentId"] = doc.id  # optionally include the Firestore doc ID
        # Convert Firestore timestamp to ISO string if present
        for key in ["uploadedAt", "createdAt"]:
            if key in doc_dict and hasattr(doc_dict[key], "isoformat"):
                doc_dict[key] = doc_dict[key].isoformat()
        data.append(doc_dict)

    return data

def write_to_json(data, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    initialize_firebase()
    filehashed_data = fetch_filehashed_documents()
    write_to_json(filehashed_data, OUTPUT_FILE)
    print(f"Exported {len(filehashed_data)} documents to {OUTPUT_FILE}")
