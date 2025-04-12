import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firestore
cred = credentials.Certificate("linguo-cbb63-firebase-adminsdk-9earb-832e97d49c.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# Function to generate searchable fields
def generate_searchable_fields(course_data):
    # Extract relevant fields
    course_name = course_data.get("course_name", "")
    normalized_code = course_data.get("normalized_code", "")

    # Generate tokens from course_name and normalized_code
    course_name_tokens = course_name.lower().split()  # Split by spaces
    normalized_code_tokens = [normalized_code.lower()]

    # Extract numeric substrings from normalized_code
    numeric_tokens = ["".join(filter(str.isdigit, normalized_code))]

    # Combine all tokens into searchable_fields
    searchable_fields = set(course_name_tokens + normalized_code_tokens + numeric_tokens)

    return list(searchable_fields)

# Firestore collection name
collection_name = "courses"

# Update documents in Firestore
def update_searchable_fields():
    courses_collection = db.collection(collection_name)
    
    try:
        # Fetch all documents in the collection
        docs = courses_collection.stream()

        for doc in docs:
            data = doc.to_dict()

            # Generate searchable fields
            searchable_fields = generate_searchable_fields(data)

            # Update the document with searchable fields
            courses_collection.document(doc.id).update({
                "searchable_fields": searchable_fields
            })

            print(f"Updated document {doc.id} with searchable_fields: {searchable_fields}")

    except Exception as e:
        print(f"Error updating documents: {e}")

if __name__ == "__main__":
    update_searchable_fields()
