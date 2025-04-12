import firebase_admin
from firebase_admin import credentials, firestore

def initialize_firebase(credentials_path: str) -> firestore.Client:
    """
    Initialize Firebase connection and return Firestore client
    """
    if not firebase_admin._apps:
        cred = credentials.Certificate(credentials_path)
        firebase_admin.initialize_app(cred)
    return firestore.client()

def update_item_count(db: firestore.Client):
    """
    Fetch courses in alphabetical order and update item_count field.
    """
    try:
        courses_ref = db.collection('courses')
        courses = courses_ref.order_by("normalized_code").stream()
        
        for index, course in enumerate(courses, start=1):
            course_ref = courses_ref.document(course.id)
            course_ref.update({"item_count": index})
            print(f"Updated {course.id} with item_count {index}")
        
        print("Successfully updated all courses with item_count.")
    except Exception as e:
        print(f"Error updating item_count: {e}")

if __name__ == "__main__":
    db = initialize_firebase("campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")
    update_item_count(db)
