import csv
import firebase_admin
from firebase_admin import credentials, firestore


def initialize_firebase(credentials_path):
    try:
        cred = credentials.Certificate(credentials_path)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        print(f"Failed to initialize Firebase: {e}")
        return None

def update_course_totals(db):
    if not db:
        return
    
    courses_ref = db.collection("courses")
    try:
        courses = courses_ref.stream()
        
        for course in courses:
            course_id = course.id
            total_papers = 0
            
            # Get existing document data
            course_doc = courses_ref.document(course_id).get()
            if not course_doc.exists:
                print(f"Warning: Course {course_id} document doesn't exist")
                continue
                
            for sub_collection in ["mid-semester", "end-semester", "quizzes"]:
                try:
                    sub_collection_ref = courses_ref.document(course_id).collection(sub_collection)
                    papers = list(sub_collection_ref.stream())  # Materialize the stream
                    
                    # Exclude placeholder doc
                    real_papers = [doc for doc in papers if doc.id != "placeholder"]
                    total_papers += len(real_papers)
                except Exception as e:
                    print(f"Error processing {sub_collection} for {course_id}: {e}")
                    continue
            
            try:
                # Update course document with total_papers field
                courses_ref.document(course_id).update({"total_papers": total_papers})
                print(f"Updated {course_id} with total_papers: {total_papers}")
            except Exception as e:
                print(f"Failed to update {course_id}: {e}")
    except Exception as e:
        print(f"Failed to stream courses: {e}")

if __name__ == "__main__":
    db = initialize_firebase("campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")
    if db:
        update_course_totals(db)