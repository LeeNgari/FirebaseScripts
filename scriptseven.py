import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase
cred = credentials.Certificate("linguo-cbb63-firebase-adminsdk-9earb-832e97d49c.json") # Replace with your Firebase service account key
firebase_admin.initialize_app(cred)
db = firestore.client()

def update_course_totals():
    courses_ref = db.collection("courses")
    courses = courses_ref.stream()
    
    for course in courses:
        course_id = course.id  # Course name, e.g., "SWE 2020"
        total_papers = 0
        
        for sub_collection in ["mid-semester", "end-semester", "quizzes"]:
            sub_collection_ref = courses_ref.document(course_id).collection(sub_collection)
            papers = sub_collection_ref.stream()
            
            # Exclude placeholder doc
            real_papers = [doc for doc in papers if doc.id != "placeholder"]
            total_papers += len(real_papers)
        
        # Update course document with total_papers field
        courses_ref.document(course_id).update({"total_papers": total_papers})
        print(f"Updated {course_id} with total_papers: {total_papers}")

if __name__ == "__main__":
    update_course_totals()
