import firebase_admin
from firebase_admin import credentials, firestore
from tenacity import retry, stop_after_attempt, wait_exponential

firebase_app = None  # Global Firebase app instance

def initialize_firebase(credentials_path):
    """Initializes Firebase only once."""
    global firebase_app
    if not firebase_admin._apps:  # Prevent multiple initializations
        try:
            cred = credentials.Certificate(credentials_path)
            firebase_app = firebase_admin.initialize_app(cred)
        except Exception as e:
            print(f"Failed to initialize Firebase: {e}")
            return None
    return firestore.client()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_course_documents(courses_ref):
    """Fetches course documents with retry logic."""
    return list(courses_ref.list_documents())  # Uses list_documents() instead of stream()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_sub_collection_documents(sub_collection_ref):
    """Fetches sub-collection documents with retry logic."""
    return list(sub_collection_ref.stream())  # Materializes the stream

def update_course_totals(db):
    """Updates total_papers for each course, starting from SOC 3308."""
    if not db:
        return
    
    courses_ref = db.collection("courses")
    try:
        courses = get_course_documents(courses_ref)  # Uses retry-wrapped function
        resume_from = "SOC 3308"  # Start from here
        start_updating = False

        for course in courses:
            course_id = course.id

            # Start processing when we reach SOC 3308
            if course_id == resume_from:
                start_updating = True

            if not start_updating:
                continue  # Skip courses before SOC 3308

            total_papers = 0
            
            # Get existing document data
            course_doc = course.get()
            if not course_doc.exists:
                print(f"Warning: Course {course_id} document doesn't exist")
                continue

            for sub_collection in ["mid-semester", "end-semester", "quizzes"]:
                try:
                    sub_collection_ref = course.collection(sub_collection)
                    papers = get_sub_collection_documents(sub_collection_ref)  # Uses retry-wrapped function

                    # Exclude placeholder doc
                    real_papers = [doc for doc in papers if doc.id != "placeholder"]
                    total_papers += len(real_papers)
                except Exception as e:
                    print(f"Error processing {sub_collection} for {course_id}: {e}")
                    continue
            
            try:
                # Update course document with total_papers field
                course.update({"total_papers": total_papers})
                print(f"Updated {course_id} with total_papers: {total_papers}")
            except Exception as e:
                print(f"Failed to update {course_id}: {e}")

    except Exception as e:
        print(f"Failed to stream courses: {e}")

if __name__ == "__main__":
    db = initialize_firebase("campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")
    if db:
        update_course_totals(db)
