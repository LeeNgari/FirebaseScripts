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
    """Updates total_papers, mid_sem_papers, end_sem_papers, and quiz_papers for all courses."""
    if not db:
        return
    
    courses_ref = db.collection("courses")
    
    try:
        courses = get_course_documents(courses_ref)  # Uses retry-wrapped function
        
        for course in courses:
            course_id = course.id
            
            # Initialize counters
            mid_sem_papers = 0
            end_sem_papers = 0
            quiz_papers = 0
            total_papers = 0
            
            # Get existing document data
            course_doc = course.get()
            if not course_doc.exists:
                print(f"Warning: Course {course_id} document doesn't exist")
                continue
            
            # Count papers in each sub-collection
            sub_collections = {
                "mid-semester": "mid_sem_papers",
                "end-semester": "end_sem_papers", 
                "quizzes": "quiz_papers"
            }
            
            counters = {
                "mid_sem_papers": 0,
                "end_sem_papers": 0,
                "quiz_papers": 0
            }
            
            for sub_collection, counter_field in sub_collections.items():
                try:
                    sub_collection_ref = course.collection(sub_collection)
                    papers = get_sub_collection_documents(sub_collection_ref)  # Uses retry-wrapped function
                    
                    # Exclude placeholder doc
                    real_papers = [doc for doc in papers if doc.id != "placeholder"]
                    paper_count = len(real_papers)
                    
                    counters[counter_field] = paper_count
                    total_papers += paper_count
                    
                except Exception as e:
                    print(f"Error processing {sub_collection} for {course_id}: {e}")
                    continue
            
            try:
                # Update course document with all paper counts
                update_data = {
                    "total_papers": total_papers,
                    "mid_sem_papers": counters["mid_sem_papers"],
                    "end_sem_papers": counters["end_sem_papers"],
                    "quiz_papers": counters["quiz_papers"]
                }
                
                course.update(update_data)
                print(f"Updated {course_id}:")
                print(f"  Total papers: {total_papers}")
                print(f"  Mid-semester papers: {counters['mid_sem_papers']}")
                print(f"  End-semester papers: {counters['end_sem_papers']}")
                print(f"  Quiz papers: {counters['quiz_papers']}")
                print("-" * 40)
                
            except Exception as e:
                print(f"Failed to update {course_id}: {e}")
                
    except Exception as e:
        print(f"Failed to stream courses: {e}")

if __name__ == "__main__":
    db = initialize_firebase("campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")
    if db:
        update_course_totals(db)