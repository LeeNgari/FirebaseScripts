import firebase_admin
from firebase_admin import credentials, firestore
from typing import Dict, List, Optional

def initialize_firebase(credentials_path: str) -> Optional[firestore.Client]:
    """
    Initialize Firebase connection
    """
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(credentials_path)
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        print(f"Failed to initialize Firebase: {e}")
        return None

def normalize_code(code: str) -> str:
    """
    Normalize course code by removing spaces and converting to lowercase
    """
    return code.replace(' ', '').lower()

def generate_searchable_fields(course_name: str, normalized_code: str) -> List[str]:
    """
    Generate searchable fields from course data
    """
    course_name_tokens = course_name.lower().split()
    normalized_code_tokens = [normalized_code.lower()]
    numeric_tokens = ["".join(filter(str.isdigit, normalized_code))]
    
    searchable_fields = set(course_name_tokens + normalized_code_tokens + numeric_tokens)
    return list(searchable_fields)

def add_course(db: firestore.Client, course_data: Dict) -> bool:
    """
    Add a new course to Firestore
    
    Args:
        db: Firestore client
        course_data: Dictionary containing course information with keys:
            - course_code: Course code
            - course_name: Name of the course
            - school: School/department offering the course
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        course_code = course_data.get('course_code')
        course_name = course_data.get('course_name')
        school = course_data.get('school')
        
        # Validate required fields
        if not all([course_code, course_name, school]):
            print("Error: Missing required fields (course_code, course_name, or school)")
            return False
            
        # Check if course already exists
        course_ref = db.collection('courses').document(course_code)
        if course_ref.get().exists:
            print(f"Course {course_code} already exists")
            return False
            
        # Generate normalized code and searchable fields
        normalized_code = normalize_code(course_code)
        searchable_fields = generate_searchable_fields(course_name, normalized_code)
        
        # Prepare course document data
        course_document = {
            'course_name': course_name,
            'normalized_code': normalized_code,
            'school': school,
            'course_name_lowercase': course_name.lower(),
            'searchable_fields': searchable_fields,
            'total_papers': 0  # Initialize with 0 papers
        }
        
        # Create course document
        course_ref.set(course_document)
        
        # Create placeholder documents in subcollections
        subcollections = ['end-semester', 'mid-semester', 'quizzes']
        for subcollection in subcollections:
            course_ref.collection(subcollection).document('placeholder').set({})
        
        print(f"Successfully added course: {course_code} - {course_name}")
        return True
        
    except Exception as e:
        print(f"Error adding course: {e}")
        return False

def main():
    # Initialize Firebase
    db = initialize_firebase("campus-aid-webg-firebase-adminsdk-fbsvc-d6b27736e3.json")
    if not db:
        return
    
    while True:
        print("\n=== Add New Course ===")
        print("(Press Enter without input to exit)")
        
        # Get course information
        course_code = input("Enter course code: ").strip()
        if not course_code:
            break
            
        course_name = input("Enter course name: ").strip()
        if not course_name:
            break
            
        school = input("Enter school/department: ").strip()
        if not school:
            break
        
        # Prepare course data
        course_data = {
            'course_code': course_code,
            'course_name': course_name,
            'school': school
        }
        
        # Add course to Firestore
        add_course(db, course_data)
        
        # Ask if user wants to add another course
        if input("\nAdd another course? (y/n): ").lower() != 'y':
            break
    
    print("\nThank you for using the course addition tool!")

if __name__ == "__main__":
    main()