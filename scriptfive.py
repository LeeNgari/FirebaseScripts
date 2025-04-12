import csv
import firebase_admin
from firebase_admin import credentials, firestore

def normalize_code(code):
    """
    Normalize course code by removing spaces and converting to lowercase
    """
    return code.replace(' ', '').lower()

def generate_searchable_fields(course_data):
    """
    Generate searchable fields from course data
    """
    course_name = course_data.get("course_name", "")
    normalized_code = course_data.get("normalized_code", "")

    course_name_tokens = course_name.lower().split()
    normalized_code_tokens = [normalized_code.lower()]
    numeric_tokens = ["".join(filter(str.isdigit, normalized_code))]

    searchable_fields = set(course_name_tokens + normalized_code_tokens + numeric_tokens)
    return list(searchable_fields)

def import_and_update_courses(csv_file_path):
    """
    Import courses from CSV to Firebase Firestore and update documents
    """
    # Initialize Firebase Admin SDK
    cred = credentials.Certificate('campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json')
    firebase_admin.initialize_app(cred)
    
    # Get Firestore client
    db = firestore.client()
    
    # Dictionary to track processed courses to avoid duplicates
    processed_courses = set()
    
    # Open and read the CSV file
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.DictReader(csvfile)
        
        for row in csvreader:
            course_code = row['Course Code']
            course_name = row['Course Name']
            school = row['source']
            
            # Skip if course has already been processed or empty course code
            if not course_code or course_code in processed_courses:
                continue
            
            # Normalize the course code
            normalized_code = normalize_code(course_code)
            
            # Reference to the course document
            course_ref = db.collection('courses').document(course_code)
            
            # Set course document data
            course_data = {
                'course_name': course_name,
                'normalized_code': normalized_code,
                'school': school,
                'course_name_lowercase': course_name.lower(),
                'searchable_fields': generate_searchable_fields({
                    'course_name': course_name,
                    'normalized_code': normalized_code
                })
            }
            
            course_ref.set(course_data)
            
            # Create subcollections for end-semester, mid-semester, and quizzes
            course_ref.collection('end-semester').document('placeholder').set({})
            course_ref.collection('mid-semester').document('placeholder').set({})
            course_ref.collection('quizzes').document('placeholder').set({})
            
            # Mark course as processed
            processed_courses.add(course_code)
            
            print(f"Imported and updated course: {course_code} - {course_name}")
    
    print(f"Total courses imported and updated: {len(processed_courses)}")

# Usage
if __name__ == "__main__":
    import_and_update_courses('asmr.csv')