import csv
import firebase_admin
from firebase_admin import credentials, firestore

def normalize_code(code):
    """
    Normalize course code by removing spaces and converting to lowercase
    """
    return code.replace(' ', '').lower()

def import_courses_to_firebase(csv_file_path):
    """
    Import courses from CSV to Firebase Firestore
    """
    # Initialize Firebase Admin SDK (make sure to replace with your own path to service account key)
    cred = credentials.Certificate('linguo-cbb63-firebase-adminsdk-9earb-832e97d49c.json')
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
            course_ref.set({
                'course_name': course_name,
                'normalized_code': normalized_code,
                'school': school
            })
            
            # Create subcollections for end-semester, mid-semester, and quizzes
            # These will be empty but ready for future use
            course_ref.collection('end-semester').document('placeholder').set({})
            course_ref.collection('mid-semester').document('placeholder').set({})
            course_ref.collection('quizzes').document('placeholder').set({})
            
            # Mark course as processed
            processed_courses.add(course_code)
            
            print(f"Imported course: {course_code} - {course_name}")
    
    print(f"Total courses imported: {len(processed_courses)}")

# Usage
if __name__ == "__main__":
    import_courses_to_firebase('asmr.csv')