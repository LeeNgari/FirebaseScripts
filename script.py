import csv
import firebase_admin
from firebase_admin import credentials, firestore
import logging

# Initialize logging
logging.basicConfig(filename='firebase_course_upload.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Firebase Admin SDK
cred = credentials.Certificate('song-69f15-firebase-adminsdk-igbek-26774242e2.json')  # Replace with your service account key path
firebase_admin.initialize_app(cred)

# Get Firestore client
db = firestore.client()

# Load the CSV file
file_path = 'filtered_courses.csv'  # Replace with your actual file path
try:
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Clean up the column names by stripping any extra spaces
        fieldnames = [field.strip() for field in reader.fieldnames]
        reader.fieldnames = fieldnames

        for row in reader:
            try:
                course_code = row['Course Code']
                course_name = row['Course Name']
                
                # Add a document to the 'courses' collection
                course_doc_ref = db.collection('courses').document(course_code)
                course_doc_ref.set({
                    'course_name': course_name
                })
                
                # Create sub-collections: 'end-semester', 'mid-semester', and 'quizzes'
                course_doc_ref.collection('end-semester').add({})
                course_doc_ref.collection('mid-semester').add({})
                course_doc_ref.collection('quizzes').add({})
                
                logging.info(f"Created document for course {course_code} with collections.")
            except KeyError as e:
                logging.error(f"KeyError: Missing column in CSV for row {row}. Error: {e}")
            except Exception as e:
                logging.error(f"Error processing course {row}. Error: {e}")
    
    logging.info("All courses processed.")

except FileNotFoundError as e:
    logging.error(f"File not found: {file_path}. Error: {e}")
except Exception as e:
    logging.error(f"Error reading the CSV file. Error: {e}")
