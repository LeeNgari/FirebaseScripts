from firebase_admin import credentials, firestore
import firebase_admin

cred = credentials.Certificate("campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

def fetch_lecturers():
    lecturers_ref = db.collection('lecturers')
    lecturers = lecturers_ref.stream()
    lecturer_names = [lecturer.to_dict()['name'] for lecturer in lecturers]
    return lecturer_names

lecturer_names = fetch_lecturers()
for name in lecturer_names:
    print(name)