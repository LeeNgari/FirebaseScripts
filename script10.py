import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin SDK
cred = credentials.Certificate("campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json")  # Replace with your Firebase service account key
firebase_admin.initialize_app(cred)

db = firestore.client()

def fix_lecturer_ratings():
    lecturers_ref = db.collection("lecturers")
    lecturers = lecturers_ref.stream()

    for lecturer in lecturers:
        lecturer_id = lecturer.id
        reviews_ref = lecturers_ref.document(lecturer_id).collection("reviews")
        reviews = reviews_ref.stream()

        ratings = [review.get("rating") for review in reviews if review.get("rating") is not None]

        total_ratings = len(ratings)
        avg_rating = round(sum(ratings) / total_ratings, 2) if total_ratings > 0 else 0

        # Update the lecturer document with correct values
        lecturers_ref.document(lecturer_id).update({
            "totalRatings": total_ratings,
            "rating": avg_rating
        })

        print(f"Updated {lecturer.get('name')}: rating={avg_rating}, totalRatings={total_ratings}")

# Run the script
fix_lecturer_ratings()
