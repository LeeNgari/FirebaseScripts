import firebase_admin
from firebase_admin import credentials, firestore
import requests
import hashlib
from PIL import Image
from io import BytesIO
import imagehash
import concurrent.futures
import logging
import time
from collections import defaultdict
from functools import lru_cache

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duplicate_detector')

# ---- CONFIGURE FIREBASE ---- #
def initialize_firebase():
    try:
        # Replace with the path to your service account key JSON file
        SERVICE_ACCOUNT_PATH = "campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json"
        
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        logger.error(f"Failed to initialize Firebase: {e}")
        raise

# ---- SUBCOLLECTIONS ---- #
SUBCOLLECTIONS = ['end-semester', 'mid-semester', 'quizzes']
HAMMING_THRESHOLD = 5  # Configurable threshold for image similarity
MAX_RETRIES = 3        # Number of retries for network operations
BATCH_SIZE = 300       # Maximum number of operations per batch
MAX_WORKERS = 5       # Maximum number of parallel workers

# ---- HASH HELPERS ---- #
def get_sha256(content):
    return hashlib.sha256(content).hexdigest()

def get_phash(image_bytes):
    try:
        image = Image.open(BytesIO(image_bytes)).convert("RGB")
        image = image.resize((256, 256))
        return str(imagehash.phash(image))
    except Exception as e:
        logger.warning(f"Could not generate perceptual hash: {e}")
        return None

@lru_cache(maxsize=1000)
def hamming_distance(str1, str2):
    return sum(c1 != c2 for c1, c2 in zip(str1, str2))

# ---- EFFICIENT IMAGE HASH STORAGE ---- #
class ImageHashStore:
    def __init__(self):
        self.hashes = []
        
    def add(self, phash, url, paper_doc_id):
        self.hashes.append({
            "phash": phash,
            "url": url,
            "paperDocId": paper_doc_id
        })
        
    def find_similar(self, phash, threshold=HAMMING_THRESHOLD):
        for item in self.hashes:
            if hamming_distance(item["phash"], phash) <= threshold:
                return item
        return None

# ---- FILE PROCESSING ---- #
def download_file(url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.content, response.headers.get("Content-Type", "")
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to download {url} after {retries} attempts: {e}")
                raise
                
def process_file(url, course_id, sub, paper_doc_id, paper_data, file_hash_map, image_hash_store):
    try:
        content, content_type = download_file(url)
        file_hash = get_sha256(content)
        
        # Check for exact match
        exact_match = file_hash_map.get(file_hash)
        
        # Check image similarity if it's an image
        similar_match = None
        current_phash = None
        if content_type.startswith('image/'):
            current_phash = get_phash(content)
            if current_phash:
                similar_match = image_hash_store.find_similar(current_phash)
        
        result = {
            "fileUrl": url,
            "fileHash": file_hash,
            "pHash": current_phash,
            "courseId": course_id,
            "subcollection": sub,
            "paperDocId": paper_doc_id,
            "paperData": paper_data,
            "exactMatch": exact_match,
            "similarMatch": similar_match
        }
        
        # Update our maps for future comparisons
        if not exact_match:
            file_hash_map[file_hash] = {
                "url": url,
                "paperDocId": paper_doc_id
            }
            
        if current_phash and not similar_match:
            image_hash_store.add(current_phash, url, paper_doc_id)
            
        logger.info(f"✓ Processed: {url}")
        return result
        
    except Exception as e:
        logger.error(f"⚠️ Failed to process {url}: {e}")
        return {
            "fileUrl": url,
            "courseId": course_id,
            "subcollection": sub,
            "paperDocId": paper_doc_id,
            "error": str(e)
        }

# ---- BATCH WRITE OPERATIONS ---- #
def write_batched_documents(db, results):
    """Write results to Firestore in batches"""
    file_hashes_batch = db.batch()
    duplicates_batch = db.batch()
    
    file_hash_count = 0
    duplicate_count = 0
    
    for result in results:
        if "error" in result:
            continue
            
        # Add to fileHashes collection
        doc_ref = db.collection('fileHashes').document()
        file_hash_data = {
            "fileUrl": result["fileUrl"],
            "fileHash": result["fileHash"],
            "pHash": result["pHash"],
            "courseId": result["courseId"],
            "subcollection": result["subcollection"],
            "paperDocId": result["paperDocId"],
            **result["paperData"]
        }
        file_hashes_batch.set(doc_ref, file_hash_data)
        file_hash_count += 1
        
        # If we hit the batch limit, commit and reset
        if file_hash_count >= BATCH_SIZE:
            file_hashes_batch.commit()
            file_hashes_batch = db.batch()
            file_hash_count = 0
            
        # Check for and record exact duplicates
        if result["exactMatch"]:
            doc_ref = db.collection('duplicates').document()
            duplicate_data = {
                "type": "exact",
                "duplicateFileUrl": result["fileUrl"],
                "matchedFileUrl": result["exactMatch"]["url"],
                "fileHash": result["fileHash"],
                "courseId": result["courseId"],
                "subcollection": result["subcollection"],
                "paperDocId": result["paperDocId"],
                "matchedPaperDocId": result["exactMatch"]["paperDocId"],
                **result["paperData"]
            }
            duplicates_batch.set(doc_ref, duplicate_data)
            duplicate_count += 1
            
        # Check for and record similar images
        elif result["similarMatch"]:
            doc_ref = db.collection('duplicates').document()
            duplicate_data = {
                "type": "similar",
                "duplicateFileUrl": result["fileUrl"],
                "matchedFileUrl": result["similarMatch"]["url"],
                "pHash": result["pHash"],
                "similarToPHash": result["similarMatch"]["phash"],
                "courseId": result["courseId"],
                "subcollection": result["subcollection"],
                "paperDocId": result["paperDocId"],
                "matchedPaperDocId": result["similarMatch"]["paperDocId"],
                **result["paperData"]
            }
            duplicates_batch.set(doc_ref, duplicate_data)
            duplicate_count += 1
            
        # If we hit the batch limit, commit and reset
        if duplicate_count >= BATCH_SIZE:
            duplicates_batch.commit()
            duplicates_batch = db.batch()
            duplicate_count = 0
    
    # Commit any remaining operations
    if file_hash_count > 0:
        file_hashes_batch.commit()
        
    if duplicate_count > 0:
        duplicates_batch.commit()
        
    return file_hash_count, duplicate_count

# ---- MAIN FUNCTION ---- #
def mark_duplicates():
    try:
        db = initialize_firebase()
        courses_ref = db.collection('courses')
        
        # Get all courses
        logger.info("Fetching courses...")
        courses = list(courses_ref.stream())
        logger.info(f"Found {len(courses)} courses")
        
        file_hash_map = {}      # For exact duplicates
        image_hash_store = ImageHashStore()  # For perceptual duplicates
        
        # Collect all files that need processing
        processing_queue = []
        
        for course in courses:
            course_id = course.id
            logger.info(f"Processing course: {course_id}")
            
            for sub in SUBCOLLECTIONS:
                sub_ref = courses_ref.document(course_id).collection(sub)
                try:
                    papers = list(sub_ref.stream())
                    logger.info(f"  Found {len(papers)} papers in {sub}")
                    
                    for paper in papers:
                        paper_doc_id = paper.id
                        paper_data = paper.to_dict()
                        file_urls = paper_data.get('fileUrls', [])
                        
                        for url in file_urls:
                            processing_queue.append((url, course_id, sub, paper_doc_id, paper_data))
                            
                except Exception as e:
                    logger.error(f"Error fetching {sub} for course {course_id}: {e}")
        
        logger.info(f"Total files to process: {len(processing_queue)}")
        
        # Process files in parallel
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(
                    process_file, url, course_id, sub, paper_doc_id, paper_data, 
                    file_hash_map, image_hash_store
                ): url 
                for url, course_id, sub, paper_doc_id, paper_data in processing_queue
            }
            
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Exception processing {url}: {e}")
                    results.append({
                        "fileUrl": url,
                        "error": str(e)
                    })
                    
        # Batch write results to Firebase
        logger.info("Writing results to Firestore...")
        file_hash_count, duplicate_count = write_batched_documents(db, results)
        
        # Summary
        error_count = sum(1 for r in results if "error" in r)
        logger.info("✅ Duplicate marking completed.")
        logger.info(f"Files processed: {len(results)}")
        logger.info(f"File hashes stored: {file_hash_count}")
        logger.info(f"Duplicates found: {duplicate_count}")
        logger.info(f"Errors: {error_count}")
        
    except Exception as e:
        logger.error(f"Fatal error in mark_duplicates: {e}")
        raise

# ---- RUN ---- #
if __name__ == "__main__":
    mark_duplicates()