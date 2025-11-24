import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin import exceptions as firebase_exceptions
import logging
from datetime import datetime, timezone
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duplicate_deleter')

# ---- CONFIGURE FIREBASE ---- #
def initialize_firebase():
    """Initializes Firebase Admin SDK and returns a Firestore client."""
    try:
        # Replace with the path to your service account key JSON file
        SERVICE_ACCOUNT_PATH = "campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json"
        
        # Check if Firebase app is already initialized to avoid re-initialization errors
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
            firebase_admin.initialize_app(cred)
            logger.info("Firebase app initialized successfully.")
        else:
            logger.info("Firebase app already initialized.")
        return firestore.client()
    except Exception as e:
        logger.error(f"Failed to initialize Firebase: {e}")
        raise

current_time = datetime.now(timezone.utc).isoformat()  # This includes the "+00:00" UTC offset
timestamp_filename = "pitr_restore_point.txt"

with open(timestamp_filename, "w") as f:
    f.write(f"Firestore restore point before deletion: {current_time}\n")

logger.info(f"🕒 Restore point written to {timestamp_filename}: {current_time}")

# ---- MAIN DELETION FUNCTION ---- #
def remove_duplicate_urls_from_papers():
    """
    Iterates through the 'duplicates' collection and removes the 'duplicateFileUrl'
    from the 'fileUrls' array of the corresponding paper document in Firestore.
    """
    db = None
    try:
        db = initialize_firebase()
        
        duplicates_ref = db.collection('duplicates')
        
        logger.info("Fetching duplicate entries from 'duplicates' collection...")
        
        processed_count = 0
        deleted_url_count = 0
        skipped_count = 0
        
        # Stream duplicates in batches to handle large collections
        duplicates_stream = duplicates_ref.stream()
        
        for doc in duplicates_stream:
            duplicate_entry = doc.to_dict()
            duplicate_id = doc.id
            
            duplicate_file_url = duplicate_entry.get('duplicateFileUrl')
            course_id = duplicate_entry.get('courseId')
            subcollection = duplicate_entry.get('subcollection')
            paper_doc_id = duplicate_entry.get('paperDocId')
            
            if not all([duplicate_file_url, course_id, subcollection, paper_doc_id]):
                logger.warning(f"Skipping duplicate entry {duplicate_id} due to missing required fields: {duplicate_entry}")
                skipped_count += 1
                continue
            
            paper_doc_ref = db.collection('courses').document(course_id).collection(subcollection).document(paper_doc_id)
            
            try:
                # Use a transaction for safer updates
                @firestore.transactional
                def update_paper_document(transaction, doc_ref, url_to_remove):
                    snapshot = doc_ref.get(transaction=transaction)
                    if not snapshot.exists:
                        logger.warning(f"Paper document {paper_doc_id} in {course_id}/{subcollection} not found for duplicate URL {url_to_remove}. Skipping.")
                        return False # Indicate that update didn't happen for this reason
                    
                    current_file_urls = snapshot.get('fileUrls')
                    if current_file_urls is None:
                        logger.warning(f"Paper document {paper_doc_id} in {course_id}/{subcollection} has no 'fileUrls' field. Skipping removal of {url_to_remove}.")
                        return False

                    if url_to_remove in current_file_urls:
                        # Use ArrayRemove to remove the specific URL
                        transaction.update(doc_ref, {'fileUrls': firestore.ArrayRemove([url_to_remove])})
                        logger.info(f"✓ Removed '{url_to_remove}' from {course_id}/{subcollection}/{paper_doc_id}")
                        return True # Indicate successful removal
                    else:
                        logger.info(f"'{url_to_remove}' not found in 'fileUrls' for {course_id}/{subcollection}/{paper_doc_id}. Already removed or never existed.")
                        return False # Indicate URL was not found for removal

                # Attempt the transaction
                if update_paper_document(db.transaction(), paper_doc_ref, duplicate_file_url):
                    deleted_url_count += 1

                    try:
                        duplicates_ref.document(duplicate_id).delete()
                        logger.info(f"🗑️ Deleted duplicate entry {duplicate_id}")

                        # ✅ Log the deleted duplicate ID to a file
                        with open("removed_duplicates.txt", "a") as f:
                          f.write(f"{duplicate_id}\n")

                    except Exception as e:
                        logger.warning(f"Failed to delete duplicate entry {duplicate_id}: {e}")
                
                processed_count += 1
                if processed_count % 100 == 0:
                    logger.info(f"Processed {processed_count} duplicate entries so far...")

            except firebase_exceptions.NotFound:
                logger.warning(f"Paper document {paper_doc_id} in {course_id}/{subcollection} not found for duplicate URL {duplicate_file_url}. It might have been deleted.")
                skipped_count += 1
            except Exception as e:
                logger.error(f"Error processing duplicate entry {duplicate_id} (URL: {duplicate_file_url}): {e}")
                skipped_count += 1
            
            # Small delay to avoid hitting rate limits if processing many documents
            time.sleep(0.05) 
            
        logger.info("\n--- Duplicate URL Removal Summary ---")
        logger.info(f"Total duplicate entries processed: {processed_count}")
        logger.info(f"Successfully removed URLs from paper documents: {deleted_url_count}")
        logger.info(f"Entries skipped (e.g., missing data, paper doc not found, URL already removed): {skipped_count}")
        logger.info("Process complete.")

    except Exception as e:
        logger.critical(f"Fatal error during duplicate URL removal: {e}")

# ---- RUN ---- #
if __name__ == "__main__":
    remove_duplicate_urls_from_papers()