import firebase_admin
from firebase_admin import credentials, firestore
import logging
import time

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('db_cloner')

# --- Firebase Initialization Function ---
def initialize_firebase_app(name, service_account_path):
    """Initializes a Firebase app with a given name and service account."""
    try:
        # Check if an app with this name already exists
        if name in firebase_admin._apps:
            logger.info(f"Firebase app '{name}' already initialized.")
            return firebase_admin.get_app(name=name)
        
        cred = credentials.Certificate(service_account_path)
        app = firebase_admin.initialize_app(cred, name=name)
        logger.info(f"Firebase app '{name}' initialized successfully.")
        return app
    except Exception as e:
        logger.error(f"Failed to initialize Firebase app '{name}': {e}")
        raise

# --- Configuration ---
# IMPORTANT: Replace these paths with your actual service account key paths
SOURCE_SERVICE_ACCOUNT_PATH = "campusaid-afe4b-firebase-adminsdk-i2di4-4484b29e45.json"
DEST_SERVICE_ACCOUNT_PATH = "linguo-cbb63-firebase-adminsdk-9earb-832e97d49c.json"

# Collections to transfer
COLLECTIONS_TO_TRANSFER = {
    'duplicates': [], # Empty list means no subcollections
    'courses': ['end-semester', 'mid-semester', 'quizzes'] # List of subcollections for 'courses'
}

BATCH_SIZE = 400 # Max 500 for Firestore batch writes

# --- Core Transfer Logic ---
def transfer_collection(source_db, dest_db, collection_name, subcollections=None):
    """
    Transfers documents from a source collection to a destination collection,
    preserving document IDs and optionally transferring specified subcollections.
    """
    logger.info(f"Starting transfer for collection: '{collection_name}'")
    source_collection_ref = source_db.collection(collection_name)
    
    docs_stream = source_collection_ref.stream()
    
    batch = dest_db.batch()
    batch_count = 0
    doc_count = 0

    for doc in docs_stream:
        doc_count += 1
        dest_doc_ref = dest_db.collection(collection_name).document(doc.id)
        batch.set(dest_doc_ref, doc.to_dict())
        batch_count += 1

        if batch_count >= BATCH_SIZE:
            batch.commit()
            logger.info(f"  Committed batch for '{collection_name}'. Total docs transferred: {doc_count}")
            batch = dest_db.batch()
            batch_count = 0
            time.sleep(0.1) # Small pause to avoid hitting write limits too fast

        if subcollections and doc.exists:
            for sub_name in subcollections:
                logger.debug(f"    Transferring subcollection '{sub_name}' for doc '{doc.id}'")
                source_sub_ref = source_collection_ref.document(doc.id).collection(sub_name)
                
                sub_batch = dest_db.batch()
                sub_batch_count = 0
                sub_doc_count = 0

                for sub_doc in source_sub_ref.stream():
                    sub_doc_count += 1
                    dest_sub_doc_ref = dest_doc_ref.collection(sub_name).document(sub_doc.id)
                    sub_batch.set(dest_sub_doc_ref, sub_doc.to_dict())
                    sub_batch_count += 1

                    if sub_batch_count >= BATCH_SIZE:
                        sub_batch.commit()
                        logger.debug(f"      Committed sub-batch for '{sub_name}'. Total sub-docs transferred: {sub_doc_count}")
                        sub_batch = dest_db.batch()
                        sub_batch_count = 0
                        time.sleep(0.05) # Smaller pause for sub-batches

                if sub_batch_count > 0:
                    sub_batch.commit()
                    logger.debug(f"      Committed final sub-batch for '{sub_name}'. Total sub-docs transferred: {sub_doc_count}")

    if batch_count > 0:
        batch.commit()
        logger.info(f"  Committed final batch for '{collection_name}'. Total docs transferred: {doc_count}")

    logger.info(f"Finished transfer for collection: '{collection_name}'. Total documents: {doc_count}")


# --- Main Cloning Function ---
def clone_firebase_db():
    """
    Connects to source and destination Firebase projects and transfers specified collections.
    """
    source_app = None
    dest_app = None
    
    try:
        # Initialize both Firebase apps
        source_app = initialize_firebase_app("source_db", SOURCE_SERVICE_ACCOUNT_PATH)
        source_db = firestore.client(app=source_app)

        dest_app = initialize_firebase_app("dest_db", DEST_SERVICE_ACCOUNT_PATH)
        dest_db = firestore.client(app=dest_app)

        logger.info("Starting database cloning process...")

        for collection_name, subcollections in COLLECTIONS_TO_TRANSFER.items():
            transfer_collection(source_db, dest_db, collection_name, subcollections)
        
        logger.info("\n--- Database Cloning Complete ---")
        logger.info("Your test database should now contain a copy of the specified collections.")
        logger.info("Remember to update your duplicate deletion script's SERVICE_ACCOUNT_PATH")
        logger.info("to point to the test database's service account key before running it.")

    except Exception as e:
        logger.critical(f"Fatal error during database cloning: {e}")
    finally:
        # Ensure apps are properly closed if needed (though usually not strictly necessary for scripts)
        if source_app:
            firebase_admin.delete_app(source_app)
        if dest_app:
            firebase_admin.delete_app(dest_app)

# --- Run Script ---
if __name__ == "__main__":
    clone_firebase_db()