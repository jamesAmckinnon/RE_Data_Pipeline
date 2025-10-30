def transcripts_to_vector_db(gcs_bucket, gcs_input_path):
    import os
    import json
    import time
    from pinecone import Pinecone, ServerlessSpec
    from google.cloud import storage
    from langchain_openai import OpenAIEmbeddings
    from langchain_core.documents import Document
    from dotenv import load_dotenv
    from pathlib import Path
    import datetime
    from zoneinfo import ZoneInfo   
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine, Column, Integer, Text, Date, DateTime
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker   
    from typing import List


    config_dir = Path("/home/jamesamckinnon1/air_env/configs")
    running_in_gcp = bool(os.getenv("GOOGLE_CLOUD_PROJECT"))
    if not running_in_gcp and (config_dir / ".env").exists():
        load_dotenv(dotenv_path=config_dir / ".env")
    else:
        load_dotenv(dotenv_path="/opt/airflow/env.gcp")

    PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    # ---------- Database setup ----------
    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4")
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine

    Base = declarative_base()

    class CouncilTranscript(Base):
        __tablename__ = "council_transcripts"

        council_transcript_id = Column(Integer, primary_key=True)
        transcript = Column(Text)
        date = Column(Date, nullable=False)
        start_time = Column(DateTime(timezone=True), nullable=False)
        meeting_type = Column(Text)
        video_url = Column(Text)
        timestamped_transcript = Column(JSONB)
        vectorized = Column(Integer, default=0)

    engine = get_db_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # ---------- Helper function to check if vectors exist for a transcript ----------
    def check_vectors_exist_for_transcript(start_time_epoch, vectorstore):
        """Check if vectors already exist in Pinecone for a given start_time_epoch"""
        try:
            # Query Pinecone for vectors with matching meeting_start_time metadata
            results = vectorstore.similarity_search(
                query="",  # Empty query since we're filtering by metadata
                k=1,  # We only need to know if any exist
                filter={"meeting_start_time": start_time_epoch}
            )
            return len(results) > 0
        except Exception as e:
            print(f"Error checking for existing vectors: {e}")
            return False  # If we can't check, assume they don't exist to be safe

    def get_existing_vectorized_transcripts(vectorstore):
        """Get a set of start_time_epoch values that already have vectors in Pinecone"""
        try:
            # Query Pinecone to get all existing vectors and extract their meeting_start_time values
            # This is a more efficient approach than checking each transcript individually
            results = vectorstore.similarity_search(
                query="",  # Empty query to get all vectors
                k=10000,  # Large number to get all vectors (adjust based on your data size)
                filter={}  # No filter to get all vectors
            )
            
            existing_epochs = set()
            for result in results:
                if 'meeting_start_time' in result.metadata:
                    existing_epochs.add(result.metadata['meeting_start_time'])
            
            print(f"Found {len(existing_epochs)} existing vectorized transcripts")
            return existing_epochs
        
        except Exception as e:
            print(f"Error getting existing vectorized transcripts: {e}")
            return set()  # If we can't check, return empty set to be safe

    def get_non_vectorized_transcripts_from_db():
        """Get transcripts from database that haven't been vectorized yet"""
        try:
            # Query database for transcripts that haven't been vectorized
            non_vectorized = session.query(CouncilTranscript).filter(
                CouncilTranscript.vectorized == 0
            ).all()
            
            print(f"Found {len(non_vectorized)} non-vectorized transcripts in database")
            return non_vectorized
        except Exception as e:
            print(f"Error getting non-vectorized transcripts from database: {e}")
            return []

    def process_orphaned_db_transcripts():
        """Process transcripts that are in DB but not in GCS (orphaned transcripts)"""
        try:
            # Get all non-vectorized transcripts from database
            non_vectorized_db = session.query(CouncilTranscript).filter(
                CouncilTranscript.vectorized == 0
            ).all()
            
            if not non_vectorized_db:
                print("No orphaned transcripts found in database")
                return []
            
            # Get transcript IDs that were processed from GCS
            gcs_transcript_ids = {t["council_transcript_id"] for t in transcripts}
            
            # Find orphaned transcripts (in DB but not in GCS)
            orphaned_transcripts = []
            for db_transcript in non_vectorized_db:
                if db_transcript.council_transcript_id not in gcs_transcript_ids:
                    orphaned_transcripts.append(db_transcript)
            
            if orphaned_transcripts:
                print(f"Found {len(orphaned_transcripts)} orphaned transcripts in database:")
                for transcript in orphaned_transcripts:
                    print(f"  - ID: {transcript.council_transcript_id}, Date: {transcript.date}, Video: {transcript.video_url}")
                
                # Convert orphaned DB transcripts to GCS format for processing
                orphaned_gcs_format = []
                for db_transcript in orphaned_transcripts:
                    # Convert database transcript to GCS format
                    gcs_transcript = {
                        "council_transcript_id": db_transcript.council_transcript_id,
                        "meeting_type": db_transcript.meeting_type,
                        "date": db_transcript.date.isoformat() if db_transcript.date else None,
                        "start_time": db_transcript.start_time.isoformat() if db_transcript.start_time else None,
                        "video_url": db_transcript.video_url,
                        "transcript": db_transcript.transcript,
                        "timestamped_transcript": db_transcript.timestamped_transcript  # This should be available in DB
                    }
                    orphaned_gcs_format.append(gcs_transcript)
                
                print(f"Processing {len(orphaned_gcs_format)} orphaned transcripts from database")
                return orphaned_gcs_format
            
            return []
            
        except Exception as e:
            print(f"Error processing orphaned transcripts: {e}")
            return []

    def mark_transcript_as_vectorized(council_transcript_id):
        """Mark a transcript as vectorized in the database"""
        try:
            transcript = session.query(CouncilTranscript).filter(
                CouncilTranscript.council_transcript_id == council_transcript_id
            ).first()
            if transcript:
                transcript.vectorized = 1
                session.commit()
                print(f"Marked transcript {council_transcript_id} as vectorized")
        except Exception as e:
            print(f"Error marking transcript as vectorized: {e}")
            session.rollback()

    def process_transcripts_to_chunks(transcripts_list, chunk_type="normal"):
        """Process a list of transcripts into document chunks"""
        document_chunks = []

        for transcript in transcripts_list:
            start_time_utc = transcript["start_time"]
            dt_utc = datetime.datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
            start_time_epoch = int(dt_utc.timestamp()) 

            document_metadata = {
                "council_transcript_id": transcript["council_transcript_id"],
                "meeting_type": transcript["meeting_type"],
                "meeting_date": transcript["date"],
                "meeting_start_time": start_time_epoch,
            }

            timestamped_snippets_dict = transcript["timestamped_transcript"]
            video_url = transcript["video_url"]

            # Ensure sorted by timestamp (as int)
            sorted_items = sorted(timestamped_snippets_dict.items(), key=lambda x: int(x[0]))

            for i in range(0, len(sorted_items) - N + 1, STRIDE):
                chunk_items = sorted_items[i:i+N]

                chunk_text = " ".join(item[1] for item in chunk_items)
                first_timestamp = int(chunk_items[0][0])
                timestamped_link = f"{video_url}&t={first_timestamp}s"

                document_chunks.append(Document(
                    page_content=chunk_text,
                    metadata={
                        **document_metadata,
                        "chunk_timestamp": first_timestamp,
                        "timestamped_youtube_link": timestamped_link
                    }
                ))

            if len(sorted_items) % STRIDE != 0 and (len(sorted_items) - N) % STRIDE != 0:
                final_items = sorted_items[-N:]
                chunk_text = " ".join(item[1] for item in final_items)
                first_timestamp = int(final_items[0][0])
                timestamped_link = f"{video_url}?t={first_timestamp}"

                document_chunks.append(Document(
                    page_content=chunk_text,
                    metadata={
                        **document_metadata,
                        "chunk_timestamp": first_timestamp,
                        "timestamped_youtube_link": timestamped_link
                    }
                ))
        
        print(f"Generated {len(document_chunks)} document chunks from {len(transcripts_list)} {chunk_type} transcripts")
        return document_chunks

    def add_documents_to_vectorstore(document_chunks, vectorstore, chunk_type="normal"):
        """Add document chunks to vector store and return success status"""
        if not document_chunks:
            print(f"No {chunk_type} document chunks to add to vector database")
            return False
            
        batch_size = 100
        for i in range(0, len(document_chunks), batch_size):
            batch = document_chunks[i:i+batch_size]
            vectorstore.add_documents(documents=batch)
        print(f"Added {len(document_chunks)} {chunk_type} document chunks to vector database")
        return True

    pc = Pinecone(api_key=PINECONE_API_KEY, environment="us-west1-gcp")
    index_name = "transcripts-index"

    if index_name not in pc.list_indexes().names():
        pc.create_index(
            name=index_name,
            dimension=1536, 
            metric='cosine',
            spec=ServerlessSpec(cloud='aws', region='us-east-1')
        )

    index = pc.Index(index_name)

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blobs = list(bucket.list_blobs(prefix=gcs_input_path))
    json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]
    print(f"Found {len(json_blobs)} JSON files in {gcs_bucket}/{gcs_input_path}")

    transcripts = []
    for blob in json_blobs:
        content = blob.download_as_string()
        gcs_transcripts = json.loads(content)
        transcripts.extend(gcs_transcripts)
    print(f"Combined {len(transcripts)} transcripts from all files")


    N = 35        
    OVERLAP = 6 
    STRIDE = N - OVERLAP

    # Embed and store in batches to avoid OpenAI token limit
    embeddings = OpenAIEmbeddings(
        model="text-embedding-3-small",
        openai_api_key=OPENAI_API_KEY
    )

    class SimplePineconeVectorStore:
        def __init__(self, index, embedding):
            self.index = index
            self.embedding = embedding

        def add_documents(self, documents: List[Document]):
            vectors = []
            for doc in documents:
                values = self.embedding.embed_query(doc.page_content)
                metadata = dict(doc.metadata) if doc.metadata else {}
                metadata["text"] = doc.page_content
                # Stable-ish ID using transcript id and chunk timestamp if available
                chunk_id = f"{metadata.get('council_transcript_id', 'unknown')}:{metadata.get('chunk_timestamp', int(time.time()*1e6))}"
                vectors.append({
                    "id": chunk_id,
                    "values": values,
                    "metadata": metadata,
                })
            # Upsert in batches
            batch = 100
            for i in range(0, len(vectors), batch):
                self.index.upsert(vectors=vectors[i:i+batch])

        def similarity_search(self, query: str, k: int = 4, filter: dict | None = None) -> List[Document]:
            # Use an embedding of the query; allow empty query for filter-only searches
            qvec = self.embedding.embed_query(query or "")
            res = self.index.query(vector=qvec, top_k=k, filter=filter or {}, include_metadata=True)
            matches = res.get("matches", []) if isinstance(res, dict) else res.matches
            docs: List[Document] = []
            for m in matches:
                md = m.get("metadata", {}) if isinstance(m, dict) else (m.metadata or {})
                text = md.get("text", "")
                docs.append(Document(page_content=text, metadata=md))
            return docs

    vectorstore = SimplePineconeVectorStore(index=index, embedding=embeddings)
    
    try:
        # Get non-vectorized transcripts from database
        non_vectorized_db_transcripts = get_non_vectorized_transcripts_from_db()
        
        # Create a set of non-vectorized transcript IDs for quick lookup
        non_vectorized_ids = {t.council_transcript_id for t in non_vectorized_db_transcripts}
        
        # Filter GCS transcripts to only process those not yet vectorized
        transcripts_to_process = []
        for transcript in transcripts:
            if transcript["council_transcript_id"] in non_vectorized_ids:
                transcripts_to_process.append(transcript)
                print(f"Processing transcript ID {transcript['council_transcript_id']} (not vectorized in DB)")
            else:
                print(f"Skipping transcript ID {transcript['council_transcript_id']} (already vectorized in DB)")
        
        print(f"Processing {len(transcripts_to_process)} out of {len(transcripts)} transcripts (using DB method)")
        
    except Exception as e:
        print(f"Database method failed, falling back to Pinecone method: {e}")
        
        # Method 2: Fallback to Pinecone-based checking
        existing_vectorized_epochs = get_existing_vectorized_transcripts(vectorstore)
        
        transcripts_to_process = []
        for transcript in transcripts:
            start_time_utc = transcript["start_time"]
            dt_utc = datetime.datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
            dt_mt = dt_utc.astimezone(ZoneInfo("America/Denver"))
            start_time_epoch = int(time.mktime(dt_mt.timetuple()))
            
            if start_time_epoch not in existing_vectorized_epochs:
                transcripts_to_process.append(transcript)
                print(f"Processing transcript with start_time_epoch: {start_time_epoch}")
            else:
                print(f"Skipping transcript with start_time_epoch: {start_time_epoch} (vectors already exist)")
        
        print(f"Processing {len(transcripts_to_process)} out of {len(transcripts)} transcripts (using Pinecone method)")
    
    # Process transcripts to document chunks
    document_chunks = process_transcripts_to_chunks(transcripts_to_process, "normal")

    # Add documents to vector store
    if add_documents_to_vectorstore(document_chunks, vectorstore, "normal"):
        # Mark processed transcripts as vectorized in the database
        for transcript in transcripts_to_process:
            mark_transcript_as_vectorized(transcript["council_transcript_id"])
    
    # Process any orphaned transcripts from database (transcripts in DB but not in GCS)
    print("\n=== Processing orphaned transcripts from database ===")
    orphaned_transcripts = process_orphaned_db_transcripts()
    
    if orphaned_transcripts:
        print(f"Processing {len(orphaned_transcripts)} orphaned transcripts...")
        
        # Process orphaned transcripts using the same chunking logic
        orphaned_document_chunks = process_transcripts_to_chunks(orphaned_transcripts, "orphaned")

        # Add orphaned document chunks to vector database
        if add_documents_to_vectorstore(orphaned_document_chunks, vectorstore, "orphaned"):
            # Mark orphaned transcripts as vectorized
            for transcript in orphaned_transcripts:
                mark_transcript_as_vectorized(transcript["council_transcript_id"])
    
    session.close()