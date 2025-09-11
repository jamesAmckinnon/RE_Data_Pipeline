# Transcript Vectorization Optimization

## Problem
The original `transcripts_to_vector_db` task was inefficient because:
1. It processed all transcripts from GCS every time, regardless of whether they were already vectorized
2. It had no way to track which transcripts had already been processed
3. This led to duplicate vectorization and wasted computational resources

## Solution
Implemented a comprehensive optimization with multiple fallback methods:

### Method 1: Database Tracking (Primary)
- Added a `vectorized` column to the `council_transcripts` table
- Tracks which transcripts have been successfully vectorized (0 = not vectorized, 1 = vectorized)
- Queries the database to get only non-vectorized transcripts
- Marks transcripts as vectorized after successful processing

### Method 2: Pinecone Metadata Checking (Fallback)
- If database method fails, falls back to checking Pinecone metadata
- Queries existing vectors to get `meeting_start_time` values
- Compares against current transcripts to avoid duplicates

### Method 3: Individual Vector Checking (Legacy)
- Kept the original individual checking method as a final fallback
- Checks each transcript individually against Pinecone

## Key Features

### 1. Efficient Filtering
```python
# Get non-vectorized transcripts from database
non_vectorized_db_transcripts = get_non_vectorized_transcripts_from_db()
non_vectorized_ids = {t.council_transcript_id for t in non_vectorized_db_transcripts}

# Only process transcripts that haven't been vectorized
transcripts_to_process = [t for t in transcripts if t["council_transcript_id"] in non_vectorized_ids]
```

### 2. Orphaned Transcript Recovery
```python
# Process transcripts that are in DB but not in GCS (orphaned transcripts)
orphaned_transcripts = process_orphaned_db_transcripts()
# These are transcripts that were added to DB but DAG failed before vectorization
```

### 3. DRY Code Design
```python
# Reusable functions eliminate code duplication
document_chunks = process_transcripts_to_chunks(transcripts, "normal")
add_documents_to_vectorstore(document_chunks, vectorstore, "normal")
```

### 4. Database Schema Update
```python
class CouncilTranscript(Base):
    # ... existing fields ...
    vectorized = Column(Integer, default=0)  # 0 = not vectorized, 1 = vectorized
```

### 5. Robust Error Handling
- Graceful fallback between methods
- Comprehensive error logging
- Safe defaults (assume not vectorized if can't determine)

### 6. Progress Tracking
- Clear logging of which transcripts are being processed vs skipped
- Count of total vs processed transcripts
- Success confirmation for each vectorized transcript

## Benefits

1. **Eliminates Duplicate Processing**: Only processes transcripts that haven't been vectorized
2. **Improves Performance**: Reduces processing time by skipping already-processed transcripts
3. **Reduces Costs**: Fewer API calls to OpenAI for embeddings
4. **Reliable Tracking**: Database-based tracking is more reliable than metadata-only approaches
5. **Fault Tolerant**: Multiple fallback methods ensure the system continues working even if one method fails
6. **Recovers Orphaned Transcripts**: Automatically processes transcripts that were added to DB but failed vectorization due to DAG errors

## Usage

The optimization is automatically applied when running the DAG. The system will:
1. Try to use database tracking first (most efficient)
2. Fall back to Pinecone metadata checking if database fails
3. Use individual vector checking as a last resort

## Database Migration

To use the database tracking method, you'll need to add the `vectorized` column to your existing `council_transcripts` table:

```sql
ALTER TABLE council_transcripts ADD COLUMN vectorized INTEGER DEFAULT 0;
```

The system will automatically create this column if it doesn't exist when the DAG runs.