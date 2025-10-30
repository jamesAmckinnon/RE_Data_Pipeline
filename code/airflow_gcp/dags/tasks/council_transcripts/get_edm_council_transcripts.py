def get_edm_council_transcripts(gcs_bucket, gcs_output_path):   
    from tqdm import tqdm
    import time
    import re
    import traceback
    import json
    import requests
    from urllib.parse import urlparse, parse_qs
    from datetime import datetime
    from google.cloud import storage
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine, Column, Integer, Float, String, Date, DateTime, text
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.dialects.postgresql import insert, JSONB
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
    from webdriver_manager.chrome import ChromeDriverManager
    import logging


    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4") 
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}',
            pool_pre_ping=True,
            pool_recycle=300
        )
        return engine

    # Check which video URLs already exist in database
    def get_existing_video_urls(engine):
        """Get all existing video URLs from the database to avoid reprocessing"""
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT video_url FROM council_transcripts WHERE video_url IS NOT NULL"))
                existing_urls = {row[0] for row in result}
                logger.info(f"Found {len(existing_urls)} existing video URLs in database")
                return existing_urls
        except Exception as e:
            logger.error(f"Error fetching existing video URLs: {e}")
            return set()

    # Create SQLAlchemy model for rental_rates
    Base = declarative_base()

    print("Print statement (test)")

    class CouncilTranscript(Base):
        __tablename__ = 'council_transcripts'
        
        council_transcript_id = Column(Integer, primary_key=True)
        date = Column(Date)
        year = Column(Integer)
        month = Column(Integer)
        day = Column(Integer)
        start_time = Column(DateTime(timezone=True))
        end_time = Column(DateTime(timezone=True))
        meeting_type = Column(String)
        transcript = Column(String)
        timestamped_transcript = Column(JSONB)
        video_url = Column(String)
        title = Column(String)

        def __repr__(self):
            return f"<CouncilTranscript(meeting_type=${self.meeting_type}, date={self.date})>"

    def create_webdriver():
        """Create and configure webdriver with improved error handling"""
        options = Options()
        options.binary_location = "/usr/bin/google-chrome" 
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--memory-pressure-off")
        options.add_argument("--max_old_space_size=4096")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return driver
        except Exception as e:
            logger.error(f"Failed to create webdriver: {e}")
            raise

    logger.info("Initializing web driver...")
    driver = create_webdriver()

    def clean_youtube_url(url):
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        video_id = query_params.get("v", [None])[0]
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"
        return None

    def collect_video_urls(driver):
        """Collect video URLs from YouTube streams page with improved error handling"""
        try:
            logger.info("Navigating to streams page...")
            driver.get("https://www.youtube.com/@councilchambers-cityofedmo9617/streams")
            time.sleep(5)

            last_height = driver.execute_script("return document.documentElement.scrollHeight")
            scroll_count = 1
            max_scroll_count = 1

            ##################### FOR TESTING PURPOSES ONLY #####################
            testing = False
            #####################################################################

            while True:
                logger.info(f"({scroll_count}) Scrolling to load next portion of page...")
                driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(4)
                new_height = driver.execute_script("return document.documentElement.scrollHeight")
                if new_height == last_height or scroll_count >= max_scroll_count or testing:  
                    break
                last_height = new_height
                scroll_count += 1

            # Collect all video links
            video_elements = driver.find_elements(By.XPATH, '//ytd-rich-grid-media//a[contains(@href, "/watch?v=")]')

            video_urls = []
            for elem in video_elements:
                href = elem.get_attribute('href')
                cleaned_href = clean_youtube_url(href)
                if cleaned_href not in video_urls:
                    video_urls.append(cleaned_href)

            logger.info(f"Found {len(video_urls)} video URLs.")
            return video_urls
                
        except Exception as e:
            logger.error(f"Error collecting video URLs: {e}")
            return []

    # Collect video URLs
    video_urls = collect_video_urls(driver)

    driver.quit()

    if not video_urls:
        logger.error("No video URLs found, exiting...")
        return None

    # Initialize database connection
    engine = get_db_engine()
    Base.metadata.create_all(engine)

    # Get existing video URLs to avoid reprocessing
    existing_urls = get_existing_video_urls(engine)
    
    # Filter out existing URLs
    new_video_urls = [url for url in video_urls if url not in existing_urls]
    logger.info(f"Found {len(new_video_urls)} new video URLs to process (filtered out {len(video_urls) - len(new_video_urls)} existing)")

    if not new_video_urls:
        logger.info("No new video URLs to process, exiting...")
        return f"gs://{gcs_bucket}/{gcs_output_path}"

    # Create new driver for video processing
    driver = create_webdriver()


    def get_video_details(video_url, max_retries=3):
        """Get video details with retry logic and improved error handling"""
        for attempt in range(max_retries):
            try:
                response = requests.get(video_url, timeout=30)
                response.raise_for_status()

                pattern = re.compile(r'ytInitialPlayerResponse\s*=\s*({.*?});', re.DOTALL)
                match = pattern.search(response.text)
                if not match:
                    raise ValueError("ytInitialPlayerResponse not found in HTML")

                data = json.loads(match.group(1))

                micro = data.get("microformat", {}).get("playerMicroformatRenderer", {})
                live = micro.get("liveBroadcastDetails", {})

                title_text = micro.get("title", {}).get("simpleText")
                if not title_text:
                    raise ValueError("Title not found in video data")

                start = live.get("startTimestamp")
                if not start:
                    raise ValueError("Start timestamp not found in video data")

                scheduled = False
                start_dt = datetime.fromisoformat(start)

                if "endTimestamp" in live:
                    end = live.get("endTimestamp")
                    end_dt = datetime.fromisoformat(end)
                else:
                    end_dt = None
                    scheduled = True

                # Clean up the meeting type
                title_clean = re.sub(r'^[A-Za-z]+ \d{1,2}, \d{4}\s*-?\s*', '', title_text)
                title_clean = re.sub(r'\s+\d{1,2}/\d{1,2}/\d{4}$', '', title_clean)
                meeting_type = title_clean.strip()

                return title_text, meeting_type, start_dt, end_dt, scheduled
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {video_url}: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff


    def transcript_exists(driver, max_retries=3):
        """Check if transcript exists with retry logic"""
        for attempt in range(max_retries):
            try:
                wait = WebDriverWait(driver, 10)
                expand_button = wait.until(EC.element_to_be_clickable((By.ID, "expand")))
                expand_button.click()

                wait.until(EC.presence_of_element_located((By.XPATH, '//ytd-video-description-transcript-section-renderer')))
                transcript_button = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, '//ytd-video-description-transcript-section-renderer//button[.//span[text()="Show transcript"]]')))
                transcript_button.click()   
                return True
            except TimeoutException:
                if attempt == max_retries - 1:
                    return False
                logger.warning(f"Transcript check attempt {attempt + 1} failed, retrying...")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Unexpected error in transcript_exists: {e}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(2)
        return False


    def timestamp_to_seconds(ts):
        parts = ts.split(':')
        parts = [int(p) for p in parts]

        if len(parts) == 3:
            h, m, s = parts
        elif len(parts) == 2:
            h = 0
            m, s = parts
        elif len(parts) == 1:
            h = 0
            m = 0
            s = parts[0]
        else:
            raise ValueError(f"Unexpected timestamp format: {ts}")

        return h * 3600 + m * 60 + s


    def get_transcript(driver, max_retries=3):
        """Get transcript with improved error handling and retry logic"""
        wait = WebDriverWait(driver, 15)
        transcript_segments = []
        timestamped_transcript = {}
    
        segments_xpath = '//ytd-transcript-segment-renderer'
        
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, segments_xpath)))
        except TimeoutException:
            logger.error("Transcript segments not found")
            return "", {}

        for retry_count in range(max_retries):
            try:
                segments = driver.find_elements(By.XPATH, segments_xpath)
                transcript_segments = []
                timestamped_transcript = {}
                
                for i, segment in enumerate(segments):
                    try:
                        text_elem = segment.find_element(By.CLASS_NAME, 'segment-text')
                        timestamp_elem = segment.find_element(By.CLASS_NAME, 'segment-timestamp')
                        
                        text = text_elem.text.strip().lower()
                        timestamp_str = timestamp_elem.text.strip()
                        timestamp_secs = timestamp_to_seconds(timestamp_str)
                        
                        transcript_segments.append(text)
                        timestamped_transcript[timestamp_secs] = text   
                        
                    except StaleElementReferenceException:
                        logger.warning(f"Stale element at segment {i}, retrying...")
                        break  # Break inner loop to retry getting all segments
                    except Exception as e:
                        logger.warning(f"Error processing segment {i}: {e}")
                        continue
                
                # If we processed all segments successfully, break out of retry loop
                if i == len(segments) - 1:
                    break
                    
            except Exception as e:
                logger.warning(f"Error getting transcript segments (attempt {retry_count + 1}): {e}")
                if retry_count < max_retries - 1:
                    time.sleep(2)
                
        if retry_count >= max_retries - 1:
            logger.warning("Max retries reached, using partial transcript")
        
        transcript = " ".join(transcript_segments)
        return transcript, timestamped_transcript



    def process_video_batch(video_urls, driver, engine, batch_size=5):
        """Process videos in batches with improved error handling and database operations"""
        transcripts_temp = []
        all_transcripts_for_GCS = []
        Session = sessionmaker(bind=engine)
        
        for i, video_url in enumerate(video_urls): 
            video_url = clean_youtube_url(video_url)
            if not video_url:
                logger.warning(f"Invalid URL format, skipping: {video_url}")
                continue
                
            logger.info(f"============ ({i+1}/{len(video_urls)}) Processing: {video_url} ============") 
            
            try:
                # Get video details
                title_text, meeting_type, start_dt, end_dt, scheduled = get_video_details(video_url)
                
                # Navigate to video
                driver.get(video_url)
                time.sleep(3)  # Allow page to load

                # Check if transcript exists
                if scheduled or not transcript_exists(driver):
                    logger.info(f"No transcript available for {title_text}")
                    continue

                # Get transcript
                transcript, timestamped_transcript = get_transcript(driver)
                
                if not transcript.strip():
                    logger.warning(f"Empty transcript for {title_text}")
                    continue

                new_transcript = {
                    "date": start_dt.date(), 
                    "day": start_dt.day,
                    "month": start_dt.month,
                    "year": start_dt.year,
                    "meeting_type": meeting_type,
                    "title": title_text,
                    "video_url": video_url,
                    "transcript": transcript.strip(),
                    "start_time": start_dt,
                    "end_time": end_dt,
                    "timestamped_transcript": timestamped_transcript
                }

                transcripts_temp.append(new_transcript)
                
                # Batch database operations
                if len(transcripts_temp) >= batch_size or i == len(video_urls) - 1:
                    try:
                        session = Session()
                        db_transcripts = []

                        for transcript_data in transcripts_temp:
                            db_transcript = CouncilTranscript(**transcript_data)
                            session.add(db_transcript)
                            db_transcripts.append(db_transcript)

                        session.commit()
                        
                        # Add IDs to the transcript dicts
                        for transcript_data, db_transcript in zip(transcripts_temp, db_transcripts):
                            transcript_data['council_transcript_id'] = db_transcript.council_transcript_id

                        logger.info(f"Successfully saved {len(transcripts_temp)} transcripts to database")
                        all_transcripts_for_GCS.extend(transcripts_temp)

                        transcripts_temp = []  # Reset for next batch
                    except Exception as db_err:
                        session.rollback()
                        logger.error(f"Database error: {db_err}")
                        # Continue processing other videos
                    finally:                        
                        session.close()
                
                logger.info(f"Successfully processed: {title_text}")
                logger.info(f"Transcript preview: {transcript[:60]}...")
                
            except Exception as e:
                logger.error(f"Error processing {video_url}: {e}") 
                traceback.print_exc()
                continue

        return all_transcripts_for_GCS

    # Process videos
    all_transcripts_for_GCS = process_video_batch(new_video_urls, driver, engine)
    
    # Clean up
    try:
        driver.quit()
    except Exception as e:
        logger.warning(f"Error closing driver: {e}")

    def upload_to_gcs(transcripts, bucket_name, output_path):
        """Upload transcripts to Google Cloud Storage with error handling"""
        try:
            logger.info(f"Uploading {len(transcripts)} transcripts to GCS: {bucket_name}/{output_path}")
            
            # Convert date/datetime fields to ISO strings for JSON serialization
            def serialize_transcript(t):
                t = t.copy()
                if hasattr(t.get("date"), "isoformat"):
                    t["date"] = t["date"].isoformat()
                if hasattr(t.get("start_time"), "isoformat"):
                    t["start_time"] = t["start_time"].isoformat()
                if hasattr(t.get("end_time"), "isoformat"):
                    t["end_time"] = t["end_time"].isoformat()
                return t
            
            transcripts_serialized = [serialize_transcript(t) for t in transcripts]
            
            # Save data to GCS
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(output_path)
            blob.upload_from_string(
                json.dumps(transcripts_serialized, indent=4), 
                content_type="application/json"
            )
            
            logger.info(f"Successfully uploaded to gs://{bucket_name}/{output_path}")
            return f"gs://{bucket_name}/{output_path}"
            
        except Exception as e:
            logger.error(f"Error uploading to GCS: {e}")
            raise

    # Upload to GCS if there are transcripts
    if all_transcripts_for_GCS:
        result_path = upload_to_gcs(all_transcripts_for_GCS, gcs_bucket, gcs_output_path)
    else:
        logger.info("No new transcripts to upload")
        result_path = f"gs://{gcs_bucket}/{gcs_output_path}"
    
    return result_path