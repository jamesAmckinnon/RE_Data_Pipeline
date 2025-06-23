def get_edm_council_transcripts():   
    from tqdm import tqdm
    import time
    import re
    import traceback
    from datetime import datetime
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine, Column, Integer, Float, String
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.dialects.postgresql import insert
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager

    # Get database connection from Airflow connections
    def get_db_engine():
        conn = BaseHook.get_connection("supabase_db_TP_IPv4") 
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        )
        return engine

    # Create SQLAlchemy model for rental_rates
    Base = declarative_base()

    class CouncilTranscript(Base):
        __tablename__ = 'council_transcripts'
        
        council_transcript_id = Column(Integer, primary_key=True)
        date = Column(String)
        year = Column(String)
        month = Column(String)
        day = Column(String)
        meeting_type = Column(String)
        transcript = Column(String)
        video_url = Column(String)
        title = Column(String)

        
        def __repr__(self):
            return f"<CouncilTranscript(meeting_type=${self.meeting_type}, date={self.date})>"


    options = Options()
    options.binary_location = "/usr/bin/google-chrome"  # point to installed binary
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

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get("https://www.youtube.com/@councilchambers-cityofedmo9617/streams")
    print("Finding videos in streams page...")
    time.sleep(5)

    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    scroll_count = 1

    while True:
        print(f"({scroll_count}) Scrolling to load next portion of page...")
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(3)
        new_height = driver.execute_script("return document.documentElement.scrollHeight")
        time.sleep(2)
        if new_height == last_height: 
            break
        last_height = new_height
        scroll_count += 1

    # Collect all video links
    video_elements = driver.find_elements(By.XPATH, '//ytd-rich-grid-media//a[contains(@href, "/watch?v=")]')

    video_urls = []
    for elem in video_elements:
        href = elem.get_attribute('href')
        if href and href not in video_urls:
            video_urls.append(href)

    print(f"Found {len(video_urls)} video URLs.")
    driver.quit()

    transcripts = []
    failed_transcripts = []

    engine = get_db_engine()
    # Create tables if they don't exist
    Base.metadata.create_all(engine)

    # Create a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    for i, video_url in enumerate(video_urls): 
        print(f"\n\n============ ({i}/{len(video_urls)}) Scraping url: ", video_url, "\n")

        if i % 20 == 0 and i > 0:
            print("\n\nRestarting driver to prevent potential issues with memory leak...\n")
            driver.quit()
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )
            print("\n\n")
        
        try:
            driver.get(video_url)
            wait = WebDriverWait(driver, 10)

            info_xpath = '//ytd-watch-info-text//yt-formatted-string[@id="info"]'
            info_elem = wait.until(EC.presence_of_element_located((By.XPATH, info_xpath)))
            info_text = info_elem.text.strip().lower()

            if "scheduled for" in info_text:
                print(f"Video at {video_url} is a scheduled stream, skipping...")
                continue

            time.sleep(2)

            expand_button = wait.until(EC.element_to_be_clickable((By.ID, "expand")))
            expand_button.click()

            time.sleep(1)
            transcript_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//ytd-video-description-transcript-section-renderer//button[.//span[text()="Show transcript"]]')))
            transcript_button.click()   

            time.sleep(1)
            dropdown_xpath = '//tp-yt-paper-button[contains(@class, "dropdown-trigger") and .//div[contains(text(), "English")]]'
            dropdown_trigger = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath)))
            dropdown_trigger.click()

            time.sleep(1)
            auto_gen_xpath = '//tp-yt-paper-item//div[contains(text(), "English (auto-generated")]'
            auto_gen_option = wait.until(EC.element_to_be_clickable((By.XPATH, auto_gen_xpath)))
            auto_gen_option.click()

            time.sleep(1)
            segments_xpath = '//ytd-transcript-segment-renderer'
            wait.until(EC.presence_of_element_located((By.XPATH, segments_xpath)))

            # Get all transcript text segment elements
            time.sleep(1)
            segments = driver.find_elements(By.XPATH, segments_xpath)

            transcript = ""

            for segment in segments:
                # timestamp = segment.find_element(By.CLASS_NAME, 'segment-timestamp').text.strip()
                text = segment.find_element(By.CLASS_NAME, 'segment-text').text.strip()
                transcript += f" {text}"

            print(f"Transcript: {transcript[0:90]} \n\n")

            # Extract video title
            title_xpath = '//div[@id="title"]//h1//yt-formatted-string'
            title_elem = wait.until(EC.presence_of_element_located((By.XPATH, title_xpath)))
            title_text = title_elem.get_attribute("title").strip()

            match = re.match(r"([A-Za-z]+ \d{2}, \d{4}) - (.*?)(?: - Part.*)?$", title_text)
            if match:
                date_str = match.group(1) 
                meeting_type = match.group(2).strip()  

                # Convert date string to datetime object
                date_obj = datetime.strptime(date_str, "%B %d, %Y")

                new_transcript = CouncilTranscript(
                    date=date_obj.strftime("%Y-%m-%d"),
                    day=str(date_obj.day),
                    month=date_obj.strftime("%B"),
                    year=str(date_obj.year),
                    meeting_type=meeting_type,
                    title=title_text,
                    video_url=video_url,
                    transcript=transcript.strip()
                )

                try:
                    session.add(new_transcript)
                    session.commit()
                    print(f"Saved transcript for {video_url}")
                except Exception as db_err:
                    session.rollback()
                    print(f"DB error for {video_url}: {db_err}")
                    failed_transcripts.append(video_url)

            else:
                print(f"Could not parse title: {title_text}")
        
        except Exception as e:
            print(f"Error scraping {video_url}: ")
            traceback.print_exc()
            failed_transcripts.append(video_url)

    driver.quit()
    session.close()
    log_file_path = "/opt/airflow/logs/failed_transcripts.txt"

    with open(log_file_path, "w") as f:
        for url in failed_transcripts:
            f.write(url + "\n")
    
    return