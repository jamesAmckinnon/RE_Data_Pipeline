def transcript_vectors_to_summaries():
    import os
    import re
    import random
    import json
    import time
    from datetime import datetime, timedelta, timezone
    from dotenv import load_dotenv
    from pathlib import Path
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine, Column, Integer, Text, Date, DateTime
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.dialects.postgresql import JSONB
    from pinecone import Pinecone
    from langchain_openai import ChatOpenAI, OpenAIEmbeddings
    from typing import List
    
    # Simple Document class to replace langchain's Document
    class Document:
        def __init__(self, page_content: str, metadata: dict = None):
            self.page_content = page_content
            self.metadata = metadata or {}
    
    config_dir = Path("/home/jamesamckinnon1/air_env/configs")
    load_dotenv(dotenv_path= config_dir / ".env")
    PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    
    # ---------- Database ----------
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
        summarized = Column(Integer, default=0)
    
    class CouncilTranscriptSummary(Base):
        __tablename__ = "summarized_transcripts"
        id = Column(Integer, primary_key=True, autoincrement=True)
        council_transcript_id = Column(Integer, nullable=False)
        summary = Column(Text, nullable=False)
        date = Column(Date)
        start_time = Column(DateTime(timezone=True))
        meeting_type = Column(Text)
        topics = Column(JSONB) 
        tags = Column(JSONB) 
        video_url = Column(Text)
        created_at = Column(DateTime(timezone=True), server_default="now()")
    
    engine = get_db_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # ---------- Get transcripts ----------
    three_months_ago = (datetime.now(timezone.utc) - timedelta(days=90))
    transcripts = (
        session.query(CouncilTranscript)
        .filter(
            CouncilTranscript.date >= three_months_ago,
            CouncilTranscript.summarized == 0
        )
        .all()
    )
    print("Total num transcripts: ", len(transcripts))
    
    # ---------- Pinecone setup ----------
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index_name = "transcripts-index"
    embeddings = OpenAIEmbeddings(
        model="text-embedding-3-small",
        openai_api_key=OPENAI_API_KEY
    )
    index = pc.Index(index_name)
    
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
                chunk_id = f"{metadata.get('council_transcript_id', 'unknown')}:{metadata.get('chunk_timestamp', int(time.time()*1e6))}"
                vectors.append({
                    "id": chunk_id,
                    "values": values,
                    "metadata": metadata,
                })
            batch = 100
            for i in range(0, len(vectors), batch):
                self.index.upsert(vectors=vectors[i:i+batch])
        
        def similarity_search(self, query: str, k: int = 4, filter: dict | None = None) -> List[Document]:
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
    
    # ---------- LLM setup ----------
    llm = ChatOpenAI(
        openai_api_key=OPENAI_API_KEY,
        model="gpt-4o-mini",
        temperature=0.0
    )
    
    # Real estate query
    real_estate_query = (
        "zoning changes, rezoning approvals or denials, bylaw changes, land use planning, "
        "variances, building permits, construction permits, property tax rules, property tax rates, "
        "tax incentives, development fees, infrastructure projects, transportation projects, "
        "new housing developments, mixed-use developments, commercial property, retail property, "
        "industrial property, multifamily housing, affordable housing, rental housing, "
        "redevelopment projects, urban renewal, neighborhood revitalization, downtown development, "
        "commercial real estate, real estate development, development approvals, "
        "environmental regulations affecting development, sustainability requirements for real "
        "estate development, land acquisition"
    )
    
    # Helper functions
    def format_timestamp(seconds: int) -> str:
        td = timedelta(seconds=seconds)
        h, remainder = divmod(td.seconds, 3600)
        m, s = divmod(remainder, 60)
        h += td.days * 24
        return f"{h}:{m:02}:{s:02}"
    
    def replace_sources_with_timestamps(text: str) -> str:
        pattern = re.compile(r"\[\[(\d+)]]\((https?://[^\s)]+?[&?]t=(\d+)s)\)")
        
        def replacer(match):
            seconds = int(match.group(3))
            timestamp = format_timestamp(seconds)
            return f"[[{timestamp}]]({match.group(2)})"
        
        return pattern.sub(replacer, text)
    
    def mark_transcript_as_summarized(council_transcript_id):
        try:
            transcript = session.query(CouncilTranscript).filter(
                CouncilTranscript.council_transcript_id == council_transcript_id
            ).first()
            if transcript:
                transcript.summarized = 1
                session.commit()
                print(f"Marked transcript {council_transcript_id} as summarized")
        except Exception as e:
            print(f"Error marking transcript as summarized: {e}")
            session.rollback()
    
    def generate_summary(docs_with_sources: List[Document]) -> str:
        """Generate summary using direct LLM invocation"""
        combined_text = "\n\n".join([doc.page_content for doc in docs_with_sources])
        
        prompt = (
            "Combine the following city council transcript snippets into a single coherent markdown report. Extract "
            "important information that could impact the commercial real estate industry, including zoning "
            "and bylaw changes, carried bylaw motions, property tax discussions, general discussion about real estate, or topics "
            "relevant to real estate investors, brokers or developers, etc. Make the report as concise as "
            "possible while using as many facts from the text as you can. Focus on changes that are happening "
            "and any arguments for or against these changes. For zoning changes, focus more on summarizing reasoning "
            "for why the change is supported or not supported and only provide basic information about the zones themselves. "
            "Only use information discussed in the text and do not add any of your own opinions or interpretations of the information. "
            "IMPORTANT: Include inline hyperlinked source citations (Eg. [[1]](the YouTube Link) at the end of the lines or paragraphs where the snippet's information was used. "
            "IMPORTANT: It is okay for a report to be short if there is not a lot of relevant information in the transcript. The purpose of the report is to summarize relevant real estate market information only. DO NOT explicitly state the fact that the information you are looking for was not discussed, only summarize information that was discussed. "
            "IMPORTANT: If no relevant information is found in the transcript, output [NONE] as the summary. "
            f"\n\n{combined_text}\n\nFinal Summary:"
        )
        
        response = llm.invoke(prompt)
        return response.content
    
    def extract_topics(summary: str) -> List[str]:
        """Extract topics using direct LLM invocation"""
        prompt = (
            "Extract up to 5 key topics from the following city council meeting summary that are relevant to the "
            "commercial real estate industry. The topics should be concise phrases or keywords that capture the main "
            "themes discussed in the summary. Examples of relevant topics include zoning changes, property tax discussions, "
            "bylaw amendments, key motions that could impact the real estate industry, development projects, "
            "real estate related legislation or regulation, affordable housing development, capital budgeting, "
            "real estate market trends etc. Be specific if possible but brief. Only include topics that are directly supported by the summary text. "
            "If no relevant topics are found, return an empty list.\n\n"
            "IMPORTANT: Format the topics as JSON. For example: {\"topics\": [\"Bylaw Amendments\", \"Property Tax Increase\"]}\n\n"
            f"Summary: {summary}\n\nRelevant Topics JSON:"
        )
        
        response = llm.invoke(prompt)
        topics_response = response.content
        
        topics_match = re.search(r"\{.*\}", topics_response, re.DOTALL)
        if topics_match:
            try:
                topics_json = topics_match.group(0)
                topics_dict = json.loads(topics_json)
                return topics_dict.get("topics", [])
            except json.JSONDecodeError as e:
                print(f"Error parsing topics JSON: {e}")
                return []
        else:
            print("No JSON object found in the topics response.")
            return []
    
    def extract_tags(summary: str) -> List[str]:
        """Extract tags using direct LLM invocation"""
        prompt = (
            "Extract up to 5 key topic tags from the following city council meeting summary that are relevant to the "
            "commercial real estate industry. The tags should be concise 1 to 3 word short phrases or single keywords that capture the main "
            "themes discussed in the summary. Examples of relevant topics include zone change, property tax, "
            "bylaw amendment, new development, RE legislation, RE regulation, affordable housing, capital budgeting, "
            "real estate supply, etc. You can use other tags but just make sure to be brief. Only include tags that are directly supported by the summary text. "
            "If no relevant tags are found, return an empty list.\n\n"
            "IMPORTANT: Format the tags as JSON and capitalize words. For example: {\"tags\": [\"Property Tax\", \"Rezoning\"]}\n\n"
            f"Summary: {summary}\n\nRelevant tags JSON:"
        )
        
        response = llm.invoke(prompt)
        tags_response = response.content
        
        tags_match = re.search(r"\{.*\}", tags_response, re.DOTALL)
        if tags_match:
            try:
                tags_json = tags_match.group(0)
                tags_dict = json.loads(tags_json)
                return tags_dict.get("tags", [])
            except json.JSONDecodeError as e:
                print(f"Error parsing tags JSON: {e}")
                return []
        else:
            print("No JSON object found in the tags response.")
            return []
    
    # Main processing loop
    transcripts_summarized = 0
    for transcript in transcripts:
        # Check if summary already exists
        existing = (
            session.query(CouncilTranscriptSummary)
            .filter_by(council_transcript_id=transcript.council_transcript_id)
            .first()
        )
        if existing:
            print(f"Skipping transcript {transcript.council_transcript_id}, summary already exists.")
            continue
        
        meeting_epoch = int(transcript.start_time.timestamp())
        
        # Retrieve chunks for this meeting
        docs = vectorstore.similarity_search(
            real_estate_query,
            k=23,
            filter={"meeting_start_time": {"$eq": meeting_epoch}}
        )
        
        docs_with_sources = []
        for d in docs:
            text_with_source = f"[Source: {d.metadata.get('timestamped_youtube_link', '')}]\n{d.page_content}"
            docs_with_sources.append(Document(page_content=text_with_source, metadata=d.metadata))
        
        # Generate summary
        summary = generate_summary(docs_with_sources)
        
        if "[NONE]" in summary:
            mark_transcript_as_summarized(transcript.council_transcript_id)
            print(f"Skipping transcript {transcript.council_transcript_id}, no relevant info found.")
            continue
        else:
            summary = replace_sources_with_timestamps(summary)
        
        # Extract topics and tags
        extracted_topics = extract_topics(summary)
        extracted_tags = extract_tags(summary)
        
        # Save to DB
        new_summary = CouncilTranscriptSummary(
            council_transcript_id=transcript.council_transcript_id,
            summary=summary,
            date=transcript.date,
            start_time=transcript.start_time,
            meeting_type=transcript.meeting_type,
            video_url=transcript.video_url,
            topics=extracted_topics,
            tags=extracted_tags
        )
        session.add(new_summary)
        session.commit()
        transcripts_summarized += 1
        mark_transcript_as_summarized(transcript.council_transcript_id)
    
    print(f"\n\nTotal transcripts summarized: {transcripts_summarized}")
    return