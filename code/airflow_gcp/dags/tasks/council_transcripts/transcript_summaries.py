def transcript_vectors_to_summaries():
    import os
    import re
    import random
    from datetime import datetime, timedelta, timezone
    from dotenv import load_dotenv
    from pathlib import Path
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine, Column, Integer, Text, Date, DateTime
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from pinecone import Pinecone
    from langchain_openai import ChatOpenAI, OpenAIEmbeddings
    from langchain_pinecone import PineconeVectorStore
    from langchain.chains import load_summarize_chain
    from langchain.docstore.document import Document
    from langchain.prompts import PromptTemplate


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
        video_url = Column(Text)
        created_at = Column(DateTime(timezone=True), server_default="now()")

    engine = get_db_engine()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # ---------- Get transcripts ----------
    one_months_ago = (datetime.now(timezone.utc) - timedelta(days=90))
    transcripts = (
        session.query(CouncilTranscript)
        .filter(
            CouncilTranscript.date >= one_months_ago,
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
    vectorstore = PineconeVectorStore(embedding=embeddings, index=pc.Index(index_name))

    # ---------- LLM setup ----------
    llm = ChatOpenAI(
        openai_api_key=OPENAI_API_KEY,
        model="gpt-4.1-mini",
        temperature=0.0
    )

    # Real estate query to steer retrieval
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

    transcripts_summarized = 0

    for transcript in transcripts:
        
        # Check if a summary already exists for this transcript
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
        retriever = vectorstore.as_retriever(
            search_kwargs={
                "k": 23,
                "filter": {"meeting_start_time": {"$eq": meeting_epoch}}
            }
        )

        docs = retriever.get_relevant_documents(real_estate_query)
        docs_with_sources = []
        for d in docs:
            text_with_source = f"[Source: {d.metadata.get('timestamped_youtube_link', '')}]\n{d.page_content}"
            docs_with_sources.append(Document(page_content=text_with_source, metadata=d.metadata))


        # map_prompt = PromptTemplate(
        #     template=(
        #         "Summarize the following city council transcript excerpt, focusing ONLY on topics that"
        #         "could be relevant to the commercial real estate industry (zoning, property bylaws,"
        #         "property tax issues, general discussion about real estate or topics relevant to real estate"
        #         "investors, brokers or developers etc.). When stating factual information from the transcript"
        #         "excerpt, include the provided source link in parentheses for attribution.\n\n{text}\n\nSummary:"
        #     ),
        #     input_variables=["text"],
        # )


        combine_prompt = PromptTemplate(
            template=(
                "Combine the following city council transcript snippets into a single coherent markdown report. Extract "
                "important information that could impact the commercial real estate industry, including zoning "
                "and bylaw changes, carried bylaw motions, property tax discussions, general discussion about real estate, or topics "
                "relevant to real estate investors, brokers or developers, etc. Make the report as concise as "
                "possible while using as many facts from the text as you can. Focus on changes that are happening "
                "and any arguments for or against these changes. For zoning changes, focus more on summarizing reasoning "
                "for why the change is supported or not supported and only provide basic information about the zones themselves. "
                "Only use information discussed in the text and do not add any of your own opinions or interpretations of the information. "
                "IMPORTANT: Include inline hyperlinked source citations (Eg. [[1]](the YouTube Link) at the end of the lines or paragraphs where the snippet's information was used. "
                "IMPORTANT: It is okay for a report to be short if there is not a lot of relevant information in the transcript. The purpose of the report is to summarize relevant real estate market information only. "
                "IMPORTANT: If no relevant information is found in the transcript, output [NONE] as the summary. "
                "\n\n{text}\n\nFinal Summary:"
            ),
            input_variables=["text"],
        )

        # map_prompt=map_prompt,
        chain = load_summarize_chain(
            llm,
            chain_type="stuff",
            prompt=combine_prompt, 
            verbose=False
        )

        def format_timestamp(seconds: int) -> str:
            td = timedelta(seconds=seconds)
            h, remainder = divmod(td.seconds, 3600)
            m, s = divmod(remainder, 60)
            h += td.days * 24
            return f"{h}:{m:02}:{s:02}"

        def replace_sources_with_timestamps(text: str) -> str:
            # Handles both &t= and ?t= cases
            pattern = re.compile(r"\[\[(\d+)]]\((https?://[^\s)]+?[&?]t=(\d+)s)\)")
            
            def replacer(match):
                seconds = int(match.group(3))
                timestamp = format_timestamp(seconds)
                return f"[[{timestamp}]]({match.group(2)})"
            
            return pattern.sub(replacer, text)

        def mark_transcript_as_summarized(council_transcript_id):
            """Mark a transcript as summarized in the database"""
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

        # After creating summary
        summary = chain.run(docs_with_sources)
        if "[NONE]" in summary:
            mark_transcript_as_summarized(transcript.council_transcript_id)
            print(f"Skipping transcript {transcript.council_transcript_id}, no relevant info found.")
            continue
        else:
            summary = replace_sources_with_timestamps(summary)

        # Save to DB
        new_summary = CouncilTranscriptSummary(
            council_transcript_id=transcript.council_transcript_id,
            summary=summary,
            date = transcript.date,
            start_time = transcript.start_time,
            meeting_type = transcript.meeting_type,
            video_url = transcript.video_url
        )

        session.add(new_summary)
        session.commit()
        transcripts_summarized += 1

        mark_transcript_as_summarized(transcript.council_transcript_id)

    print(f"\n\nTotal transcripts summarized: {transcripts_summarized}")

    return