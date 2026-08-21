"""
## WIP - Title

WIP - Description
"""

from airflow.sdk import dag, task, task_group, get_current_context
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from pendulum import datetime, duration
from pathlib import Path
from typing import List
import os
import logging
import pandas as pd
import weaviate.classes.config as wvcc
from weaviate.util import generate_uuid5
from common.constants import *

# Set logging
t_log = logging.getLogger("airflow.task")

# Dag control constants
_EXISTS_TASK_ID = "collection_exists"
_CREATE_TASK_ID = "create_collection"

# Weavieate constants
_WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID")
_VECTORIZER = wvcc.Configure.Vectorizer.text2vec_openai(model="text-embedding-3-small")

# General constants
_INGESTION_PATH = "include/data/"

@dag(
    dag_display_name="📚 Ingest Knowledge Base",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["AI", "Vector", "Embedding", "Ingestion", "Weaviate", "Knowledge Base"],
    default_args={
        "retries": 1,
        "retry_delay": duration(minutes=2),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Ingest knowledge into the vector database for RAG.",
)
def ingest_knowledge_base_dag():

    @task.branch
    def check_for_collection(connection_id: str, collection_name: str) -> str:
        "Check if the provided collection already exists and decide on the next step."

        # Create a hook to interact with the Weaviate server
        hook = WeaviateHook(conn_id=connection_id)

        # Check if the collection exists in the Weaviate database
        has_collection = hook.get_conn().collections.exists(collection_name)

        if has_collection:
            t_log.info(f"Collection {collection_name} already exists.")
            # TODO: Fetch object count from collection.
            # Try to refactor the previous code to reuse the collection object
            return _EXISTS_TASK_ID
        else:
            t_log.info(f"collection {collection_name} does not exist yet.")
            return _CREATE_TASK_ID

    check_for_collection_ti = check_for_collection(connection_id=_WEAVIATE_CONN_ID, collection_name=KNOWLEDGE_BASE_COLLECTION)

    collections_exists_ti = EmptyOperator(task_id=_EXISTS_TASK_ID)

    # NOTE: Vectorizer is defined when creating the COLLECTION, not when ingesting objects
    @task(task_id=_CREATE_TASK_ID)
    def create_collection(connection_id: str, collection_name: str, vectorizer_config: str) -> None:
        "Create a collection with the provided name and vectorizer."

        # Create a hook to interact with the Weaviate server
        hook = WeaviateHook(conn_id=connection_id)

        # Create the collection using the hook
        hook.create_collection(
            name=collection_name,
            vectorizer_config=vectorizer_config,
            properties=[
                wvcc.Property(
                    name="section_id",
                    data_type=wvcc.DataType.UUID,
                ),
                wvcc.Property(
                    name="document_title",
                    data_type=wvcc.DataType.TEXT,
                ),
                wvcc.Property(
                    name="document_path",
                    data_type=wvcc.DataType.TEXT,
                ),
                wvcc.Property(
                    name="section_title_index",
                    data_type=wvcc.DataType.INT,
                ),
                wvcc.Property(
                    name="parent_section_index",
                    data_type=wvcc.DataType.INT,
                ),
                wvcc.Property(
                    name="section_title",
                    data_type=wvcc.DataType.TEXT,
                ),
                wvcc.Property(
                    name="section_reference",
                    data_type=wvcc.DataType.TEXT,
                ),
                wvcc.Property(
                    name="chunk_content",
                    data_type=wvcc.DataType.TEXT,
                ),
            ],
        )

    create_collection_ti = create_collection(connection_id=_WEAVIATE_CONN_ID, collection_name=KNOWLEDGE_BASE_COLLECTION, vectorizer_config=_VECTORIZER)

    weaviate_ready_instance = EmptyOperator(task_id="weaviate_ready", trigger_rule="none_failed")

    @task
    def fetch_documents_paths(ingestion_folders_paths, file_format: str = "*.md") -> List[str]:

        # Define the directory path
        dir_path = Path(ingestion_folders_paths)

        # Get all files recursively
        files = [str(f) for f in dir_path.rglob(file_format) if f.is_file()]

        return files

    fetch_documents_paths_ti = fetch_documents_paths(_INGESTION_PATH)

    # Create a group
    @task_group(
        group_id=f"load_data_pipeline",
        tooltip=f"Read markdown files in sections, and load each document objects into Weaviate.",
        prefix_group_id=False,
    )
    def load_data_group(group_file_path: str, collection_name: str):

        # Import inside the group to avoid DAG parsing overhead
        # TODO: Check if we should actually place the import here, as it
        # will be loaded for each group instance
        # Does it make sense to move the import to the DAG level?
        from common.utils import extract_sections_from_markdown

        @task(
            map_index_template="{{ document_map_index }}"
        )
        def extract_sections(document_path, collection_name: str):

            context = get_current_context()
            context["document_map_index"] = f"Document from: {document_path}"

            sections_generator = extract_sections_from_markdown(
                file_path=document_path,
                collection_name=collection_name,
            )

            # TODO: Check if returning a dataframe is the best option here
            # TODO: Should we "fix" the types inside the df before returning it?
            # Should we go with a Pydantic object instead of a dataframe to validate data? Future version
            return pd.DataFrame(sections_generator)

        extract_sections_ti = extract_sections(document_path=group_file_path, collection_name=collection_name)

        @task(
            map_index_template="{{ document_map_index }}"                
        )
        # TODO: Abstract this to a separate module that can reused
        def chunk_document_sections(df: pd.DataFrame, collection_name: str):

            from langchain_text_splitters import RecursiveCharacterTextSplitter
            from langchain_core.documents import Document

            context = get_current_context()
            context["document_map_index"] = f"Chunks from document: {df['document_title'].iloc[0]}"

            # TODO: Fine-tune the splitter params
            splitter = RecursiveCharacterTextSplitter(
                chunk_size=2000,    # Default = 4000    - Testing with 2000
                chunk_overlap=200,
                length_function=len,
                keep_separator=True
            )

            # Chunk only the content of each section
            # TODO: Replace the apply with a more performant approach
            df["chunks"] = df["section_content"].apply(
                lambda x: splitter.split_documents([Document(page_content=x)])
            )

            df = df.explode("chunks", ignore_index=True)
            df.dropna(subset=["chunks"], inplace=True)

            df["chunk_content"] = df["chunks"].apply(lambda x: x.page_content)
            df["chunk_id"] = df.apply(lambda row: generate_uuid5(identifier=[row['section_id'], row['chunks']], namespace=collection_name), axis=1)

            # Drop both chunks (temporary column) and section_content (replicates full content on each chunk)
            df.drop(["chunks"], inplace=True, axis=1)
            df.drop(["section_content"], inplace=True, axis=1)
            df.reset_index(inplace=True, drop=True)

            t_log.info(f"Chunks DataFrame general structure: {df.info()}")
            t_log.info(f"Chunks DataFrame main stats: {df.describe()}")
            t_log.info(f"Chunks DataFrame first rows: {df.head()}")

            return df

        chunk_document_ti = chunk_document_sections(df=extract_sections_ti, collection_name=collection_name)

        ingest_chunks_into_weaviate_ti = WeaviateIngestOperator(
            task_id="ingest_chunks",
            conn_id=_WEAVIATE_CONN_ID,  # TODO: This should be passed by the task group
            collection_name=collection_name,
            map_index_template="Ingested chunks from document: {{ task.input_data.to_dict()['document_title'][0] }}.",
            input_data=chunk_document_ti,
            uuid_column="chunk_id", # This param avoids duplicates insertion
        )

        extract_sections_ti >> chunk_document_ti >> ingest_chunks_into_weaviate_ti

    load_data_group_instance = load_data_group.partial(collection_name=KNOWLEDGE_BASE_COLLECTION).expand(group_file_path=fetch_documents_paths_ti)

    check_for_collection_ti >> [collections_exists_ti, create_collection_ti] >> weaviate_ready_instance >> fetch_documents_paths_ti >> load_data_group_instance

ingest_knowledge_base_dag()
