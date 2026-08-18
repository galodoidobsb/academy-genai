"""
## Simple RAG DAG to ingest new knowledge data into a vector database

This DAG ingests text data from markdown files, chunks the text, and then ingests 
the chunks into a Weaviate vector database.
"""

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from pendulum import datetime, duration
from pathlib import Path
import os
import re
import logging
import pandas as pd
import weaviate.classes.config as wvcc
from weaviate.util import generate_uuid5


# Set logging
t_log = logging.getLogger("airflow.task")

# Private variables used in the DAG
_INGESTION_FOLDERS_LOCAL_PATHS = os.getenv("INGESTION_FOLDERS_LOCAL_PATHS")

_WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID")
_WEAVIATE_CLASS_NAME = os.getenv("WEAVIATE_CLASS_NAME")
_WEAVIATE_VECTORIZER = os.getenv("WEAVIATE_VECTORIZER")
_WEAVIATE_SCHEMA_PATH = os.getenv("WEAVIATE_SCHEMA_PATH")

_CREATE_COLLECTION_TASK_ID = "create_collection"
_COLLECTION_ALREADY_EXISTS_TASK_ID = "collection_already_exists"

VECTORIZER = wvcc.Configure.Vectorizer.text2vec_transformers()
# VECTORIZER = wvcc.Configure.Vectorizer.text2vec_openai(model="ada")


@dag(
    dag_display_name="📚 Ingest Knowledge Base",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["RAG", "AI", "Tutorial"],
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Ingest knowledge into the vector database for RAG.",
)
def updated_rag_dag():

    @task.branch
    def check_collection(
        conn_id: str,
        collection_name: str,
    ) -> str:
        """
        Check if the target collection (formerly known as class) exists in the Weaviate schema.
        Args:
            conn_id: The connection ID to use.
            collection_name: The name of the collection to check.
            create_collection_task_id: The task ID to execute if the collection does not exist.
            collection_already_exists_task_id: The task ID to execute if the collection already exists.
        Returns:
            str: Task ID of the next task to execute.
        """

        # connect to Weaviate using the Airflow connection `conn_id`
        hook = WeaviateHook(conn_id)

        # check if the collection exists in the Weaviate database
        collection = hook.get_conn().collections.exists(collection_name)

        if collection:
            t_log.info(f"Collection {collection_name} already exists.")
            return _COLLECTION_ALREADY_EXISTS_TASK_ID
        else:
            t_log.info(f"collection {collection_name} does not exist yet.")
            return _CREATE_COLLECTION_TASK_ID

    check_collection_instance = check_collection(
        conn_id=_WEAVIATE_CONN_ID,
        collection_name=_WEAVIATE_CLASS_NAME,
    )

    @task
    def create_collection(
        conn_id: str,
        collection_name: str,
        vectorizer: str,
        schema_json_path: str
    ) -> None:
        """
        Create a collection in the Weaviate schema.
        Args:
            conn_id: The connection ID to use.
            collection_name: The name of the collection to create.
            vectorizer: The vectorizer to use for the collection.
            schema_json_path: The path to the schema JSON file.
        """

        import json

        weaviate_hook = WeaviateHook(conn_id)

        with open(schema_json_path) as f:
            schema = json.load(f)
            class_obj = next(
                (item for item in schema["classes"] if item["class"] == collection_name),
                None,
            )
            class_obj["vectorizer"] = vectorizer

        weaviate_hook.create_collection(name=collection_name, vectorizer_config=VECTORIZER)       

    collection_exists_instance = EmptyOperator(task_id=_COLLECTION_ALREADY_EXISTS_TASK_ID)

    weaviate_ready_instance = EmptyOperator(task_id="weaviate_ready", trigger_rule="none_failed")

    @task
    def fetch_ingestion_folders_local_paths(ingestion_folders_local_paths):

        # get all the folders in the given location
        folders = os.listdir(ingestion_folders_local_paths)

        # return the full path of the folders
        return [os.path.join(ingestion_folders_local_paths, folder) for folder in folders]

    fetch_ingestion_folders_local_paths_instance = fetch_ingestion_folders_local_paths(_INGESTION_FOLDERS_LOCAL_PATHS)

    check_collection_instance >> [
        create_collection(
            conn_id=_WEAVIATE_CONN_ID,
            collection_name=_WEAVIATE_CLASS_NAME,
            vectorizer=VECTORIZER,
            schema_json_path=_WEAVIATE_SCHEMA_PATH,
        ),
        collection_exists_instance
    ] >> weaviate_ready_instance

    @task(
        map_index_template="{{ my_custom_map_index }}"
    )
    def extract_document_text(ingestion_folder_local_path):
        """
        Extract information from markdown files in a folder.
        Args:
            folder_path (str): Path to the folder containing markdown files.
        Returns:
            pd.DataFrame: A list of dictionaries containing the extracted information.
        """

        # NOTE: We need the get_current_context method here to MODIFY the current context,
        # passing he map index value at runtime
        # When using def task_method(arg, **context), we can only access it, but we
        # can't update the "root" dictionary object
        context = get_current_context()
        context["my_custom_map_index"] = ingestion_folder_local_path

        files = [
            f for f in os.listdir(ingestion_folder_local_path) if f.endswith(".md")
        ]

        titles = []
        texts = []

        for file in files:
            # file_path = os.path.join(ingestion_folder_local_path, file)
            file_path = Path(ingestion_folder_local_path) / Path(file)
            
            titles.append(file_path.stem)
            # titles.append(file.split(".")[0])

            with open(file_path, "r", encoding="utf-8") as f:
                texts.append(f.read())

        document_df = pd.DataFrame(
            {
                "folder_path": ingestion_folder_local_path,
                "title": titles,
                "text": texts,
            }
        )
        # NOTE: Inserting the caption metadata is not working
        document_df.style.set_caption(f"DataFrame for {ingestion_folder_local_path} folder files")

        t_log.info(f"DataFrame general structure: {document_df.info()}")
        t_log.info(f"DataFrame main stats: {document_df.describe()}")
        t_log.info(f"DataFrame first rows: {document_df.head()}")

        return document_df

    # NOTE: Expand expects kwargs, if we just pass the pargument value, it doesnt work
    extract_document_text_instance = extract_document_text.expand(
        ingestion_folder_local_path=fetch_ingestion_folders_local_paths_instance
    )

    fetch_ingestion_folders_local_paths_instance >> extract_document_text_instance

    # @task(
    #     map_index_template="{{ my_custom_map_index }}"
    # )
    # def chunk_text(df):
    #     """
    #     Chunk the text in the DataFrame.
    #     Args:
    #         df (pd.DataFrame): The DataFrame containing the text to chunk.
    #     Returns:
    #         pd.DataFrame: The DataFrame with the text chunked.
    #     """

    #     from langchain.text_splitter import RecursiveCharacterTextSplitter
    #     from langchain.schema import Document

    #     context = get_current_context()
    #     # context["my_custom_map_index"] = df.style.caption
    #     context["my_custom_map_index"] = f"Chunked files from a df of length: {len(df)}."

    #     splitter = RecursiveCharacterTextSplitter()

    #     df["chunks"] = df["text"].apply(
    #         lambda x: splitter.split_documents([Document(page_content=x)])
    #     )
    #     # df.head()

    #     df = df.explode("chunks", ignore_index=True)
    #     df.dropna(subset=["chunks"], inplace=True)
    #     # df.head()
    #     # for chunk_object in df["chunks"]:
    #     #     print(chunk_object)
    #     #     print(chunk_object.__dict__)

    #     df["text"] = df["chunks"].apply(lambda x: x.page_content)
    #     df.drop(["chunks"], inplace=True, axis=1)
    #     df.reset_index(inplace=True, drop=True)

    #     t_log.info(f"Chunks DataFrame general structure: {df.info()}")
    #     t_log.info(f"Chunks DataFrame main stats: {df.describe()}")
    #     t_log.info(f"Chunks DataFrame first rows: {df.head()}")

    #     return df

    # chunk_text_obj = chunk_text.expand(df=extract_document_text_obj)

    # ingest_data = WeaviateIngestOperator.partial(
    #     task_id="ingest_data",
    #     conn_id="weaviate_default",
    #     class_name=_WEAVIATE_CLASS_NAME,
    #     # NOTE: Use jinja templating to pass custom map index as well
    #     map_index_template="Ingested files from: {{ task.input_data.to_dict()['folder_path'][0] }}.",
    # ).expand(input_data=chunk_text_obj)

    # check_class_obj >> [create_class_obj, class_already_exists] >> weaviate_ready
    # fetch_ingestion_folders_local_paths_obj >> extract_document_text_obj >> chunk_text_obj
    # # [weaviate_ready, chunk_text_obj] >> ingest_data
    # chain(
    #     [chunk_text_obj, weaviate_ready],
    #     ingest_data,
    # )

updated_rag_dag()
