"""
## Use the Airflow Weaviate Provider to generate and query vectors for movie descriptions

This DAG runs a simple MLOps pipeline that uses the Weaviate Provider to import
movie descriptions, generate vectors for them, and query the vectors for movies based on
concept descriptions.
"""

from airflow.sdk import dag, task, Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from weaviate.util import generate_uuid5
from pendulum import datetime, duration
from typing import List
import weaviate.classes.config as wvcc
import weaviate.classes as wvc
import logging
import re


# CONSTANTS

# General constants
TEXT_FILE_PATH = "include/movie_data.txt"
# the base collection name is used to create a unique collection name for the vectorizer
# note that it is best practice to capitalize the first letter of the collection name
COLLECTION_NAME = "Movie"

# Dag control constants
EXISTS_TASK_ID = "collection_exists"
CREATE_TASK_ID = "create_collection"

# Weaviate constants
WEAVIATE_USER_CONN_ID = "weaviate_default"
VECTORIZER = wvcc.Configure.Vectorizer.text2vec_transformers()
# NOTE: using the OpenAI vectorizer requires a valid API key in the AIRFLOW_CONN_WEAVIATE_DEFAULT connection.
# If you want to use a different vectorizer model (https://weaviate.io/developers/weaviate/model-providers)
# make sure to also add it to the weaviate configuration's `ENABLE_MODULES` list
OPEN_AI_VECTORIZER = wvcc.Configure.Vectorizer.text2vec_openai(model="text-embedding-3-small")

# Start logger
t_log = logging.getLogger("airflow.task")


@dag(
    dag_display_name="​​🎬​ Ingest movies into vector database",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["embedding", "AI", "Tutorial", "weaviate", "movies", "ingest"],
    default_args={
        "retries": 1,
        "retry_delay": duration(minutes=2),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Ingest movies data into the vector database.",
    params={
        "collection_name": Param(
            COLLECTION_NAME,
            type="string",
            description=(
                "Name of the collection in Weaviate database to ingest the data."
            ),
        ),
    },
)
def ingest_movie_vectors_dag():

    @task.branch
    def check_for_collection(conn_id: str, collection_name: str) -> str:
        "Check if the provided collection already exists and decide on the next step."

        # Create a hook to interact with the Weaviate server
        hook = WeaviateHook(conn_id=WEAVIATE_USER_CONN_ID)

        # Check if the collection exists in the Weaviate database
        collection = hook.get_conn().collections.exists(collection_name)

        if collection:
            t_log.info(f"Collection {collection_name} already exists.")
            return EXISTS_TASK_ID
        else:
            t_log.info(f"collection {collection_name} does not exist yet.")
            return CREATE_TASK_ID

    check_for_collection_ti = check_for_collection(conn_id=WEAVIATE_USER_CONN_ID, collection_name=COLLECTION_NAME)

    @task(task_id=EXISTS_TASK_ID)
    def display_objects_in_collection(weaviate_conn_id: str, collection_name: str):
        "Display objects stored in the existing collection."

        # Create the hook to interact with Weaviate server
        hook = WeaviateHook(weaviate_conn_id)

        # Get all objects stores in the collection
        objects = hook.get_all_objects(collection_name)

        t_log.info(f"The collection already has {len(objects)} objects. First 5 objects: {objects[:4]}.")

    collections_exists_ti = display_objects_in_collection(weaviate_conn_id=WEAVIATE_USER_CONN_ID, collection_name=COLLECTION_NAME)

    # NOTE: Vectorizer is defined when creating the COLLECTION, not when ingesting objects
    @task(task_id=CREATE_TASK_ID)
    def create_collection(collection_name: str, vectorizer_config: str) -> None:
        "Create a collection with the provided name and vectorizer."

        # Create a hook to interact with the Weaviate server
        hook = WeaviateHook(conn_id=WEAVIATE_USER_CONN_ID)

        # Create the collection using the hook
        hook.create_collection(name=collection_name, vectorizer_config=vectorizer_config)

    create_collection_ti = create_collection(collection_name=COLLECTION_NAME, vectorizer_config=OPEN_AI_VECTORIZER)

    @task(
        task_id="read_data_from_source",
        trigger_rule="none_failed",
    )
    def read_data_from_source(text_file_path: str, collection_name: str) -> List:
        "Read the text file and create a list of dicts for ingestion to Weaviate."

        with open(text_file_path, "r") as f:
            lines = f.readlines()

            num_skipped_lines = 0
            data = []
            for line in lines:
                parts = line.split(":::")
                title_year = parts[1].strip()
                match = re.match(r"(.+) \((\d{4})\)", title_year)
                try:
                    title, year = match.groups()
                    year = int(year)
                # skip malformed lines
                except:
                    num_skipped_lines += 1
                    continue

                genre = parts[2].strip()
                description = parts[3].strip()

                data.append(
                    {
                        "movie_id": generate_uuid5(
                            identifier=[title, year],
                            namespace=collection_name,
                        ),
                        "title": title,
                        "year": year,
                        "genre": genre,
                        "description": description,
                    }
                )

        t_log.info(f"Created a list with {len(data)} elements while skipping {num_skipped_lines} lines.")
        return data

    read_data_from_source_ti = read_data_from_source(text_file_path=TEXT_FILE_PATH, collection_name=COLLECTION_NAME)

    # NOTE: The operator is extremely flexible:
    # If data does not have vectors, it creates them before ingesting
    ingest_data_ti = WeaviateIngestOperator(
        task_id="ingest_data",
        conn_id=WEAVIATE_USER_CONN_ID,
        collection_name=COLLECTION_NAME,
        input_data=read_data_from_source_ti,
        uuid_column="movie_id", # This param is required to avoid duplicates insertion
    )

    check_for_collection_ti >> [collections_exists_ti, create_collection_ti] >> read_data_from_source_ti >> ingest_data_ti

ingest_movie_vectors_dag()
