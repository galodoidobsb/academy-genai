"""
## Use the Airflow Weaviate Provider to generate and query vectors for movie descriptions

This DAG runs a simple MLOps pipeline that uses the Weaviate Provider to import
movie descriptions, generate vectors for them, and query the vectors for movies based on
concept descriptions.
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from weaviate.util import generate_uuid5
from pendulum import datetime, duration
from typing import List
import weaviate.classes.config as wvcc
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
# set the vectorizer to text2vec-openai if you want to use the openai model
# note that using the OpenAI vectorizer requires a valid API key in the
# AIRFLOW_CONN_WEAVIATE_DEFAULT connection.
# If you want to use a different vectorizer model
# (https://weaviate.io/developers/weaviate/model-providers)
# make sure to also add it to the weaviate configuration's `ENABLE_MODULES` list
# for example in the docker-compose.override.yml file
VECTORIZER = wvcc.Configure.Vectorizer.text2vec_transformers()
# VECTORIZER = wvcc.Configure.Vectorizer.text2vec_openai(model="ada")

# Start logger
t_log = logging.getLogger("airflow.task")


@dag(
    dag_display_name="🎞️​ Query movies from vector database",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["RAG", "AI", "Tutorial", "weaviate", "movies"],
    default_args={
        "retries": 1,
        "retry_delay": duration(minutes=2),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Retrieve movies from the vector database.",
    params={
        "movie_concepts": Param(
            ["innovation", "friends"],
            type="array",
            description=(
                "What kind of movie do you want to watch today?"
                + " Add one concept per line."
            ),
        ),
    },
)
def query_movie_vectors_dag():

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

    exists_ti = EmptyOperator(
        task_id=EXISTS_TASK_ID
    )

    @task(task_id=CREATE_TASK_ID)
    def create_collection(collection_name: str, vectorizer_config: str) -> None:
        "Create a collection with the provided name and vectorizer."

        # Create a hook to interact with the Weaviate server
        hook = WeaviateHook(conn_id=WEAVIATE_USER_CONN_ID)

        # Create the collection using the hook
        hook.create_collection(name=collection_name, vectorizer_config=VECTORIZER)

    create_collection_ti = create_collection(collection_name=COLLECTION_NAME, vectorizer_config=VECTORIZER)

    # NOTE: This should not be a task, because the Weaviate Operator downstream
    # requires a python callable
    # TODO: After validating the whole DAG, we should refactor this to separate
    # data import/load/treatment from ingestion. Also, we should evaluate
    # a new version where we decouple vectorization from ingestion
    def import_data_from_source(text_file_path: str, collection_name: str) -> List:
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
                            identifier=[title, year, genre, description],
                            namespace=collection_name,
                        ),
                        "title": title,
                        "year": year,
                        "genre": genre,
                        "description": description,
                    }
                )

            t_log.info(f"Created a list with {len(data)} elements while skipping {num_skipped_lines} lines.")

            # NOTE: Data is returned inside the context manager. Why?
            return data

    # NOTE: The operator is extremely flexible:
    # If data does not have vectors, it creates them before ingesting
    ingest_data_ti = WeaviateIngestOperator(
        task_id="ingest_data",
        trigger_rule="none_failed",
        conn_id=WEAVIATE_USER_CONN_ID,
        collection_name=COLLECTION_NAME,
        # input_json=import_data_func(
        #     text_file_path=TEXT_FILE_PATH, collection_name=COLLECTION_NAME
        # ),
        input_data=import_data_from_source(
            text_file_path=TEXT_FILE_PATH,
            collection_name=COLLECTION_NAME
        )
    )

    check_for_collection_ti >> [exists_ti, create_collection_ti] >> ingest_data_ti

query_movie_vectors_dag()