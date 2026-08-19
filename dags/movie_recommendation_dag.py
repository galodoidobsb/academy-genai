"""
## Use the Airflow Weaviate Provider to query vectors for movie descriptions.

This DAG runs a simple pipeline that queries vectors from a Weaviate Database to
provide movie recommendations based on concept descriptions provided by the user.
"""

from airflow.sdk import dag, task, Param
from airflow.exceptions import AirflowFailException
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from pendulum import datetime, duration
import weaviate.classes as wvc
import logging


# CONSTANTS

# General constants
COLLECTION_NAME = "Movie"

# Weaviate constants
WEAVIATE_USER_CONN_ID = "weaviate_default"

# Start logger
t_log = logging.getLogger("airflow.task")


@dag(
    dag_display_name="​​🍿​ Movie recommendation",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["RAG", "AI", "Tutorial", "weaviate", "movies", "search"],
    default_args={
        "retries": 1,
        "retry_delay": duration(minutes=2),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Recommend movies based on concepts provided by the user.",
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
def movie_recommendation_dag():

    @task
    def check_collection(conn_id: str, collection_name: str) -> None:
        "Check if the provided collection exists and has objects."

        # Create a hook to interact with the Weaviate server
        hook = WeaviateHook(conn_id=WEAVIATE_USER_CONN_ID)

        # Check if the collection exists in the Weaviate database
        collection = hook.get_conn().collections.exists(collection_name)

        if collection:
            t_log.info(f"Collection {collection_name} already exists.")
            objects = hook.get_all_objects(collection_name)
            collection_len = len(objects)
            if collection_len:
                t_log.info(f"Collection '{collection_name}' already has {collection_len} objects. First 5 objects: {objects[:4]}.")
            else:
                raise AirflowFailException(f"Collection '{collection_name}' exists, but is empty! Please, ingest movie data before querying.")
        else:
            raise AirflowFailException(f"The collection {collection_name} does not exist yet. Please, create it and ingest movie data before querying.")

    check_collection_ti = check_collection(conn_id=WEAVIATE_USER_CONN_ID, collection_name=COLLECTION_NAME)

    @task
    def query_embeddings(weaviate_conn_id: str, collection_name: str, **context) -> None:
        "Query the Weaviate instance for movies based on the provided concepts."

        # Create the hook to interact with Weaviate server
        hook = WeaviateHook(weaviate_conn_id)

        # Get concepts passed to the DAG
        movie_concepts = context["params"]["movie_concepts"]

        # Retrieve the collection stored in in Weaviate
        my_movie_collection = hook.get_collection(collection_name)

        # Use the near_text search to retrieve the most relevant movie
        movie = my_movie_collection.query.near_text(
            query=movie_concepts,
            limit=1,
            return_properties=["title", "year", "genre", "description"],
            # Request confidence metrics in metadata
            return_metadata=wvc.query.MetadataQuery(certainty=True, distance=True, creation_time=True)             
        )

        movie_object = movie.objects[0]

        movie_title = movie_object.properties["title"]
        movie_year = movie_object.properties["year"]
        movie_genre = movie_object.properties["genre"]
        movie_description = movie_object.properties["description"]

        search_distance = movie_object.metadata.distance
        search_certainty = movie_object.metadata.certainty

        t_log.info(f"You should watch {movie_title}!")
        t_log.info(
            f"It was filmed in {int(movie_year)} and belongs to the {movie_genre} genre."
        )
        t_log.info(f"Description: {movie_description}")
        t_log.info(
            f"The search is {(100*search_certainty):.2f}% certain and has a distance of {search_distance:.4f}."
        )

    query_embeddings_ti = query_embeddings(weaviate_conn_id=WEAVIATE_USER_CONN_ID, collection_name=COLLECTION_NAME)

    check_collection_ti >> query_embeddings_ti

movie_recommendation_dag()
