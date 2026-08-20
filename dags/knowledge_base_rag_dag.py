"""
## WIP - Title

WIP - Description
"""

from airflow.sdk import dag, task, Param
from airflow.exceptions import AirflowFailException
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from pendulum import datetime, duration
from common.constants import KNOWLEDGE_BASE_COLLECTION
import weaviate.classes as wvc
import logging

# Start logger
t_log = logging.getLogger("airflow.task")

# Weaviate constants
_WEAVIATE_USER_CONN_ID = "weaviate_default"


@dag(
    dag_display_name="​🔎​​ Retrieve content from Knowledge Base",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["RAG", "AI", "Weaviate", "Retrieval", "Knowledge Base"],
    default_args={
        "retries": 1,
        "retry_delay": duration(minutes=2),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Retrieves content from vectorized database related to the user input.",
    params={
        "user_input": Param(
            None,
            type="string",
            description=(
                "Add the ticket content that needs to be searched in the database"
            ),
        ),
    },
)
def knowledge_base_rag_dag():

    @task
    def check_collection(conn_id: str, collection_name: str) -> None:
        "Check if the provided collection exists and has objects."

        # Create a hook to interact with the Weaviate server
        hook = WeaviateHook(conn_id=conn_id)

        # Check if the collection exists in the Weaviate database
        collection = hook.get_conn().collections.exists(collection_name)

        if collection:
            t_log.info(f"Collection {collection_name} already exists.")
            objects = hook.get_all_objects(collection_name)
            collection_len = len(objects)
            if collection_len:
                t_log.info(f"Collection '{collection_name}' already has {collection_len} objects. First 5 objects: {objects[:4]}.")
            else:
                raise AirflowFailException(f"Collection '{collection_name}' exists, but is empty! Please, ingest data before querying.")
        else:
            raise AirflowFailException(f"The collection {collection_name} does not exist yet. Please, create it and ingest data before querying.")

    check_collection_ti = check_collection(conn_id=_WEAVIATE_USER_CONN_ID, collection_name=KNOWLEDGE_BASE_COLLECTION)

    check_collection_ti

    # @task
    # def query_embeddings(weaviate_conn_id: str, collection_name: str, **context) -> None:
    #     "Query the Weaviate instance for movies based on the provided concepts."

    #     # Create the hook to interact with Weaviate server
    #     hook = WeaviateHook(weaviate_conn_id)

    #     # Get concepts passed to the DAG
    #     movie_concepts = context["params"]["movie_concepts"]

    #     # Retrieve the collection stored in in Weaviate
    #     my_movie_collection = hook.get_collection(collection_name)

    #     # Use the near_text search to retrieve the most relevant movie
    #     movie = my_movie_collection.query.near_text(
    #         query=movie_concepts,
    #         limit=1,
    #         return_properties=["title", "year", "genre", "description"],
    #         # Request confidence metrics in metadata
    #         return_metadata=wvc.query.MetadataQuery(certainty=True, distance=True, creation_time=True)             
    #     )

    #     movie_object = movie.objects[0]

    #     movie_title = movie_object.properties["title"]
    #     movie_year = movie_object.properties["year"]
    #     movie_genre = movie_object.properties["genre"]
    #     movie_description = movie_object.properties["description"]

    #     search_distance = movie_object.metadata.distance
    #     search_certainty = movie_object.metadata.certainty

    #     t_log.info(f"You should watch {movie_title}!")
    #     t_log.info(
    #         f"It was filmed in {int(movie_year)} and belongs to the {movie_genre} genre."
    #     )
    #     t_log.info(f"Description: {movie_description}")
    #     t_log.info(
    #         f"The search is {(100*search_certainty):.2f}% certain and has a distance of {search_distance:.4f}."
    #     )

    # query_embeddings_ti = query_embeddings(weaviate_conn_id=WEAVIATE_USER_CONN_ID, collection_name=COLLECTION_NAME)

knowledge_base_rag_dag()
