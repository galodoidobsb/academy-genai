"""
## WIP - Title

WIP - Description
"""

from airflow.sdk import dag, task, Param
from airflow.exceptions import AirflowFailException
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from pendulum import datetime, duration
from typing import List
from common.constants import KNOWLEDGE_BASE_COLLECTION
import weaviate.classes as wvc
import logging
import pandas as pd

# Start logger
t_log = logging.getLogger("airflow.task")

# Weaviate constants
_WEAVIATE_USER_CONN_ID = "weaviate_default"
_WEAVIATE_RETURN_PROPERTIES = ["document_title", "section_title", "chunk_content", "section_title_index", "parent_section_index"]
_CHUNKS_LIMIT = 3
_HYBRID_SEARCH_ALPHA = 0.8

_WEAVIATE_RETURN_METADATA = ""


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
        "collection_name": Param(
            KNOWLEDGE_BASE_COLLECTION,
            type="string",
            description=(
                "Collection name on the Weaviate database."
            ),
        ),
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
    def check_collection(conn_id: str, **context) -> None:
        "Check if the provided collection exists and has objects."

        # Get param passed to the DAG
        collection_name = context["params"]["collection_name"]

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

    check_collection_ti = check_collection(conn_id=_WEAVIATE_USER_CONN_ID)  

    @task
    def query_embeddings(weaviate_conn_id: str, search_method: str="hybrid", **context) -> List:
        "Query the Weaviate instance for documentation chunks related to the user input."

        # Get user input passed to the DAG
        collection_name = context["params"]["collection_name"]
        user_input = context["params"]["user_input"]

        # Create the hook to interact with Weaviate server
        hook = WeaviateHook(weaviate_conn_id)

        # Retrieve the collection stored in in Weaviate
        knowledge_base_collection = hook.get_collection(collection_name)

        if search_method == "hybrid":
            document_chunks = knowledge_base_collection.query.hybrid(
                query=user_input,
                limit=_CHUNKS_LIMIT,    # This is an interesting param to experiment with
                alpha=_HYBRID_SEARCH_ALPHA,
                return_properties=_WEAVIATE_RETURN_PROPERTIES,
                return_metadata=wvc.query.MetadataQuery(certainty=True, distance=True, score=True, explain_score=True, creation_time=True)
            )
        elif search_method == "near_text":
            document_chunks = knowledge_base_collection.query.near_text(
                query=user_input,
                limit=_CHUNKS_LIMIT,    # This is an interesting param to experiment with
                return_properties=_WEAVIATE_RETURN_PROPERTIES,
                return_metadata=wvc.query.MetadataQuery(certainty=True, distance=True, creation_time=True)
            )
        else:
            raise AirflowFailException(f"Search method {search_method} is not supported. Please, chose either 'hybrid' or 'near_text'.")

        t_log.info(f"Chunks retrieved: {len(document_chunks.objects)}.")

        chunks = []

        for chunk in document_chunks.objects:
            chunk_id = str(chunk.uuid)
            document_title = chunk.properties["document_title"]
            section_title = chunk.properties["section_title"]
            chunk_content = chunk.properties["chunk_content"]
            # Properties of the near_text search
            search_distance = chunk.metadata.distance
            search_certainty = chunk.metadata.certainty
            # Property of the hybrid search
            search_score = chunk.metadata.score
            explain_score = chunk.metadata.explain_score

            chunk_properties = {
                "chunk_id": chunk_id,
                "chunk_content": chunk_content,
                "document_title": document_title,
                "section_title": section_title,
                "section_title_index": chunk.properties["section_title_index"],
                "parent_section_index": chunk.properties["parent_section_index"],
            }

            t_log.info(f"Chunk found in document: {document_title}")
            t_log.info(f"Chunk extracted from section: {section_title}")
            if search_method == "hybrid":
                chunk_properties.update(
                    {
                        "search_score": search_score,
                        "explain_score": explain_score,
                    }
                )
                t_log.info(f"Chunk search score is {(100*search_score):.2f}%.")
                t_log.info(f"Chunk score explanation: {explain_score}.")
            elif search_method == "near_text":
                chunk_properties.update(
                    {
                        "search_certainty": search_certainty,
                        "search_distance": search_distance,
                    }
                )
                t_log.info(f"Chunk search certainty is {(100*search_certainty):.2f}%.")
                t_log.info(f"Chunk search distance is {(search_distance):.4f}.")

            chunks.append(chunk_properties)

        return chunks

    query_embeddings_ti = query_embeddings(weaviate_conn_id=_WEAVIATE_USER_CONN_ID)

    # NOTE: Após o retrieval, precisamos desduplicar os chunks (utilizar o chunk_id)
    # Também seria importante ORDERNAR os chunks para tentar "reconstruir" a seção
    # Quando uma seção for quebrada em muitos chunks, precisamos reordenar esses chunks
    # para que a documentação faça sentido
    # NOTE: Se a ordenação for um problema (como adicionar um "índice" no splitter???)
    # a solução mais fácil para o MVP seria então simplesmente PULAR o splitter e ingerir
    # cada seção "por inteiro"
    @task
    def retrieve_related_objects(weaviate_conn_id: str, chunks):
        pass

    retrieve_related_objects_ti = retrieve_related_objects(weaviate_conn_id=_WEAVIATE_USER_CONN_ID, chunks=query_embeddings_ti)

    # TODO: Next step is to implement the related-objects retrieval
    # Only retrieve chunks of a defined score threshold (experiment with 0.7 or 0.8)

    check_collection_ti >> query_embeddings_ti >> retrieve_related_objects_ti

knowledge_base_rag_dag()
