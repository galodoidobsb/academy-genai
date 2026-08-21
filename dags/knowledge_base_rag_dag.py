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
import pandas as pd

# Start logger
t_log = logging.getLogger("airflow.task")

# Weaviate constants
_WEAVIATE_USER_CONN_ID = "weaviate_default"
_WEAVIATE_RETURN_PROPERTIES = ["document_title", "section_title", "chunk_content", "section_title_index", "parent_section_index", "heading_level"]
_CHUNKS_LIMIT = 3
_RELATED_CHUNKS_LIMIT = 5
_HYBRID_SEARCH_ALPHA = 0.8

_SCORE_THRESHOLD = 0.85
_CERTAINTY_THRESHOLD = 0.8

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
    def query_embeddings(
        weaviate_conn_id: str,
        certainty_threshold: float = _CERTAINTY_THRESHOLD,
        score_threshold: float = _SCORE_THRESHOLD,
        search_method: str="hybrid",
        **context
    ) -> list[dict]:
        "Query the Weaviate instance for documentation chunks related to the user input."        

        # Get user input passed to the DAG
        collection_name = context["params"]["collection_name"]
        user_input = context["params"]["user_input"]

        # Create the hook to interact with Weaviate server
        hook = WeaviateHook(weaviate_conn_id)

        # Retrieve the collection stored in in Weaviate
        knowledge_base_collection = hook.get_collection(collection_name)

        # Define adequate query and threshold
        if search_method == "hybrid":
            threshold = score_threshold
            document_chunks = knowledge_base_collection.query.hybrid(
                query=user_input,
                limit=_CHUNKS_LIMIT,    # This is an interesting param to experiment with
                alpha=_HYBRID_SEARCH_ALPHA,
                return_properties=_WEAVIATE_RETURN_PROPERTIES,
                return_metadata=wvc.query.MetadataQuery(certainty=True, distance=True, score=True, explain_score=True, creation_time=True)
            )
        elif search_method == "near_text":
            threshold = certainty_threshold
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

            quality = chunk.metadata.score or chunk.metadata.certainty

            if quality < threshold:
                # Fazer o log do objeto descartado
                t_log.info(f"Chunk score/certainty {(100*quality):.2f}% below the defined {(100*threshold):.2f}% threshold, so it will be discarded.")
                t_log.info(
                    f"""Discarded chunk:
                    Document: {chunk.properties["document_title"]}
                    Section: {chunk.properties["section_title"]}
                    Content: {chunk.properties["chunk_content"]}
                """)
                continue
 
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
                "heading_level": chunk.properties["heading_level"],
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

    # NOTE: É importante ORDERNAR os chunks para tentar "reconstruir" a seção
    # Quando uma seção for quebrada em muitos chunks, precisamos reordenar esses chunks
    # para que a documentação faça sentido
    # NOTE: Se a ordenação for um problema (como adicionar um "índice" no splitter???)
    # a solução mais fácil para o MVP seria então simplesmente PULAR o splitter e ingerir
    # cada seção "por inteiro"
    # TODO: Abstract this to a separate module and break into atomic methods
    # Evaluate if we should create a class to hold all Weaviate-related methods
    @task
    def retrieve_related_objects(weaviate_conn_id: str, chunks: list, **context) -> list[dict]:

        from weaviate.classes.query import Filter

        # Get collection name passed to the DAG
        collection_name = context["params"]["collection_name"]

        # Create the hook to interact with Weaviate server
        hook = WeaviateHook(weaviate_conn_id)

        # Retrieve the collection stored in in Weaviate
        knowledge_base_collection = hook.get_collection(collection_name)

        retrieved_chunks_ids = [chunk["chunk_id"] for chunk in chunks]

        related_chunks = []

        # NOTE: This would be a nice async implementation, so all queries could run in parallel
        for chunk in chunks:

            same_section = knowledge_base_collection.query.fetch_objects(
                filters=(
                    Filter.all_of([
                        Filter.by_property("document_title").equal(chunk["document_title"]),
                        Filter.by_property("section_title_index").equal(chunk["section_title_index"]),
                        Filter.not_(Filter.by_id().contains_any(retrieved_chunks_ids)),     # This avoids duplicates
                    ])
                ),
                limit=_RELATED_CHUNKS_LIMIT,
                return_properties=_WEAVIATE_RETURN_PROPERTIES,
            ).objects

            print("Same section chunks: ", same_section)

            children_sections = knowledge_base_collection.query.fetch_objects(
                filters=(
                    Filter.all_of([
                        Filter.by_property("document_title").equal(chunk["document_title"]),
                        Filter.by_property("parent_section_index").equal(chunk["section_title_index"]),
                        Filter.not_(Filter.by_id().contains_any(retrieved_chunks_ids)),     # This avoids duplicates
                    ])
                ),
                limit=_RELATED_CHUNKS_LIMIT,
                return_properties=_WEAVIATE_RETURN_PROPERTIES,
            ).objects

            print("Children sections: ", children_sections)

            if children_sections:
                t_log.info(f"Chunks retrieved from children sections: {len(children_sections)}.")

            if chunk["heading_level"] >= 3:
                siblings_sections = knowledge_base_collection.query.fetch_objects(
                    filters=(
                        Filter.all_of([
                            Filter.by_property("document_title").equal(chunk["document_title"]),
                            Filter.by_property("parent_section_index").equal(chunk["parent_section_index"]),
                            Filter.by_property("heading_level").equal(chunk["heading_level"]),
                            Filter.not_(Filter.by_id().contains_any(retrieved_chunks_ids)),     # This avoids duplicates
                        ])
                    ),
                    limit=_RELATED_CHUNKS_LIMIT,
                    return_properties=_WEAVIATE_RETURN_PROPERTIES,
                ).objects
            else:
                siblings_sections = []

            if siblings_sections:
                t_log.info(f"Chunks retrieved from siblings sections: {len(siblings_sections)}.")

            print("Siblings sections: ", siblings_sections)

            related_sections = same_section + children_sections + siblings_sections

            for related_section in related_sections:
                # Avoids duplicates insertion between siblings and parents
                if str(related_section.uuid) not in retrieved_chunks_ids:
                    related_chunk = {
                        "chunk_id": str(related_section.uuid),
                        "chunk_content": related_section.properties["chunk_content"],
                        "document_title": related_section.properties["document_title"],
                        "section_title": related_section.properties["section_title"],
                        "section_title_index": related_section.properties["section_title_index"],
                        "parent_section_index": related_section.properties["parent_section_index"],
                        "heading_level": related_section.properties["heading_level"],
                    }
                    print(related_chunk)
                    related_chunks.append(related_chunk)
                    retrieved_chunks_ids.append(related_chunk["chunk_id"])

        t_log.info(f"Unique related chunks retrieved: {len(related_chunks)}.")

        return related_chunks

    retrieve_related_objects_ti = retrieve_related_objects(weaviate_conn_id=_WEAVIATE_USER_CONN_ID, chunks=query_embeddings_ti)

    @task
    def combine_chunks(rag_chunks: list[dict], related_chunks: list[dict]):

        combined_chunks = rag_chunks + related_chunks
        sorted_chunks = sorted(combined_chunks, key=lambda x: (x['document_title'], x['section_title_index']))

        t_log.info(f"Total chunks combined for consumption: {len(sorted_chunks)}.")

        return sorted_chunks

    combine_chunks_ti = combine_chunks(rag_chunks=query_embeddings_ti, related_chunks=retrieve_related_objects_ti)

    check_collection_ti >> query_embeddings_ti >> retrieve_related_objects_ti >> combine_chunks_ti

knowledge_base_rag_dag()
