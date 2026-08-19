"""
## Delete a collection in Weaviate

CAUTION: This DAG will delete a specified collection in your Weaviate instance.
Meant to be used during development to reset Weaviate.
Please use it with caution.
"""

from airflow.sdk import dag, task, Param
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
import os

# Provider your Weaviate conn_id here.
WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID", "weaviate_default")
# Provide the collection name to delete the schema.
WEAVIATE_COLLECTION_TO_DELETE = "MY_SCHEMA_TO_DELETE"


@dag(
    dag_display_name="🧼 Delete a Schema/Collection in Weaviate",
    schedule=None,
    start_date=None,
    catchup=False,
    description="CAUTION! Will delete a collection in Weaviate!",
    tags=["helper"],
    params={
        "collection_name": Param(
            WEAVIATE_COLLECTION_TO_DELETE,
            type="string",
            description="Weaviate collection name to delete"

        )
    }
)
def clear_weaviate():

    @task(
        task_display_name=f"Delete collection in Weaviate",
    )
    def delete_weaviate_collection(**context):

        # connect to Weaviate using the Airflow connection `conn_id`
        hook = WeaviateHook(WEAVIATE_CONN_ID)

        # Get param passed to the DAG
        collection_name = context["params"]["collection_name"]

        # delete collection
        hook.delete_collections(collection_name, if_error="stop")

    delete_weaviate_collection_ti = delete_weaviate_collection()


clear_weaviate()
