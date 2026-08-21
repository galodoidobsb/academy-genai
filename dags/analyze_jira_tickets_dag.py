"""
## WIP - Title

WIP - Description
"""

from airflow.sdk import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.providers.atlassian.jira.operators.jira import JiraOperator
from airflow.providers.atlassian.jira.hooks.jira import JiraHook
from pendulum import datetime, duration
from common.constants import KNOWLEDGE_BASE_COLLECTION
import logging

# Start logger
t_log = logging.getLogger("airflow.task")

# Constants


@dag(
    dag_display_name="​​🤖​ Analyze Jira tickets",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    tags=["AI", "LLM", "Jira", "Tickets"],
    default_args={
        "retries": 1,
        "retry_delay": duration(minutes=2),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Retrieves Jira tickets, call RAG DAG to provide knowledge context, and generate possible solution.",
)
def analyze_jira_tickets_dag():

    # # Task to query Jira tickets using JQL
    # fetch_tickets_ti = JiraOperator(
    #     task_id="fetch_tickets_via_jql",
    #     jira_conn_id="jira_default",          # The Airflow Connection ID configured for Jira
    #     jira_method="jql",                     # The underlying SDK method to trigger
    #     jira_method_args={
    #         "jql": "project = 'BSM' AND status = 'To Do'", # Your JQL query here
    #         # "fields": [],
    #         # "expand": None,
    #         # "validate_query": None,
    #         "limit": 100                        # Number of tickets to return (max is usually 100)
    #     },
    #     # Optional: Parse the raw SDK response before pushing it to XCom
    #     # result_processor=lambda results: [issue["key"] for issue in results.get("issues", [])]
    #     # result_processor=lambda context, result: [issue["key"] for issue in result.get("issues", [])]
    # )

    @task
    def fetch_tickets_via_jql() -> list[dict]:
        hook = JiraHook(jira_conn_id="jira_default")
        client = hook.get_conn()

        # JiraHook 3.3.5 does not propagate a cloud option.
        client.cloud = True

        response = client.enhanced_jql(
            jql='project = BSM AND status = "To Do"',
            fields=["key", "summary", "description", "status"],
            limit=100,
        )

        return response["issues"]

    fetch_tickets_ti = fetch_tickets_via_jql()

    # @task
    # def fetch_tickets():
    #     return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # fetch_tickets_ti = fetch_tickets()

    @task
    def filter_tickets(ticket_list):
        return ticket_list[:5]

    filter_tickets_ti = filter_tickets(ticket_list=fetch_tickets_ti)

    @task_group
    def ticket_processing_pipeline(ticket):

        # NOTE: Trigger RAG DAG using TriggerDagRunOperator
        # Opportunity to use deferrable operator
        @task
        def request_knowledge_base_context():
            pass

        request_knowledge_base_context_ti = request_knowledge_base_context()

        # NOTE: The output should be validated using a Pydantic class
        @task
        def generate_candidate_solution():
            pass

        generate_candidate_solution_ti = generate_candidate_solution()

        # NOTE: In a future version, we should convert this to a branch task
        # Low confidence solutions should be manually evaluated before discarding
        # Implement a full decision cycle combining llm + human judge
        @task
        def evaluate_candidate_solution():
            pass

        evaluate_candidate_solution_ti = evaluate_candidate_solution()

        @task
        def solution_discarded():
            pass

        solution_discarded_ti = solution_discarded()

        @task
        def write_solution_to_ticket():
            pass

        write_solution_to_ticket_ti = write_solution_to_ticket()

        request_knowledge_base_context_ti >> generate_candidate_solution_ti >> evaluate_candidate_solution_ti
        evaluate_candidate_solution_ti >> solution_discarded_ti >> write_solution_to_ticket_ti

    ticket_processing_pipeline_instance = ticket_processing_pipeline.expand(ticket=filter_tickets_ti)

    fetch_tickets_ti >> filter_tickets_ti >> ticket_processing_pipeline_instance

analyze_jira_tickets_dag()
