"""
## WIP - Title

WIP - Description
"""

from airflow.sdk import dag, task, task_group
from airflow.exceptions import AirflowFailException
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

    @task
    def fetch_tickets():
        return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    fetch_tickets_ti = fetch_tickets()

    @task
    def filter_tickets(ticket_list):
        return ticket_list[:2]

    filter_tickets_ti = filter_tickets(ticket_list=fetch_tickets_ti)

    @task_group
    def ticket_processing_pipeline(ticket):

        @task
        def request_knowledge_base_context():
            pass

        request_knowledge_base_context_ti = request_knowledge_base_context()

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
