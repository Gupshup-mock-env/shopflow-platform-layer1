"""Celery configuration for the ShopFlow order pipeline.

Loaded by ``orders.celery_app`` via ``app.config_from_object("celeryconfig")``,
so it must sit on the import path of every process that touches the pipeline.
"""

broker_connection_retry_on_startup = True
broker_connection_max_retries = 120
broker_heartbeat = 30
broker_pool_limit = 10

task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
task_ignore_result = True
task_acks_late = True
task_create_missing_queues = True
task_default_queue = "celery"
task_time_limit = 120
task_soft_time_limit = 90

task_routes = {
    "orders.tasks.process_payment": {"queue": "payments"},
}

worker_prefetch_multiplier = 1
worker_send_task_events = False
worker_hijack_root_logger = False

timezone = "UTC"
enable_utc = True
