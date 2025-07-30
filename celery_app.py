# from celery import Celery

# celery_app = Celery(
#     "Scraped_Api",
#     broker="redis://localhost:6379/0",
#     backend="redis://localhost:6379/0"
# )

# celery_app.conf.task_routes = {
#     "task.scrape_task": {"queue": "scrape_queue"},
# }
