"""A three-job chain to test Dagster+ Serverless.

Each job runs after the previous one finishes successfully, via run status
sensors:

    upstream_job -> downstream_job -> final_job

Sleeps are added so the runs take long enough to observe as "started"/
"running" in the UI before they complete. This is an ordering dependency
only: no data is passed between the jobs (see assets.py for data lineage).
"""

import time

import dagster as dg

# Seconds each op sleeps so runs are visibly long-running in the UI.
SLEEP_SECONDS = 30


@dg.op
def say_hello(context: dg.OpExecutionContext) -> str:
    context.log.info(f"Upstream op running; sleeping {SLEEP_SECONDS}s.")
    time.sleep(SLEEP_SECONDS)
    return "hello from upstream"


@dg.op
def print_message(context: dg.OpExecutionContext) -> None:
    context.log.info(f"Downstream op running; sleeping {SLEEP_SECONDS}s.")
    time.sleep(SLEEP_SECONDS)


@dg.op
def wrap_up(context: dg.OpExecutionContext) -> None:
    context.log.info(f"Final op running; sleeping {SLEEP_SECONDS}s.")
    time.sleep(SLEEP_SECONDS)


@dg.job
def upstream_job():
    say_hello()


@dg.job
def downstream_job():
    print_message()


@dg.job
def final_job():
    wrap_up()


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[upstream_job],
    request_job=downstream_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def run_downstream_after_upstream(context: dg.RunStatusSensorContext):
    context.log.info(
        f"{context.dagster_run.job_name} succeeded; launching downstream_job."
    )
    return dg.RunRequest(run_key=None)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[downstream_job],
    request_job=final_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def run_final_after_downstream(context: dg.RunStatusSensorContext):
    context.log.info(
        f"{context.dagster_run.job_name} succeeded; launching final_job."
    )
    return dg.RunRequest(run_key=None)
