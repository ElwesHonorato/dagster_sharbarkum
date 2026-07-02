"""Two minimal op-based jobs chained together to test Dagster+ Serverless.

`downstream_job` is launched automatically whenever `upstream_job` finishes
successfully, via a run status sensor. This is an ordering dependency only:
no data is passed between the jobs.
"""

import dagster as dg


@dg.op
def say_hello(context: dg.OpExecutionContext) -> str:
    context.log.info("Upstream op running.")
    return "hello from upstream"


@dg.op
def print_message(context: dg.OpExecutionContext) -> None:
    context.log.info("Downstream op running.")


@dg.job
def upstream_job():
    say_hello()


@dg.job
def downstream_job():
    print_message()


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
