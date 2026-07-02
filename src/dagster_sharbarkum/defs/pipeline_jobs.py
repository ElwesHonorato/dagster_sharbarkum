"""A single op-based job with several sequential steps.

Each step sleeps, so in the UI you can watch the run progress step-by-step in
the run timeline / Gantt view and see how a longer-running job is represented.
Data is passed op-to-op, producing an in-job op dependency graph.
"""

import time

import dagster as dg


@dg.op
def extract(context: dg.OpExecutionContext) -> list[int]:
    context.log.info("Extracting data...")
    time.sleep(20)
    return list(range(10))


@dg.op
def transform(context: dg.OpExecutionContext, records: list[int]) -> list[int]:
    context.log.info(f"Transforming {len(records)} records...")
    time.sleep(30)
    return [r * 2 for r in records]


@dg.op
def validate(context: dg.OpExecutionContext, records: list[int]) -> list[int]:
    context.log.info(f"Validating {len(records)} records...")
    time.sleep(15)
    return records


@dg.op
def load(context: dg.OpExecutionContext, records: list[int]) -> None:
    context.log.info(f"Loading {len(records)} records; sum={sum(records)}.")
    time.sleep(20)


@dg.job
def etl_pipeline_job():
    load(validate(transform(extract())))
