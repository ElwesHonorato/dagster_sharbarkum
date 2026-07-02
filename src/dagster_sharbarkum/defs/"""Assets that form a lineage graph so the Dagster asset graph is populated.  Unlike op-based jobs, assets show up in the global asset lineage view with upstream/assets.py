"""Assets that form a lineage graph so the Dagster asset graph is populated.

Unlike op-based jobs, assets show up in the global asset lineage view with
upstream/downstream edges. The dependency graph built below looks like:

    raw_users ---> cleaned_users ---> user_stats ---.
                                                      +--> combined_report
    raw_orders --> cleaned_orders --------------------'

Each asset sleeps so materializations are long enough to observe as running in
the UI. `analytics_job` materializes the whole graph in one run.
"""

import time

import dagster as dg

GROUP = "analytics"


@dg.asset(group_name=GROUP)
def raw_users(context: dg.AssetExecutionContext) -> list[dict]:
    context.log.info("Fetching raw users...")
    time.sleep(20)
    return [{"id": i, "name": f"user_{i}"} for i in range(100)]


@dg.asset(group_name=GROUP)
def raw_orders(context: dg.AssetExecutionContext) -> list[dict]:
    context.log.info("Fetching raw orders...")
    time.sleep(20)
    return [{"id": i, "user_id": i % 100, "amount": i * 1.5} for i in range(500)]


@dg.asset(group_name=GROUP)
def cleaned_users(
    context: dg.AssetExecutionContext, raw_users: list[dict]
) -> list[dict]:
    context.log.info(f"Cleaning {len(raw_users)} users...")
    time.sleep(25)
    return raw_users


@dg.asset(group_name=GROUP)
def cleaned_orders(
    context: dg.AssetExecutionContext, raw_orders: list[dict]
) -> list[dict]:
    context.log.info(f"Cleaning {len(raw_orders)} orders...")
    time.sleep(25)
    return raw_orders


@dg.asset(group_name=GROUP)
def user_stats(
    context: dg.AssetExecutionContext, cleaned_users: list[dict]
) -> dict:
    context.log.info("Computing user stats...")
    time.sleep(20)
    return {"user_count": len(cleaned_users)}


@dg.asset(group_name=GROUP)
def combined_report(
    context: dg.AssetExecutionContext,
    user_stats: dict,
    cleaned_orders: list[dict],
) -> dict:
    context.log.info("Building combined report...")
    time.sleep(30)
    total_amount = sum(o["amount"] for o in cleaned_orders)
    return {
        "user_count": user_stats["user_count"],
        "order_count": len(cleaned_orders),
        "total_amount": total_amount,
    }


analytics_job = dg.define_asset_job(
    name="analytics_job",
    selection=dg.AssetSelection.groups(GROUP),
)
