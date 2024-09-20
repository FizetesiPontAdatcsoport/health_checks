from datetime import date
import pyodbc
import logging

from health_checks import checks
from itertools import product

logging.basicConfig(level=logging.INFO)


insert = """INSERT INTO T_HealthCheckResult_Staging ([RunDate], [Table], [Database], [Column], [MetricType], [AbsoluteDifference], [PercentDifference], [IsHealthy], [Query])
Values (?, ?, ?, ?, ?, ?, ?, ?, ?);"""


def check_column_level_metrics(
    metrics: list[checks.Metric], cur: pyodbc.Cursor
) -> list[checks.HealthCheckResult]:
    results = []
    for metric, mtype in product(metrics, checks.ColumnMetrics):
        script = checks.column_level_query.format(method=mtype, **metric._asdict())
        cur.execute(script)
        while not cur.description:
            cur.nextset()
            pass
        metric_result = cur.fetchone()

        if not metric_result:
            print("metric is invalid.")
            continue

        if len(metric_result) != 2:
            print("metric is invalid.")
            continue

        percent_difference, absolute_difference = metric_result

        is_healthy = absolute_difference == 0
        result = checks.HealthCheckResult(
            RunDate=date.today(),
            Table=metric.destination_table,
            Database=metric.destination_db,
            Column=metric.checked_col,
            MetricType=mtype,
            AbsoluteDifference=absolute_difference,
            PercentDifference=percent_difference,
            IsHealthy=is_healthy,
            Query=script,
        )

        results.append(result)
    return results


def check_table_level_metrics(
    tables: list[checks.Metric], cur: pyodbc.Cursor
) -> list[checks.HealthCheckResult]:
    results = []
    for metric in tables:
        script = checks.count_query.format(method="Count", **metric._asdict())
        cur.execute(script)
        while not cur.description:
            cur.nextset()
            pass
        metric_result = cur.fetchone()

        if not metric_result:
            print("metric is invalid.")
            continue

        if len(metric_result) != 2:
            print("metric is invalid.")
            continue

        percent_difference, absolute_difference = metric_result

        is_healthy = absolute_difference == 0
        result = checks.HealthCheckResult(
            RunDate=date.today(),
            Table=metric.destination_table,
            Database=metric.destination_db,
            Column=metric.checked_col,
            MetricType="Count",
            AbsoluteDifference=absolute_difference,
            PercentDifference=percent_difference,
            IsHealthy=is_healthy,
            Query=script,
        )

        results.append(result)
    return results


def main() -> None:
    server, database = "OFSZDWH01", "WORK"
    conn = pyodbc.connect(f"""
        DRIVER=ODBC Driver 17 for SQL Server;
        SERVER={server};
        DATABASE={database};
        TRUSTED_CONNECTION=YES;
    """)

    print("Successfully connected to db.")

    results = []
    with conn.cursor() as cur:
        results = check_table_level_metrics(
            checks.metrics, cur
        ) + check_column_level_metrics(checks.metrics, cur)

        cur.execute("TRUNCATE TABLE WORK.dbo.T_HealthCheckResult_Staging;")
        cur.executemany(insert, results)
        cur.commit()

        cur.execute(checks.merge)
    print("done")


if __name__ == "__main__":
    main()
