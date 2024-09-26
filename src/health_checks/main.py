import asyncio
from email.message import EmailMessage
import logging
from pathlib import Path
import smtplib
import ssl
import time
import aioodbc
import pyodbc
import polars as pl
from datetime import date, datetime, timedelta
import tomllib
from typing import NamedTuple, Optional

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure file handler for warnings
file_handler = logging.FileHandler("warnings.log")
file_handler.setLevel(logging.WARNING)
logger.addHandler(file_handler)

# Configure stream handler for debug and above
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

# Email configuration from environment variables
_SMTP_SERVER = "smtp-mail.outlook.com"
_SMTP_PORT = 587
_SMTP_USER = "eros.daniel@fizetesipont.hu"
_SMTP_PASSWORD = "FreshPrinceOfPersia3@"


class TableConfig(NamedTuple):
    date_col: str
    sum_col: str
    source_table: str
    destination_table: str
    num_days: int | None = None


class DatabaseConfig(NamedTuple):
    linked_server: str
    source_type: str
    tables: dict[str, TableConfig]


class MonitoringResult(NamedTuple):
    data_source: str
    value: int


class ResultTableRow(NamedTuple):
    RunDate: date
    Table: str
    Database: str
    MetricType: str
    AbsoluteDifference: int
    PercentDifference: float
    IsHealthy: bool
    Query: str
    Column: str
    ErrorMessage: Optional[str]


async def execute_query(
    pool: aioodbc.Pool, query: str
) -> pl.DataFrame | pyodbc.ProgrammingError:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(query)
                rows = [
                    MonitoringResult(row[0], row[1]) for row in await cur.fetchall()
                ]
                return pl.DataFrame(rows)
            except pyodbc.ProgrammingError as e:
                return e


def generate_count_query(
    db_config: DatabaseConfig, table_config: TableConfig, default_days: int = 14
) -> str:
    current_date = datetime.now().date()
    days = table_config.num_days or default_days
    threshold_date = (current_date - timedelta(days=days)).isoformat()

    if db_config.source_type == "sqlserver":
        source_query = f"""
        SELECT 'Source' AS DataSource, * FROM (
            SELECT * FROM OPENQUERY([{db_config.linked_server}],
            '
            SELECT COUNT(*) AS value
            FROM {table_config.source_table} WITH (NOLOCK)
            WHERE [{table_config.date_col}] >= ''{threshold_date}''
            OPTION (RECOMPILE)
            ')
        ) AS SourceData
        """
    else:  # Unknown db
        source_query = f"""
        SELECT 'Source' AS DataSource, * FROM (
            SELECT * FROM OPENQUERY([{db_config.linked_server}], '
                SELECT COUNT(*) AS value
                FROM {table_config.source_table}
                WHERE "{table_config.date_col}" >= ''{threshold_date}''
            ')
        ) AS SourceData
        """

    dest_query = f"""
    SELECT 'Destination' AS DataSource,
        COUNT(*) AS value
    FROM {table_config.destination_table} WITH (NOLOCK)
    WHERE [{table_config.date_col}] >= '{threshold_date}'
    OPTION (RECOMPILE)
    """

    return f"{source_query}\nUNION ALL\n{dest_query}"


def generate_sum_query(
    db_config: DatabaseConfig, table_config: TableConfig, days: int = 7
) -> str:
    current_date = datetime.now().date()
    threshold_date = (current_date - timedelta(days=days)).isoformat()

    if db_config.source_type == "sqlserver":
        source_query = f"""
        SELECT 'Source' AS DataSource, * FROM (
            SELECT * FROM OPENQUERY([{db_config.linked_server}],
            '
            SELECT SUM(cast([{table_config.sum_col}] as bigint)) AS value
            FROM {table_config.source_table}
            WHERE [{table_config.date_col}] >= ''{threshold_date}''
            ')
        ) AS SourceData
        """
    else:  # Unknown db
        source_query = f"""
        SELECT 'Source' AS DataSource, * FROM (
            SELECT * FROM OPENQUERY([{db_config.linked_server}], '
                SELECT SUM(cast("{table_config.sum_col}" as bigint)) AS value
                FROM {table_config.source_table}
                WHERE "{table_config.date_col}" >= ''{threshold_date}''
            ')
        ) AS SourceData
        """

    dest_query = f"""
    SELECT 'Destination' AS DataSource,
        SUM(cast([{table_config.sum_col}] as bigint)) AS value
    FROM {table_config.destination_table} WITH (NOLOCK)
    WHERE [{table_config.date_col}] >= '{threshold_date}'
    OPTION (RECOMPILE)
    """

    return f"{source_query}\nUNION ALL\n{dest_query}"


async def perform_single_health_check(
    pool: aioodbc.Pool,
    db_name: str,
    db_config: DatabaseConfig,
    table_name: str,
    table_config: TableConfig,
) -> list[ResultTableRow]:
    print(f"Performing health check for {table_name}")
    count_query = generate_count_query(db_config, table_config)
    sum_query = generate_sum_query(db_config, table_config)
    results = []
    current_date = datetime.now().date()

    async def process_query(
        query: str, metric_type: str, column: str, current_date: date
    ) -> ResultTableRow:
        match await execute_query(pool, query):
            case pl.DataFrame() as df:
                source_data = df.filter(pl.col("data_source") == "Source")
                dest_data = df.filter(pl.col("data_source") == "Destination")

                source_val = source_data["value"][0]
                dest_val = dest_data["value"][0]
                abs_diff = source_val - dest_val
                percent_diff = (abs_diff * 100 / source_val) if source_val != 0 else 0

                is_healthy = abs(percent_diff) < 1  # Less than 1% difference

                return ResultTableRow(
                    RunDate=current_date,
                    Table=table_name,
                    Database=db_name,
                    MetricType=metric_type,
                    AbsoluteDifference=int(abs_diff),
                    PercentDifference=round(percent_diff, 3),
                    IsHealthy=is_healthy,
                    Query=query,
                    Column=column,
                    ErrorMessage=None,
                )
            case pyodbc.ProgrammingError() as e:
                return ResultTableRow(
                    RunDate=current_date,
                    Table=table_name,
                    Database=db_name,
                    MetricType=metric_type,
                    AbsoluteDifference=-1,
                    PercentDifference=-1,
                    IsHealthy=False,
                    Query=query,
                    Column=column,
                    ErrorMessage=str(e),
                )

    count_task = process_query(count_query, "RowCount", "N/A", current_date)
    sum_task = process_query(sum_query, "SumAmount", table_config.sum_col, current_date)

    results = list(await asyncio.gather(count_task, sum_task))

    return results


async def save_results_staging(
    pool: aioodbc.Pool, results: list[ResultTableRow]
) -> None:
    insert_query = """
    INSERT INTO [dbo].[T_HealthCheckResult_Staging]
    ([RunDate], [Table], [Database], [MetricType], [AbsoluteDifference], [PercentDifference], [IsHealthy], [Query], [Column], [ErrorMessage])
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(insert_query, results)
            await conn.commit()


async def merge_results(pool: aioodbc.Pool) -> None:
    merge_query = """
    MERGE INTO [dbo].[T_HealthCheckResult] AS target
    USING [dbo].[T_HealthCheckResult_Staging] AS source
    ON (target.RunDate = source.RunDate 
        AND target.[Table] = source.[Table]
        AND target.[Database] = source.[Database]
        AND target.MetricType = source.MetricType
        AND target.[Column] = source.[Column])
    WHEN MATCHED THEN
        UPDATE SET
            AbsoluteDifference = source.AbsoluteDifference,
            PercentDifference = source.PercentDifference,
            IsHealthy = source.IsHealthy,
            Query = source.Query,
            ErrorMessage = source.ErrorMessage
    WHEN NOT MATCHED THEN
        INSERT ([RunDate], [Table], [Database], [MetricType], [AbsoluteDifference], [PercentDifference], [IsHealthy], [Query], [Column], [ErrorMessage])
        VALUES (source.RunDate, source.[Table], source.[Database], source.MetricType, source.AbsoluteDifference, source.PercentDifference, source.IsHealthy, source.Query, source.[Column], source.[ErrorMessage]);

    TRUNCATE TABLE [dbo].[T_HealthCheckResult_Staging];
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(merge_query)
            await conn.commit()


async def rerun_failed_checks(
    pool: aioodbc.Pool, config: dict[str, DatabaseConfig]
) -> None:
    today = datetime.now().date()
    failed_checks_query = f"""
    SELECT DISTINCT [Table], [Database], [MetricType], [Column]
    FROM [dbo].[T_HealthCheckResult]
    WHERE RunDate = '{today}' AND IsHealthy = 0
    """
    failed_checks = await execute_query(pool, failed_checks_query)

    assert isinstance(failed_checks, pl.DataFrame)
    tasks = []
    for row in failed_checks.iter_rows(named=True):
        db_config = config[row["Database"]]
        table_config = db_config.tables[row["Table"]]
        tasks.append(
            perform_single_health_check(
                pool, row["Database"], db_config, row["Table"], table_config
            )
        )

    results = await asyncio.gather(*tasks)
    results = [item for sublist in results for item in sublist]

    await save_results_staging(pool, results)
    await merge_results(pool)


async def perform_health_check(
    pool: aioodbc.Pool, config: dict[str, DatabaseConfig]
) -> list[ResultTableRow]:
    tasks = []
    for db_name, db_config in config.items():
        for table_name, table_config in db_config.tables.items():
            tasks.append(
                perform_single_health_check(
                    pool, db_name, db_config, table_name, table_config
                )
            )

    results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]


def load_config(file_path: Path) -> dict[str, DatabaseConfig]:
    with open(file_path, "rb") as f:
        config = tomllib.load(f)
        return {
            db_name: DatabaseConfig(
                linked_server=db_config.pop("linked_server"),
                source_type=db_config.pop("source_type"),
                tables={
                    table_name: TableConfig(**table_config)
                    for table_name, table_config in db_config.items()
                },
            )
            for db_name, db_config in config.items()
        }


def send_email(subject: str, body: str, recipients: list[str]) -> None:
    message = EmailMessage()
    message["From"] = _SMTP_USER
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject

    message.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP(_SMTP_SERVER, _SMTP_PORT) as email_server:
        email_server.starttls(context=context)
        email_server.login(_SMTP_USER, _SMTP_PASSWORD)

        email_server.send_message(message)


async def main() -> None:
    config = load_config(Path("data/config.toml"))
    server, database = "OFSZDWH01", "WORK"
    dsn = f"""
        DRIVER=ODBC Driver 17 for SQL Server;
        SERVER={server};
        DATABASE={database};
        TRUSTED_CONNECTION=YES;
    """

    start = time.time()
    pool = await aioodbc.create_pool(dsn=dsn, minsize=1, maxsize=20)

    try:
        results = await perform_health_check(pool, config)
        await save_results_staging(pool, results)
        await merge_results(pool)

        failing_checks = [r for r in results if not r.IsHealthy]

        if failing_checks:
            subject = "Health Check Alert: Failing Checks Detected"
            body = "The following health checks have failed:\n\n"
            for check in failing_checks:
                body += f"Database: {check.Database} Table: {check.Table}, Metric: {check.MetricType} \n"

            recipients = [
                "eros.daniel@fizetesipont.hu",
                "hunyadi.valter@fizetesipont.hu",
            ]
            send_email(subject, body, recipients)
    finally:
        pool.close()

    end = time.time()
    delta = end - start
    print(f"Health check run took {delta:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
