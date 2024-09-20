from datetime import date
import pathlib
from typing import Literal, NamedTuple


ColumnMetrics = ["Sum", "Avg", "Min", "Max"]


class Metric(NamedTuple):
    destination_table: str
    destination_db: str
    source_table: str
    source_db: str
    checked_col: str
    date_col: str


class HealthCheckResult(NamedTuple):
    RunDate: date
    Table: str
    Database: str
    Column: str
    MetricType: str
    AbsoluteDifference: int
    PercentDifference: float
    IsHealthy: bool
    Query: str


base_path = pathlib.Path("data/queries/monitoring")
metrics: list[Metric] = [
    Metric("T_PosUsage", "OTMR_BI", "T_PosUsage", "OTMR", "SumAmount", "Date"),
]

column_level_query = """
drop table if exists #source_{method};
drop table if exists #dest_{method};

select {method}(cast({checked_col} as bigint)) as {method} into #dest_{method}
from {destination_db}.dbo.{destination_table}
where [{date_col}] >= Dateadd(day, -14, getdate());

select {method} into #source_{method}
from OPENQUERY(
    [OFSZTMRSQL01],
    'select {method}(cast([{checked_col}] as bigint)) as {method} from {source_db}.dbo.{source_table} where [{date_col}] >= Dateadd(day, -14, getdate())'
);

select (
    (#source_{method}.{method} - #dest_{method}.{method})* 1.0 /
    case
        when #source_{method}.{method} = 0 then 1 
        else #source_{method}.{method} 
    end
    ) * 100 as pct_diff,
    #source_{method}.{method} - #dest_{method}.{method} as diff
from #dest_{method}
cross join #source_{method}
"""

count_query = """
drop table if exists #source_count;
drop table if exists #dest_count;

select count(*) as count into #dest_count
from {destination_db}.dbo.{destination_table}
where [{date_col}] >= Dateadd(day, -14, getdate());

select count into #source_count
from OPENQUERY(
    [OFSZTMRSQL01],
    'select count(*) as count from {source_db}.dbo.{source_table} where [{date_col}] >= Dateadd(day, -14, getdate())'
);

select (
    (#source_count.count - #dest_count.count)* 1.0 /
    case
        when #source_count.count = 0 then 1 
        else #source_count.count 
    end
    ) * 100 as pct_diff,
    #source_count.count - #dest_count.count as diff
from #dest_count
cross join #source_count
"""

merge = """
    merge WORK.dbo.T_HealthCheckResult as target
        USING WORK.dbo.T_HealthCheckResult_Staging as source
        on target.[RunDate] = source.[RunDate]
        and target.[Table] = source.[Table]
        and target.[Database] = source.[Database]
        and target.[Column] = source.[Column]
        and target.[MetricType] = source.[MetricType]
    when not matched by target then 
        insert ([RunDate], [Table], [Database], [Column], [MetricType], [AbsoluteDifference], [PercentDifference], [IsHealthy], [Query])
        values (source.[RunDate], source.[Table], source.[Database], source.[Column], source.[MetricType], source.[AbsoluteDifference], source.[PercentDifference], source.[IsHealthy], source.[Query])
    when matched then 
        update set 
            target.[AbsoluteDifference] = source.[AbsoluteDifference], 
            target.[PercentDifference] = source.[PercentDifference],
            target.[IsHealthy] = source.[IsHealthy],
            target.[Query] = source.[Query];
"""
