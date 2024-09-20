CREATE TABLE [WORK].[dbo].[T_HealthCheckResult] (
    [RunDate]            DATE           NOT NULL,
    [Table]              VARCHAR (30)   NOT NULL,
    [Database]           VARCHAR (30)   NOT NULL,
    [MetricType]         VARCHAR (30)   NOT NULL,
    [AbsoluteDifference] INT            NOT NULL,
    [PercentDifference]  DECIMAL (5, 3) NOT NULL,
    [IsHealthy]          BIT            NOT NULL,
    [Query]              VARCHAR (1000) NOT NULL,
    [Column]             VARCHAR (30)   NOT NULL,
    CONSTRAINT [PK_T_HealthCheckResult_RunDate_Table_Database_MetricType_IsHealthy_Column] PRIMARY KEY CLUSTERED ([RunDate] ASC, [Table] ASC, [Database] ASC, [MetricType] ASC, [Column] ASC, [IsHealthy] ASC, [Column] ASC)
);