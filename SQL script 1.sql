-- Find top 10 songs
SELECT
    TOP 10 *
FROM
    OPENROWSET(
        BULK 'https://vsksyn.dfs.core.windows.net/vsksyn/playlist_data.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) AS [result];


--Find SZA Songs
SELECT *
FROM OPENROWSET(
    BULK 'https://vsksyn.dfs.core.windows.net/vsksyn/playlist_data.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    [name] NVARCHAR(255),
    [artist] NVARCHAR(255),
    [album] NVARCHAR(255),
    [release_date] DATE,
    [duration_ms] BIGINT
) AS [result]
WHERE [artist] = 'SZA';

--Find 2023 Songs
SELECT *
FROM OPENROWSET(
    BULK 'https://vsksyn.dfs.core.windows.net/vsksyn/playlist_data.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    [name] NVARCHAR(255),
    [artist] NVARCHAR(255),
    [album] NVARCHAR(255),
    [release_date] DATE,
    [duration_ms] BIGINT
) AS [result]
WHERE YEAR([release_date]) IN ('2023');


--Find total duration of songs by each artist
SELECT [artist], SUM([duration_ms]) AS TotalDuration
FROM OPENROWSET(
    BULK 'https://vsksyn.dfs.core.windows.net/vsksyn/playlist_data.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    [name] NVARCHAR(255),
    [artist] NVARCHAR(255),
    [album] NVARCHAR(255),
    [release_date] DATE,
    [duration_ms] BIGINT
) AS [result]
GROUP BY [artist]
ORDER BY TotalDuration DESC;
