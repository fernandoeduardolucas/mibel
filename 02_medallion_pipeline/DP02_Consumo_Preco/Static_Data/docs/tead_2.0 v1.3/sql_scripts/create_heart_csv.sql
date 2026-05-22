CREATE TABLE hive.tead.heart_csv (
    age	varchar,
    sex	varchar,
    cp	varchar,
    trtbps varchar,
    chol varchar,
    fbs	varchar,
    restecg	varchar,
    thalachh varchar,
    exng varchar,
    oldpeak	varchar,
    slp	varchar,
    caa	varchar,
    thall varchar,
    result varchar 
)
WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/raw/heart_csv/',
    skip_header_line_count = 1,
    csv_separator = ',',
    csv_quote = '"',
    csv_escape = '"'
);



CREATE TABLE iceberg.tead.heart
WITH (format = 'PARQUET')
AS
SELECT
    try_cast(age as integer) AS age,
    try_cast(sex as integer) AS sex,
    try_cast(cp as integer) AS cp,
    try_cast(trtbps as integer) AS trtbps,
    try_cast(fbs as integer) AS fbs,
    try_cast(restecg as integer) AS restecg,
    try_cast(thalachh as integer) AS thalachh,
    try_cast(exng as integer) AS exng,
    try_cast(oldpeak as double) AS oldpeak,
    try_cast(slp as integer) AS slp,
    try_cast(caa as integer) AS caa,
    try_cast(thall as integer) AS thall,
    try_cast(result as integer) AS result
FROM hive.tead.heart_csv;