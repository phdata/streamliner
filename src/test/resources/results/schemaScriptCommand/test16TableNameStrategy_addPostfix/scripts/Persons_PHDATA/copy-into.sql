


USE DATABASE SANDBOX_POC1;

USE SCHEMA EMPLOYEES;

COPY INTO Persons_PHDATA (PersonID,LastName,FirstName,Address,City,Age)
FROM ( SELECT $1:PersonID::int,
$1:LastName::VARCHAR(255),
$1:FirstName::VARCHAR(255),
$1:Address::VARCHAR(255),
$1:City::VARCHAR(255),
$1:Age::VARCHAR(40)
FROM @STREAMLINER_QUICKSTART_1_stage/Persons)
FILE_FORMAT = ( TYPE = PARQUET);