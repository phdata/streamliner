





USE DATABASE SANDBOX_POC1;

USE SCHEMA EMPLOYEES;

CREATE OR REPLACE PROCEDURE streamliner_recreate_pipe(pipeName string)
RETURNS string
LANGUAGE JAVASCRIPT
AS
$$
try_until_max("Pause pipe", 600, function() {
pause();
var status = pipe_status();
return status && status.executionState === "PAUSED" && status.pendingFileCount === 0;
});

var pipeRedefinition_sql = `CREATE OR REPLACE PIPE Employee_pipe
AUTO_INGEST = true
AS
COPY INTO Employee (ID)
FROM ( SELECT $1:ID::NUMBER(38, 0)
FROM @STREAMLINER_QUICKSTART_1_stage/Employee)
FILE_FORMAT = ( TYPE = PARQUET);`;

snowflake.execute({sqlText: pipeRedefinition_sql});

return JSON.stringify(pipe_status());

function try_until_max(desc, attempts, f) {
for (var i = 0; i < attempts; i++) {
var result = f();
if (result) return result;
sleepFor(1000);
}
throw new Error("Timed out waiting for " + desc);
}
function sleepFor(sleepDuration){
var now = new Date().getTime();
while(new Date().getTime() < now + sleepDuration){ /* Do nothing */ }
}
function pause() {
snowflake.execute({sqlText: "ALTER PIPE " + PIPENAME + " SET PIPE_EXECUTION_PAUSED = TRUE"});
}
function pipe_status() {
var s = snowflake.createStatement( {sqlText: "SELECT SYSTEM$PIPE_STATUS('" + PIPENAME + "');"} );
var rs = s.execute();
if (rs.next()) {
var status = rs.getColumnValue(1);
if (status) {
return JSON.parse(status);
}
}
return null;
}
$$
;

CALL streamliner_recreate_pipe('Employee_pipe');

DROP PROCEDURE streamliner_recreate_pipe(string);