# Streamliner Quickstart

Streamliner is a Data Pipeline Automation tool that simplifies the process of ingesting data onto a new platform. It is not a data ingestion tool in and of itself; rather, it automates other commonly used tools that ingest and manipulate data on platforms like Snowflake, Amazon Redshift, Cloudera, and Databricks.

This Streamliner Quickstart will walk you through a real-world example of using Streamliner to automate a MySQL to Snowflake
incremental ingestion pipeline. The goal of this quickstart is to give you real-world streamliner experience with as 
little effort as possible. Therefore, there are a few places you won't follow best practices. Specifically:

1. You will create a MySQL instance and open it to the public internet. You'll take some precautions to ensure it's not exploited but in the real world you'd create this database in a less accessible environment.
2. You will create AWS infrastructure manually in the AWS console. In the real world you would use phData's [Infrastructure-as-code library Cloud Foundation](https://docs.customer.phdata.io/docs/cloudfoundation/).
3. You will create an AWS IAM service account as opposed to configuring a storage integration.

## Create Snowflake Account

This quickstart was executed on a [Snowflake trial](https://trial.snowflake.com/) account. This will be easier than using
an account created by your employer since you will have full administrative rights.

## Create AWS Account (If Required)

You will need access to an AWS account where you can start EC2 instances, start RDS instances, create IAM Roles, and 
create S3 buckets. This might be possible in your employers "dev" account, but you can also create your own account. 
The costs won't be more than few dollars as long as you shutdown the infrastructure when done. If you sign up for an AWS Free-Tier account, you should be able to complete the quickstart without incurring charges, assuming you shut down all the infrastructure when you're done.

## Create Sample Source Database

Now you need to create a test mysql instance and loading data. AWS has a great 
[Getting Started](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_GettingStarted.CreatingConnecting.MySQL.html) 
guide on how to create a simple and small MySQL instance. Please follow that guide making note of the `HOSTNAME` and
admin `PASSWORD`.

Even though the intent of this quickstart is to give a real-world streamliner experience with very little effot, you are going to open the mysql database to the world, which you should never really do. To mitigate the security risk, the database will have a strong password, be on an alternate port, and only be around for a few hours.

Therefore, after you have created your mysql instance using the instructions above, modify the instance to make it
publicly accessible:

![Modify RDS Instance to be Publicly Accessible](../images/quickstart-mysql-config-public-access.png)

MySQL typically uses port `3306` but you should change that to port `33006` to reduce your attack surface:

![Modify RDS Instance to use port 33006](../images/quickstart-mysql-config-port-33006.png)

Now you need to open port `33006` to the public internet:

![Modify Security group open port 33006](../images/quickstart-mysql-config-firewall.png)

Now you need to load some sample data. You can load a well-known MySQL Test Database with fake employee data:

```shell script
git clone https://github.com/datacharmer/test_db.git
cd test_db
mysql -h <REPLACE_WITH_HOSTNAME> -P33006 -u admin -p"<REPLACE_WITH_PASSWORD>"
source employees.sql;
```

## Create Snowflake Databases

In this example there was a [sandbox workspace created by Tram](https://docs.customer.phdata.io/docs/tram/) 
called `SANDBOX_POC1` with a warehouse created for that workspace called `SANDBOX_POC1`. When you set up your Snowflake environment, you can use whichever warehouse and database you have.

## Create Landing Location

Create an S3 bucket:

![Create AWS S3 Bucket](../images/quickstart-ingest-create-bucket.png)

## Create Landing Location policy

Both Snowflake and AWS DMS will need to be able to read from and write to the bucket. Therefore, click IAM and create a policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion"
            ],
            "Resource": "arn:aws:s3:::REPLACE_WITH_BUCKET_NAME/*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::REPLACE_WITH_BUCKET_NAME"
        }
    ]
}
```



## Create AWS DMS Ingest

Create Replication Instance:

![Create Replication Instance](../images/quickstart-repl-01-create-instance.png)

Wait for it to be available:

![Wait for it to be available](../images/quickstart-repl-02-instance-created.png)

Create source endpoint:

![Create source endpoint](../images/quickstart-repl-03-create-source-endpoint.png)

Open `IAM` in the AWS console and then create a Role. Select `DMS` as the trusted entity:

![Create role trusted entity](../images/quickstart-repl-04-create-role-trusted-entity.png)

In this quickstart example, a policy called,  `policy-streamliner-quickstart-1`, was created in the earlier step.

![Create role policy](../images/quickstart-repl-05-create-role-policy.png)

Give the role a name:

![Create role name](../images/quickstart-repl-06-create-role-name.png)

Copy the ARN:

![Create role ARN](../images/quickstart-repl-07-create-role-arn.png)

Back in DMS, create the target endpoint where you will need the ARN from the previous step:

![Create target endpoint](../images/quickstart-repl-08-create-target-endpoint.png)

Add `dataFormat=parquet;` to the extra connection attributes:

![Add parquet](../images/quickstart-repl-09-create-target-endpoint.png)

Test both endpoints:

![Test endpoint](../images/quickstart-repl-10-test-endpoint.png)

Verify the test results are _successful_:

![Test endpoint result](../images/quickstart-repl-11-test-endpoint-result.png)

Create a task:

![Create task](../images/quickstart-repl-12-create-task.png)

Make sure to select enable Cloudwatch logs or if anything goes wrong you will have no idea what happened:

![cloudwatch logs](../images/quickstart-repl-13-create-task.png)

Click `Add new selection rule` and under `Schema` select `Enter Schema` which populates wildcard values for schema and table.

![Add new selection rule](../images/quickstart-repl-14-create-task.png)

After the task is created and successfully running, change back to S3 and you should see the data:

![refresh s3](../images/quickstart-repl-15-refresh-s3.png)

S3 is now populated with the data from the MySQL database.

## Configure Storage Integration

Next you will configure a Storage Integration which is a secure way of delegating access to Snowflake. Follow the 
Snowflake guide on [Option 1: Configuring a Snowflake Storage Integration](https://docs.snowflake.com/en/user-guide/data-load-s3-config.html#option-1-configuring-a-snowflake-storage-integration).

Please note policy in the above document is more complicated than is required. In this quickstart, the policy created earlier was used (`policy-streamliner-quickstart-1`), but you can use the Snowflake example as well. _Note you can skip, "Step 6: Create an External Stage" which isn't required for this use case._


Here is an example of the storage integration creation SQL used:

```sql
CREATE STORAGE INTEGRATION STREAMLINER_QUICKSTART_1
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<REPLACE_WITH_YOUR_ACCOUNT>:role/role-snowflake-streamliner-quickstart-1'
  STORAGE_ALLOWED_LOCATIONS = ('s3://streamliner-quickstart-1/')
```

You also need to grant your user access to `EXECUTE TASKS` and on the `INTEGRATION` created above. Assuming you have a free
Snowflake account, the default role for your user is `sysadmin` and the following grants will suffice:

```sql
GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;
GRANT ALL ON INTEGRATION STREAMLINER_QUICKSTART_1 TO ROLE sysadmin;
```


## Download

Find the [latest version](curl -O https://repository.phdata.io/artifactory/list/binary/phdata/streamliner/) and then download:

```shell
curl -O curl -O https://repository.phdata.io/artifactory/list/binary/phdata/streamliner/streamliner-<VERSION>.zip
```

and unzip:

```shell
unzip streamliner-<VERSION>.zip
cd streamliner-<VERSION>
```

## Configure snowsql

You will run streamliner from your desktop. So the first step is [install snowsql](https://docs.snowflake.com/en/user-guide/snowsql-install-config.html), if required.

Then, configure snowsql to have a *named connection* called `streamliner_quickstart`:

Open `~/.snowsql/config` and add a config similar to the following snippet:

```
[connections.streamliner_quickstart]
accountname = <REPLACE_WITH_YOUR_SF_ACCOUNT_ID>.us-east-1.aws # e.g.: pla99474
username = <REPLACE_WITH_YOUR_SF_USER> # e.g.: brock
password = <REPLACE_WITH_YOUR_SF_PASSWORD>
```

Verify that snowsql can use your configuration settings to connect to your snowflake account by trying to open a snowsql prompt: `snowsql -c streamliner_quickstart`

If you're able to connect to snowflake, then exit the prompt with `!quit`.

## Configure Streamliner

Create the streamliner config file, eg `conf/private-ingest-configuration.yml`:

```yaml
name: "STREAMLINER_QUICKSTART_1"
environment: "SANDBOX"
pipeline: "snowflake-snowpipe-append" # Pipeline name must match a directory in the the provided template folder
source:
  type: Jdbc
  url: "jdbc:mysql://<REPLACE_WITH_YOUR_FULL_MYSQL_HOSTNAME>:33006?characterEncoding=latin1"
  username: "<YOUR MYSQL ADMIN USER>" # e.g.: "admin"
  passwordFile: "" # empty for snowflake
  schema: "employees"
  tableTypes:
    - table
destination:
  type: Snowflake
  snowSqlCommand: snowsql -c streamliner_quickstart
  storagePath: "<YOUR S3 BUCKET>/employees" # e.g.: "s3://streamliner-quickstart-1/employees/"
  storageIntegration: "<YOUR STORAGE INTEGRATION NAME>" # e.g.: "STREAMLINER_QUICKSTART_1"
  warehouse: "<YOUR SNOWFLAKE WAREHOUSE>" # e.g.: "SANDBOX_POC1"
  taskSchedule: "" # Unused for this use case
  stagingDatabase:
    name: <YOUR SNOWFLAKE STAGING DATABASE> # e.g.: SANDBOX_POC1
    schema: EMPLOYEES
  reportingDatabase:
    name: "" # Unused for this use case
    schema: ""
```

## Troubleshooting Steps

As you proceed to the next steps, keep the following troubleshooting steps in mind.

1. If an error happens, delete output and regenerate scripts.
2. The output of `schema` command is used as input to subsequent `scripts` command. Therefore, if you change the input 
config file, you need to delete the output and run the `schema` command before seeing the changes in the `scripts` output.
3. Note that there are two role contexts on the Snowflake UI. The one in the upper right is for the page and
the one in the query page is for the query itself. This can cause issues if you create objects with the `ACCOUNTADMIN` role, for example.

![Snowflake roles troubleshooting](../images/quickstart-troubleshoot-roles.png)

## Extract Schema

The command below requires the file `password` be populated with the database password so create this file with `vi` or 
another editor and populate it with the password for the database. _When the command is run, the value passed into `--database-password` is actually reading the contents of the `password` file as input for the argument._

```shell script
mkdir -p output/STREAMLINER_QUICKSTART_1
./bin/streamliner schema --config conf/private-ingest-configuration.yml --output-path output/STREAMLINER_QUICKSTART_1/ --database-password "`<password`"
```

## Generate Scripts

```shell script
./bin/streamliner scripts --config output/STREAMLINER_QUICKSTART_1/conf/streamliner-configuration.yml --template-directory templates/snowflake --type-mapping conf/type-mapping.yml
```

## Run Scripts

Navigate to the output directory for the generated scripts (e.g.: `output/STREAMLINER_QUICKSTART_1/SANDBOX/scripts`).

First, test everything with just the `departments` table:

```shell script
make first-run-departments
/Applications/Xcode.app/Contents/Developer/usr/bin/make first-run -C departments
/Applications/Xcode.app/Contents/Developer/usr/bin/make create-staging-schema
snowsql -c streamliner_quickstart -f create-staging-schema.sql
* SnowSQL * v1.2.10
Type SQL statements or !help
+--------------------------------------------+                                  
| status                                     |
|--------------------------------------------|
| Schema EMPLOYEES successfully created. |
+--------------------------------------------+
1 Row(s) produced. Time Elapsed: 0.209s
Goodbye!             
...
```

In order to enable "auto ingest" where files are automatically ingested when placed in S3, you need to do some additional work.

First in the Snowflake console, run the following sql to get the name of the SQS topic required:

```sql
USE DATABASE SANDBOX_POC1;

USE SCHEMA EMPLOYEES;

desc pipe departments_pipe;
```

And you will see something like this:

![Obtain ARN](../images/quickstart-auto-ingest-00.png)

Then open the AWS console, find your bucket, click events: 

![Open S3 Bucket Properties](../images/quickstart-auto-ingest-01.png)

And add a notification to the SQS queue:

![Add Event Notification](../images/quickstart-auto-ingest-02.png)

You can clean up the stages, tables, and tasks for the `departments` table with:

```shell script
make clean-departments
```

Once that the above is working, you can execute all tables with `first-run-all`:

```shell script
make first-run-all
```
## See Result

You can now go preview data in the staging tables:

![Preview Data](../images/quickstart-preview-data.png)
