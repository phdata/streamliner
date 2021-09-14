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
USE ROLE ACCOUNTADMIN;
```

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

Find the [latest version](https://repo.phdata.io/public/streamliner/raw/) and then download:

```shell
curl -O https://repo.phdata.io/public/streamliner/raw/versions/<VERSION>/streamliner-<VERSION>.zip
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
./bin/streamliner schema --config conf/private-ingest-configuration.yml --state-directory output/state-directory --database-password <password>
```

## Generate Scripts

```shell script
./bin/streamliner scripts --config conf/private-ingest-configuration.yml --state-directory output/state-directory --previous-state-directory output/previous-state-directory --template-directory templates/snowflake --type-mapping conf/type-mapping.yml --output-path output/STREAMLINER_QUICKSTART_1/SANDBOX/scripts
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

First in the Snowflake console, run the following sql to get the name of the SQS queue arn required:

```sql
USE DATABASE SANDBOX_POC1;

USE SCHEMA EMPLOYEES;

desc pipe departments_pipe;
```

And you will see something like this:

![Obtain ARN](../images/quickstart-auto-ingest-00.png)

Make a note of the SQS Queue ARN to use it later.

Next, you can configure Amazon Simple Notification Service (SNS) as a broadcaster to publish S3 event notifications to multiple subscribers (e.g. SQS queues or AWS Lambda ). In this case, the Snowflake SQS queue for Snowpipe automation through SNS Topic. 

```
S3 Event -> SNS (SNS Topic) -> SQS Queue -> Snowpipe
```
![SNS-SQS-Snowflake](../images/quickstart_sns_topic_filtering_sqs_lamda_endpoint.png)

[Image Source: Amazon Topic Based filtering](https://aws.amazon.com/blogs/compute/simplify-pubsub-messaging-with-amazon-sns-message-filtering/)

The following steps are involved for using SNS:
- Create an SNS topic (to receive notifications sent from your S3 bucket) and Subscribe to S3 bucket.
- Create an SQS queue for Snowflake; and subscribe the Snowflake SQS Queue to the SNS Topic.
- And, Configure S3 bucket to publish events to the SNS topic.

## Create an Amazon SNS Topic and Subscription

- Open AWS Management Console, choose Simple Notification Service (SNS)
- Create an SNS Topic (to handle all messages for the Snowflake stage location on your S3 bucket).
  - Name the topic eg. sf_snowpipe_sns_topic_test
  - This will generate a SNS ARN like arn:aws:sns:us-west-1:YOUR_AWS_ACCOUNT_ID:sf-snowpipe_sns_topic_test

- Next, subscribe the target destinations (SQS queues or AWS Lambda workloads) for the S3 event notifications to this topic. SNS publishes event notifications from S3 bucket to all subscribers to the topic.

The SNS ARN will be used in generating an IAM policy in the Snowflake UI that can listen to the topic.

## Create SQS Queue and subscribe the Snowflake SQS queue to the SNS Topic

- Open AWS Console, Click on SQS and Select the existing SQS queue.
- And, subscribe SQS Queue to the SNS Topic created.

![Subscribe-SQS-Queue-to-SNS-Topic](../images/quickstart-sqs-queue-subscribe-sns-topic.png)

Using a Snowflake client, query the SYSTEM$GET_AWS_SNS_IAM_POLICY system function with your SNS topic ARN

```sql
SELECT SYSTEM$GET_AWS_SNS_IAM_POLICY('arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:sf_snowpipe_sns_topic_test);
```
The function returns an IAM policy that grants a Snowflake SQS queue permission to subscribe to the SNS topic as shown in example below:

![snowflake-iam-policy-sns-topic](../images/quickstart-snowflake-iam-policy-sqs-sns-topic.png)

Eg: Please find a boilerplate policy document which can be edited to include required privileges as below:

```json
{ 
   "Version":"2008-10-17",
   "Id":"__default_policy_ID",
   "Statement":[ 
      { 
         "Sid":"__default_statement_ID",
         "Effect":"Allow",
         "Principal":{ 
            "AWS":"*"
         },
         "Action":[ 
            "SNS:GetTopicAttributes",
            "SNS:SetTopicAttributes",
            "SNS:AddPermission",
            "SNS:RemovePermission",
            "SNS:DeleteTopic",
            "SNS:Subscribe",
            "SNS:ListSubscriptionsByTopic",
            "SNS:Publish",
            "SNS:Receive"
         ],
         "Resource":"arn:aws:sns:us-west-1:YOUR_AWS_ACCOUNT_ID:sf-snowpipe_sns_topic_test",
         "Condition":{ 
            "StringEquals":{ 
               "AWS:SourceOwner":"YOUR_AWS_ACCOUNT_ID"
            }
         }
      },
      { 
         "Sid":"1",
         "Effect":"Allow",
         "Principal":{ 
            "AWS":"arn:aws:iam::YOUR_AWS_ACCOUNT_ID:user/REPLACE_WITH_YOUR_USER_ID"
         },
         "Action":[ 
            "sns:Subscribe"
         ],
         "Resource":[ 
            "arn:aws:sns:us-west-1:YOUR_AWS_ACCOUNT_ID:sf-snowpipe_sns_topic_test"
         ]
      },
      { 
         "Sid":"s3-event-notifier",
         "Effect":"Allow",
         "Principal":{ 
            "Service":"s3.amazonaws.com"
         },
         "Action":"SNS:Publish",
         "Resource":"arn:aws:sns:us-west-1:YOUR_AWS_ACCOUNT_ID:sf-snowpipe_sns_topic_test",
         "Condition":{ 
            "ArnLike":{ 
               "aws:SourceArn":"arn:aws:s3:*:*:REPLACE_WITH_BUCKET_NAME"
            }
         }
      }
   ]
}
```
## Edit the SNS Topic policy:

- Return to the AWS Management Console. Choose SNS Topic created from the left-hand navigation pane.
- Select the checkbox beside the topic for your S3 bucket, and from the Actions menu, click Edit topic policy. 
- Click the Advanced view tab to edit the JSON format of the policy.

- Merge the IAM policy addition from the SYSTEM$GET_AWS_SNS_IAM_POLICY function results into the JSON document.
- Click the Update policy button.

## Configure to publish the S3 event notifications to the SNS Topic

Open the AWS console, find your bucket, click events: 

![Open S3 Bucket Properties](../images/quickstart-auto-ingest-01.png)

Select S3 bucket and configure an event notification for your S3 bucket by completing fields as below:
- Name: Name of the event notification (e.g. Auto-ingest-Snowflake).
- Events: Select the ObjectCreate (All) option.
- Send to: Select SNS Topic from the list.
- SNS Topic ARN: Select the SNS Topic ARN from the dropdown list or add ARN manually in the text box


Next, you can clean up the stages, tables, and tasks for the `departments` table with:

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

When new data files are added to the S3 bucket, the event notification informs Snowpipe to load them into the target table defined in the pipe.
