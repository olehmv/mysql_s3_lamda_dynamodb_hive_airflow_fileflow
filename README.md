Usage:

    Install mysql
    https://www.digitalocean.com/community/tutorials/how-to-install-the-latest-mysql-on-ubuntu-18-04

    Install hive
    https://www.janbasktraining.com/blog/complete-guide-apache-hive-installation-ubuntu/

    $ cd /path/to/workspace

    create virtual environment
    $ virtualenv venv

    activate virtual environment
    $ source venv/bin/activate

    install airflow fileflow
    $ pip install git+git://github.com/industrydive/fileflow.git#egg=fileflow

    install airflow
    $ pip install airflow[mysql,s3,hive]==1.8.0

    change panda version
    $ pip install pandas==0.17.0

    show dependencies
    $ pip freeze

    Go to /path/to/workspace/venv/local/lib/python2.7/site-packages/fileflow/storage_drivers/storage_driver.py
    Replace in method

        def execution_date_string(self, execution_date)

            return execution_date.strftime('%Y %m %d')
            to
            return execution_date.strftime('%Y%m%d%H%M%S')

    This string  will be used as file name in s3 bucket


    Go to /path/to/workspace/venv/local/lib/python2.7/site-packages/airflow/hooks/hive_hooks.py
    Replace in method

        def load_file(self,filepath,table,delimiter=",",field_dict=None,create=True,overwrite=True,partition=None,recreate=False):

             hql = "LOAD DATA LOCAL INPATH '{filepath}'
             to
             hql = "LOAD DATA INPATH '{filepath}'

    This method will load file from s3 to hive

    $ git clone https://github.com/olehmv/mysql_s3_lamda_dynamodb_hive_airflow_fileflow.git
    $ cd mysql_s3_lamda_dynamodb_hive_airflow_fileflow

    airflow.cfg is configured to use mysql as airflow metastore db remove it and unittests.cfg

    $ export AIRFLOW_HOME=`pwd`/mysql_s3_lamda_dynamodb_hive_airflow_fileflow
    $ airflow version
    $ tree mysql_s3_lamda_dynamodb_hive_airflow_fileflow
    mysql_s3_lamda_dynamodb_hive_airflow_fileflow
      ├── airflow.cfg
      └── unittests.cfg

    init airflow database
    $ airflow initdb
    $ tree mysql_s3_lamda_dynamodb_hive_airflow_fileflow
    mysql_s3_lamda_dynamodb_hive_airflow_fileflow
          ├── airflow.cfg
          ├── airflow.db        <- Airflow SQLite DB
          └── unittests.cfg

    start airflow webserver
    $ airflow webserver

    Open new termminal

    $ cd /path/to/workspace
    $ source venv/bin/activate
    $ export AIRFLOW_HOME=`pwd`/mysql_s3_lamda_dynamodb_hive_airflow_fileflow

    start airflow scheduler
    $ airflow scheduler

    Go to Airflow Web UI http://localhost:8080/admin/

    Go to Airflow Web UI http://localhost:8080/admin/connection/
    Configure Airflow connection settings: mysql_default, hive_cli_default

    Got to http://localhost:8080/admin/variable/
    Add Airflow variables as in example Admin-Variables-Airflow.html

    Go to amazon s3 service, create bucket
    Go to amazon dynamodb service, create table named as bucket name
    Go to amazon IAM service, create role, example:
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": [
                    "s3:*",
                    "logs:*",
                    "dynamodb:*"
                ],
                "Resource": "*"
            }
        ]
    }
    Go to amazon lamda service, create lambda, add :
        previously created role
        trigger on previously created bucket
        lamda function:
                    import boto3

                    def lambda_handler(event, context):
                        print event
                        s3 = boto3.client("s3")
                        dynamodb=boto3.resource("dynamodb")
                        bucket=event['Records'][0]['s3']['bucket']['name']
                        key=event['Records'][0]['s3']['object']['key']
                        obj=s3.get_object(Bucket=bucket,Key=key)
                        rows=str(obj['Body'].read().decode('utf-8')).replace("\"","").split('\n')
                        rows=filter(None,rows)
                        print rows
                        header=rows.pop(0).split(',')
                        table=dynamodb.Table(bucket)
                        with table.batch_writer() as batch:
                            for row in rows:
                                row_split=row.split(',')
                                item={}
                                for idx, head in enumerate(header):
                                    item[head]=row_split[idx]
                                print item
                                batch.put_item(Item=item)


    Go to Airflow Web UI http://localhost:8080/admin/
    Start Airflow Dag
