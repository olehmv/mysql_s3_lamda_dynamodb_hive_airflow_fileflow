from datetime import datetime
from urlparse import urlparse
import ast
import pandas as pd
from airflow import DAG
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
from fileflow.operators import DivePythonOperator
from fileflow.storage_drivers import S3StorageDriver
from fileflow.task_runners import TaskRunner


class TaskRunnerWriteToS3FromMySQL(TaskRunner):
    def __init__(self,context):
        """
        :param context: Required.
        """
        super(TaskRunnerWriteToS3FromMySQL, self).__init__(context)
        print self.date
        print type(context['ts'])
        print  datetime(int(Variable.get("start_date_year")),int(Variable.get("start_date_month")),int(Variable.get("start_date_day")),13,9)
        print self.date

    def run(self, *args, **kwargs):
        sql=Variable.get("sql")
        mysql_conn_id=Variable.get("mysql_conn_id")
        aws_access_key_id=Variable.get("aws_access_key_id")
        aws_secret_access_key=Variable.get("aws_secret_access_key")
        aws_bucket_name=str(Variable.get("aws_bucket_name"))
        self.storage=S3StorageDriver(aws_access_key_id,aws_secret_access_key,aws_bucket_name)
        mysql = MySqlHook(mysql_conn_id)
        conn = mysql.get_conn()
        df = pd.read_sql(sql,conn)
        self.write_pandas_csv(df)



class TaskRunnerWriteToHiveFromS3(TaskRunner):
    def __init__(self,context):
        """
        :param context: Required.
        """
        super(TaskRunnerWriteToHiveFromS3, self).__init__(context)
    def run(self, *args, **kwargs):
        aws_access_key_id=Variable.get("aws_access_key_id")
        aws_secret_access_key=Variable.get("aws_secret_access_key")
        aws_bucket_name=Variable.get("aws_bucket_name")
        hive_cli_conn_id = Variable.get("hive_cli_conn_id")
        field_dict=Variable.get("field_dict")
        self.storage=S3StorageDriver(aws_access_key_id,aws_secret_access_key,aws_bucket_name)
        url = self.get_input_filename("file")
        parse_url = urlparse(url)
        cli_hook = HiveCliHook(hive_cli_conn_id=hive_cli_conn_id)
        path= '{}{}{}{}{}{}{}{}'.format(parse_url.scheme, "n://", aws_access_key_id, ":", aws_secret_access_key, "@",
                                        str(parse_url.netloc), parse_url.path)
        bucketname = '{}{}'.format("s3_", str(parse_url.path).split("/").pop())
        cli_hook.load_file(field_dict=ast.literal_eval(field_dict),table=bucketname,filepath=path)

dag = DAG(
    dag_id=Variable.get("mysql_s3_hive_dag_id"),
    start_date=datetime(int(Variable.get("start_date_year")),int(Variable.get("start_date_month")),int(Variable.get("start_date_day")),13,9),
    schedule_interval= Variable.get("schedule_interval")
)

t1 = DivePythonOperator(
    task_id=Variable.get("write_mysql_s3_task_id"),
    python_method="run",
    python_object=TaskRunnerWriteToS3FromMySQL,
    provide_context=True,
    owner=Variable.get("owner"),
    dag=dag
)

t2 = DivePythonOperator(
    task_id=Variable.get("write_s3_hive_task_id"),
    python_object=TaskRunnerWriteToHiveFromS3,
    data_dependencies={"file": t1.task_id},
    provide_context=True,
    owner=Variable.get("owner"),
    dag=dag
)


t2.set_upstream(t1)
