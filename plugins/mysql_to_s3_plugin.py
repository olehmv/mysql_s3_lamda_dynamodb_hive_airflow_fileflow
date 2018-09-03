from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.mysql_hook import MySqlHook

from airflow.models import BaseOperator

from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults
import pandas as pd

class MysqlToS3Operator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 sql,
                 index_col,
                 bucket,
                 filename,
                 mysql_conn_id='mysql_default',
                 s3_conn_id='s3_default',
                 *args,
                 **kwargs):
        super(MysqlToS3Operator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.index_col=index_col
        self.bucket = bucket
        self.filename = filename
        self.mysql_conn_id = mysql_conn_id
        self.s3_conn_id = s3_conn_id

    def _query_mysql(self):
        """
        Queries mysql and returns a cursor to the results.
        """
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = mysql.get_conn()
        df = pd.read_sql(sql=self.sql, con=conn,index_col=self.index_col)
        return df

    def execute(self, context):
        df = self._query_mysql()
        s3=S3Hook(s3_conn_id=self.s3_conn_id)
        conn = s3.get_conn()
        bucket = conn.get_bucket(self.bucket)
        key = bucket.new_key(self.filename)
        key.set_contents_from_string(df.to_csv())


# Defining the plugin class
class MysqlToS3Plugin(AirflowPlugin):
    name = "mysql_to_s3_plugin"
    operators = [MysqlToS3Operator]

    hooks = [MySqlHook,S3Hook]

    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []

