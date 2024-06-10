import logging
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

class CustomRedshiftOperator(BaseOperator):
    """
    Custom Airflow operator to execute SQL scripts on AWS Redshift.
    """
    template_fields = ('sql_file',)
    template_ext = ('.sql',)
    
    def __init__(self, sql_file: str, redshift_conn_id: str, *args, **kwargs):
        """
        Initialize the CustomRedshiftOperator.

        Args:
            sql_file (str): Path to the SQL file to be executed.
            redshift_conn_id (str): Connection ID.
        """
        super().__init__(*args, **kwargs)
        self.sql_file = sql_file
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Execute the SQL script on AWS Redshift.

        Args:
            context (dict): Airflow context dictionary.
        """
        # Split the SQL script into individual commands
        sql_script = self.sql_file
        sql_commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
        
        # Create a Redshift hook
        redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        
        # Execute each SQL command
        for sql_command in sql_commands:
            logging.info(f"Executing SQL: {sql_command}")
            redshift_hook.run(sql_command)
        
        
        
        
        