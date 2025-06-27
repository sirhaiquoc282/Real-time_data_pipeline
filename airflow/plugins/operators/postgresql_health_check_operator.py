import psycopg2
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import logging
from psycopg2 import OperationalError, sql

class PostgreSQLHealthCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 dbname: str,
                 tables: list,
                 schema: str = 'public',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.tables = tables
        self.schema = schema
        self.logger = logging.getLogger(__name__)

    def _check_connection(self):
        try:
            with psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            ) as conn:
                self.logger.info(f"Connected to PostgreSQL database '{self.dbname}' successfully.")
        except OperationalError as e:
            raise AirflowException(f"OperationalError: {e}")
        except Exception as e:
            raise AirflowException(f"Failed to connect to PostgreSQL: {e}")

    def _check_tables_exist(self):
        try:
            with psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            ) as conn:
                with conn.cursor() as cursor:
                    missing_tables = []
                    for table in self.tables:
                        cursor.execute(
                            sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);"),
                            (self.schema, table)
                        )
                        exists = cursor.fetchone()[0]
                        if not exists:
                            missing_tables.append(table)

                    if missing_tables:
                        raise AirflowException(f"Missing tables: {', '.join(missing_tables)}")
                    self.logger.info(f"All required tables exist in schema '{self.schema}'.")

        except Exception as e:
            raise AirflowException(f"Error while checking tables: {e}")

    def execute(self, context):
        self.logger.info("Starting PostgreSQL health check...")
        self._check_connection()
        self._check_tables_exist()
        self.logger.info("PostgreSQL health check completed successfully.")
