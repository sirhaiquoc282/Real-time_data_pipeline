from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2 import connect, OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Optional, List

class PostgreSQLHealthCheckException(Exception):
    pass

class PostgreSQLHealthCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, 
        host: str,
        port: int,
        user: str,
        password: str,
        dbname: Optional[str] = "postgres",
        tables: Optional[List[str]] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.tables = tables or []

    def execute(self, context):
        self.log.info("Starting PostgreSQL health checks...")
        conn_postgres = None
        conn_db = None
        
        try:
            conn_postgres = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname="postgres"
            )
            conn_postgres.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            with conn_postgres.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (self.dbname,)
                )
                if not cursor.fetchone():
                    raise PostgreSQLHealthCheckException(
                        f"Database '{self.dbname}' does not exist"
                    )
                    
            self.log.info(f"Database '{self.dbname}' verified")
            conn_postgres.close()

            if self.tables:
                conn_db = connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname=self.dbname
                )
                
                with conn_db.cursor() as cursor:
                    for table in self.tables:
                        cursor.execute(
                            """SELECT EXISTS (
                                SELECT 1 
                                FROM information_schema.tables 
                                WHERE table_name = %s
                                AND table_schema = 'public'
                            )""",
                            (table,)
                        )
                        exists = cursor.fetchone()[0]
                        
                        if not exists:
                            raise PostgreSQLHealthCheckException(
                                f"Table '{table}' does not exist in database '{self.dbname}'"
                            )
                            
                        self.log.info(f"Table '{table}' verified")
                
                conn_db.close()

            self.log.info("All PostgreSQL health checks passed")

        except OperationalError as e:
            raise PostgreSQLHealthCheckException(
                f"Connection failed: {str(e)}"
            ) from e
        except Exception as e:
            raise PostgreSQLHealthCheckException(
                f"Health check error: {str(e)}"
            ) from e
        finally:
            if conn_postgres and not conn_postgres.closed:
                conn_postgres.close()
            if conn_db and not conn_db.closed:
                conn_db.close()
            self.log.info("Completed PostgreSQL health checks")