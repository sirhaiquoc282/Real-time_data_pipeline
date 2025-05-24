from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pymongo import MongoClient
from pymongo.errors import (
    NetworkTimeout,
    InvalidURI,
    ConnectionFailure,
    PyMongoError
)
from typing import Optional

class MongoDBHealthCheckException(Exception):
    pass

class MongoDBHealthCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self,uri: str, server_selection_timeout_ms: Optional[int] = 5000 ,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uri = uri
        self.server_selection_timeout_ms = server_selection_timeout_ms
        
    def execute(self, context):
        self.log.info("Starting MongoDB health checks...")
        try:
            client = MongoClient(self.uri, serverSelectionTimeoutMS=self.server_selection_timeout_ms)
            client.admin.command("ping")
            self.log.info("Health checked MongoDB successfully.")
        except InvalidURI as e:
            raise MongoDBHealthCheckException(
                f"Invalid connection URI: {str(e)}"
            ) from e
        except NetworkTimeout as e:
            raise MongoDBHealthCheckException(
                f"Network timeout ({self.server_selection_timeout_ms}ms): {str(e)}"
            ) from e
        except ConnectionFailure as e:
            raise MongoDBHealthCheckException(
                f"Connection failed: {str(e)}"
            ) from e
        except PyMongoError as e:
            raise MongoDBHealthCheckException(
                f"MongoDB error: {str(e)}"
            ) from e
        except Exception as e:
            raise MongoDBHealthCheckException(
                f"Unexpected error: {str(e)}"
            ) from e
        finally:
            if client:
                client.close()
                self.log.info("Closed MongoDB connection")
            
        