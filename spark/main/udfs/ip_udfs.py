import IP2Location
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def get_country(ip: str) -> str:
    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
    try:
        db = IP2Location.IP2Location("main/resource/IP-COUNTRY-REGION-CITY.BIN")
        record = db.get_all(ip)
        return record.country_long or "Unknown"
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "IP2Location database file not found. Please ensure the file exists at 'main/resource/IP-COUNTRY-REGION-CITY.BIN'."
        ) from e
    
@udf(StringType())
def get_region(ip: str) -> str:
    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
    try:
        db = IP2Location.IP2Location("main/resource/IP-COUNTRY-REGION-CITY.BIN")
        record = db.get_all(ip)
        return record.region or "Unknown"
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "IP2Location database file not found. Please ensure the file exists at 'main/resource/IP-COUNTRY-REGION-CITY.BIN'."
        ) from e

@udf(StringType())
def get_city(ip: str) -> str:
    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
    try:
        db = IP2Location.IP2Location("main/resource/IP-COUNTRY-REGION-CITY.BIN")
        record = db.get_all(ip)
        return record.city or "Unknown"
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "IP2Location database file not found. Please ensure the file exists at 'main/resource/IP-COUNTRY-REGION-CITY.BIN'."
        ) from e

