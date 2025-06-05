import IP2Location
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark import SparkFiles

ip2loc_instance = None

@udf(StringType())
def get_country(ip: str) -> str:
    global ip2loc_instance
    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
    try:
        if ip2loc_instance is None:
            ip2loc_path = SparkFiles.get("IP-COUNTRY-REGION-CITY.BIN")
            ip2loc_instance = IP2Location.IP2Location(ip2loc_path)

        record = ip2loc_instance.get_all(ip)
        return record.country_long or "Unknown"
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "IP2Location database file not found."
        ) from e


@udf(StringType())
def get_region(ip: str) -> str:
    global ip2loc_instance

    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
    try:
        if ip2loc_instance is None:
            ip2loc_path = SparkFiles.get("IP-COUNTRY-REGION-CITY.BIN")
            ip2loc_instance = IP2Location.IP2Location(ip2loc_path)

        record = ip2loc_instance.get_all(ip)
        return record.region or "Unknown"
    except FileNotFoundError as e:
        raise FileNotFoundError("IP2Location database file not found.") from e

@udf(StringType())
def get_city(ip: str) -> str:
    global ip2loc_instance
    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
    try:
        if ip2loc_instance is None:
            ip2loc_path = SparkFiles.get("IP-COUNTRY-REGION-CITY.BIN")
            ip2loc_instance = IP2Location.IP2Location(ip2loc_path)

        record = ip2loc_instance.get_all(ip)
        return record.city or "Unknown"
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "IP2Location database file not found."
        ) from e
