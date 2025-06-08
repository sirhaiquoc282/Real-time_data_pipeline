"""
IP Geolocation UDFs Module

This module provides PySpark User Defined Functions (UDFs) for extracting geographic
information from IP addresses using the IP2Location database. These functions support
the location dimension processing in the data warehouse ETL pipeline.

Dependencies:
    - IP2Location: Third-party library for IP geolocation
    - IP-COUNTRY-REGION-CITY.BIN: Binary database file distributed via SparkFiles

Global Variables:
    - ip2loc_instance: Singleton instance of IP2Location for performance optimization

Usage:
    These UDFs are used in dimension processing functions to extract:
    - Country information for geographic analysis
    - Region/state data for regional reporting
    - City-level data for local market insights

Performance Notes:
    - IP2Location instance is lazily initialized and reused across function calls
    - Database file is accessed via SparkFiles for distributed processing
    - Invalid/unknown IPs return "Unknown" to maintain data quality
"""

import IP2Location
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark import SparkFiles

# Global singleton instance for performance optimization
# Avoids repeated database file loading across UDF calls
ip2loc_instance = None


@udf(StringType())
def get_country(ip: str) -> str:
    """
    Extract country name from IP address using IP2Location database.
    
    This UDF performs IP geolocation to determine the country associated with
    an IP address. Used in location dimension processing for geographic analysis.
    
    Args:
        ip (str): IP address to lookup (IPv4 format expected)
        
    Returns:
        str: Full country name (e.g., "United States", "United Kingdom")
             Returns "Unknown" for invalid/unresolvable IPs
             
    Business Rules:
        - Invalid IPs ("", "unknown", "0.0.0.0") return "Unknown"
        - Uses full country name (country_long) for better readability
        - Fallback to "Unknown" if geolocation lookup fails
        
    Raises:
        FileNotFoundError: If IP2Location database file is not found in SparkFiles
        
    Example:
        >>> get_country("8.8.8.8")
        "United States"
        >>> get_country("invalid_ip")
        "Unknown"
    """
    global ip2loc_instance
    
    # Handle invalid or placeholder IP addresses
    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
        
    try:
        # Lazy initialization of IP2Location instance for performance
        if ip2loc_instance is None:
            ip2loc_path = SparkFiles.get("IP-COUNTRY-REGION-CITY.BIN")
            ip2loc_instance = IP2Location.IP2Location(ip2loc_path)

        # Perform IP geolocation lookup
        record = ip2loc_instance.get_all(ip)
        return record.country_long or "Unknown"
        
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "IP2Location database file not found. "
            "Ensure IP-COUNTRY-REGION-CITY.BIN is distributed via SparkFiles."
        ) from e


@udf(StringType())
def get_region(ip: str) -> str:
    """
    Extract region/state name from IP address using IP2Location database.
    
    This UDF performs IP geolocation to determine the region/state associated
    with an IP address. Used for regional analysis and market segmentation.
    
    Args:
        ip (str): IP address to lookup (IPv4 format expected)
        
    Returns:
        str: Region/state name (e.g., "California", "New York", "Ontario")
             Returns "Unknown" for invalid/unresolvable IPs
             
    Business Rules:
        - Invalid IPs ("", "unknown", "0.0.0.0") return "Unknown"
        - Region granularity varies by country (state, province, region)
        - Fallback to "Unknown" if region data unavailable
        
    Raises:
        FileNotFoundError: If IP2Location database file is not found in SparkFiles
        
    Example:
        >>> get_region("8.8.8.8")
        "California"
        >>> get_region("192.168.1.1")
        "Unknown"
    """
    global ip2loc_instance

    # Handle invalid or placeholder IP addresses
    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
        
    try:
        # Lazy initialization of IP2Location instance
        if ip2loc_instance is None:
            ip2loc_path = SparkFiles.get("IP-COUNTRY-REGION-CITY.BIN")
            ip2loc_instance = IP2Location.IP2Location(ip2loc_path)

        # Perform IP geolocation lookup
        record = ip2loc_instance.get_all(ip)
        return record.region or "Unknown"
        
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "IP2Location database file not found. "
            "Ensure IP-COUNTRY-REGION-CITY.BIN is distributed via SparkFiles."
        ) from e


@udf(StringType())
def get_city(ip: str) -> str:
    """
    Extract city name from IP address using IP2Location database.
    
    This UDF performs IP geolocation to determine the city associated with
    an IP address. Used for local market analysis and city-level reporting.
    
    Args:
        ip (str): IP address to lookup (IPv4 format expected)
        
    Returns:
        str: City name (e.g., "Mountain View", "New York", "London")
             Returns "Unknown" for invalid/unresolvable IPs
             
    Business Rules:
        - Invalid IPs ("", "unknown", "0.0.0.0") return "Unknown"
        - City-level accuracy varies by IP database coverage
        - Fallback to "Unknown" if city data unavailable
        
    Raises:
        FileNotFoundError: If IP2Location database file is not found in SparkFiles
        
    Performance Notes:
        - City lookups are most granular and may have lower accuracy
        - Consider data quality implications for city-level analysis
        
    Example:
        >>> get_city("8.8.8.8")
        "Mountain View"
        >>> get_city("127.0.0.1")
        "Unknown"
    """
    global ip2loc_instance
    
    # Handle invalid or placeholder IP addresses
    if not ip or ip in ("", "unknown", "0.0.0.0"):
        return "Unknown"
        
    try:
        # Lazy initialization of IP2Location instance
        if ip2loc_instance is None:
            ip2loc_path = SparkFiles.get("IP-COUNTRY-REGION-CITY.BIN")
            ip2loc_instance = IP2Location.IP2Location(ip2loc_path)

        # Perform IP geolocation lookup
        record = ip2loc_instance.get_all(ip)
        return record.city or "Unknown"
        
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "IP2Location database file not found. "
            "Ensure IP-COUNTRY-REGION-CITY.BIN is distributed via SparkFiles."
        ) from e