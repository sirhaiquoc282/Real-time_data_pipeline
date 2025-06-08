"""
URL Processing UDFs Module

This module provides PySpark User Defined Functions (UDFs) for extracting product
information from URLs. These functions support the product dimension processing
in the data warehouse ETL pipeline.

Dependencies:
    - re: Python regular expressions library for URL pattern matching

Usage:
    These UDFs are used in product dimension processing to extract:
    - Product names from current_url field in raw event data
    - Standardized product naming for consistent reporting

Business Context:
    - Handles Glamira-specific URL patterns and product naming conventions
    - Supports e-commerce product catalog standardization
    - Enables product analytics and merchandising insights
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re


@udf(StringType())
def get_product_name(url: str) -> str:
    """
    Extract product name from URL using pattern matching.
    
    This UDF parses product URLs to extract standardized product names for
    the product dimension table. Handles various URL formats and naming
    conventions specific to the e-commerce platform.
    
    Args:
        url (str): Product URL from current_url field in raw events
                  Expected formats:
                  - /glamira-product-name-sku123
                  - /product-name.html
                  - /product-name/
                  - /product-name
        
    Returns:
        str: Standardized product name with proper capitalization
             - Hyphens converted to spaces
             - Title case formatting applied
             - Returns "Unknown" for invalid/unparseable URLs
             
    Business Rules:
        - Removes 'glamira-' prefix if present for brand consistency
        - Strips SKU suffixes (e.g., '-sku123') for clean product names
        - Removes file extensions (.html) for standardization
        - Converts hyphenated names to space-separated title case
        - Fallback to "Unknown" for invalid URLs or parsing failures
        
    URL Pattern Matching:
        - Regex: r'(?:glamira-)?([^/?]+?)(?:-sku|\.html|$)'
        - Captures product name portion between path separators
        - Handles optional 'glamira-' prefix
        - Stops at SKU suffix or file extension
        
    Examples:
        >>> get_product_name("/glamira-diamond-ring-sku123")
        "Diamond Ring"
        
        >>> get_product_name("/wedding-band.html")
        "Wedding Band"
        
        >>> get_product_name("/engagement-ring/")
        "Engagement Ring"
        
        >>> get_product_name("invalid-url")
        "Unknown"
        
        >>> get_product_name(None)
        "Unknown"
    
    Data Quality Notes:
        - Handles null/empty URLs gracefully
        - Exception handling prevents UDF failures
        - Consistent "Unknown" fallback maintains data integrity
    """
    # Handle null or empty URLs
    if not url:
        return "Unknown"
        
    try:
        # Pattern explanation:
        # (?:glamira-)? - Optional non-capturing group for 'glamira-' prefix
        # ([^/?]+?)     - Capturing group for product name (non-greedy)
        #                 Matches any character except '/' and '?'
        # (?:-sku|\.html|$) - Non-capturing group for terminators:
        #                     - '-sku' (SKU suffix)
        #                     - '.html' (file extension)
        #                     - '$' (end of string)
        match = re.search(r'(?:glamira-)?([^/?]+?)(?:-sku|\.html|$)', url)
        
        if match:
            # Extract the product name portion
            product = match.group(1)
            
            # Transform to standardized format:
            # 1. Replace hyphens with spaces for readability
            # 2. Apply title case for consistent capitalization
            return product.replace("-", " ").title()
            
        # No match found - return fallback value
        return "Unknown"
        
    except Exception:
        # Handle any unexpected errors in regex processing
        # Ensures UDF doesn't fail and maintains data pipeline stability
        return "Unknown"