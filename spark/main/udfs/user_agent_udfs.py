"""
User Agent Processing UDFs Module

This module provides PySpark User Defined Functions (UDFs) for extracting device
and browser information from user agent strings. These functions support the
browser and OS dimension processing in the data warehouse ETL pipeline.

Dependencies:
    - user_agents: Third-party library for parsing user agent strings
    - Provides comprehensive browser and OS detection capabilities

Usage:
    These UDFs are used in dimension processing to extract:
    - Browser information for user behavior analysis
    - Operating system data for device analytics
    - Platform insights for product optimization

Business Context:
    - Enables user experience analysis across different browsers
    - Supports device targeting and optimization strategies
    - Provides insights for technical support and compatibility planning
    - Powers marketing segmentation based on user technology preferences

Performance Notes:
    - User agent parsing is computationally intensive
    - Consider caching results for frequently seen user agent strings
    - Library handles complex user agent string variations automatically
"""

import user_agents
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(StringType())
def get_browser(user_agent: str) -> str:
    """
    Extract browser name from user agent string.
    
    This UDF parses user agent strings to identify the browser family for
    browser dimension processing. Used for user behavior analysis and
    browser compatibility insights.
    
    Args:
        user_agent (str): Raw user agent string from HTTP headers
                         Examples:
                         - "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                         - "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1"
        
    Returns:
        str: Browser family name (e.g., "Chrome", "Firefox", "Safari", "Edge")
             Returns "Unknown" for invalid/unparseable user agent strings
             
    Business Rules:
        - Returns browser family rather than specific version for grouping
        - Handles mobile and desktop browser variants consistently
        - Maps browser variants to primary family names (e.g., Chromium → Chrome)
        - Fallback to "Unknown" for unrecognized or malformed user agents
        
    Browser Family Examples:
        - Chrome: Includes Chrome, Chromium, Chrome Mobile
        - Firefox: Includes Firefox, Firefox Mobile, Firefox Focus
        - Safari: Includes Safari, Mobile Safari, Safari Technology Preview
        - Edge: Includes Microsoft Edge, Edge Mobile
        - Internet Explorer: IE versions and compatibility modes
        
    Data Quality Notes:
        - Handles null/empty user agent strings gracefully
        - Exception handling prevents UDF failures from malformed strings
        - Consistent "Unknown" fallback maintains dimensional integrity
        
    Examples:
        >>> get_browser("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124")
        "Chrome"
        
        >>> get_browser("Mozilla/5.0 (iPhone; CPU iPhone OS 14_6) AppleWebKit/605.1.15 Safari/604.1")
        "Mobile Safari"
        
        >>> get_browser("Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko")
        "IE"
        
        >>> get_browser("")
        "Unknown"
    """
    # Handle null or empty user agent strings
    if not user_agent:
        return "Unknown"
        
    try:
        # Parse user agent string using user_agents library
        ua = user_agents.parse(user_agent)
        
        # Extract browser family name
        # ua.browser.family provides the primary browser family
        # (e.g., 'Chrome' instead of 'Chrome 91.0.4472.124')
        return ua.browser.family or "Unknown"
        
    except Exception as e:
        # Handle parsing errors gracefully
        # Malformed or unrecognized user agent strings may cause exceptions
        # Return "Unknown" to maintain data pipeline stability
        return "Unknown"
    

@udf(StringType())
def get_os(user_agent: str) -> str:
    """
    Extract operating system name from user agent string.
    
    This UDF parses user agent strings to identify the operating system family
    for OS dimension processing. Used for device analytics and platform insights.
    
    Args:
        user_agent (str): Raw user agent string from HTTP headers
                         Same format as get_browser() function
        
    Returns:
        str: Operating system family name (e.g., "Windows", "iOS", "Android", "macOS")
             Returns "Unknown" for invalid/unparseable user agent strings
             
    Business Rules:
        - Returns OS family rather than specific version for grouping
        - Handles mobile and desktop OS variants consistently
        - Maps OS variants to primary family names
        - Distinguishes between major OS families for analytics
        
    OS Family Examples:
        - Windows: Windows 10, Windows 11, Windows Server variants
        - macOS: macOS Big Sur, Monterey, Ventura (formerly Mac OS X)
        - iOS: iPhone OS, iPad OS variants
        - Android: All Android versions and OEM customizations
        - Linux: Ubuntu, CentOS, Debian, and other distributions
        - Chrome OS: Chromebook operating system
        
    Data Quality Notes:
        - Handles null/empty user agent strings gracefully
        - Exception handling prevents UDF failures
        - Consistent "Unknown" fallback for unrecognized OS strings
        - Mobile vs desktop OS distinction preserved in family names
        
    Analytics Use Cases:
        - Device type analysis (mobile vs desktop usage patterns)
        - OS market share tracking for product planning
        - Technical support prioritization by OS prevalence
        - Cross-platform compatibility testing insights
        
    Examples:
        >>> get_os("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        "Windows"
        
        >>> get_os("Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)")
        "iOS"
        
        >>> get_os("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        "Mac OS X"
        
        >>> get_os("Mozilla/5.0 (X11; Linux x86_64)")
        "Linux"
        
        >>> get_os("invalid-user-agent")
        "Unknown"
    """
    # Handle null or empty user agent strings
    if not user_agent:
        return "Unknown"
        
    try:
        # Parse user agent string using user_agents library
        ua = user_agents.parse(user_agent)
        
        # Extract operating system family name
        # ua.os.family provides the primary OS family
        # (e.g., 'Windows' instead of 'Windows 10.0')
        return ua.os.family or "Unknown"
        
    except Exception as e:
        # Handle parsing errors gracefully
        # Malformed user agent strings or library issues may cause exceptions
        # Return "Unknown" to maintain data pipeline stability and consistency
        return "Unknown"