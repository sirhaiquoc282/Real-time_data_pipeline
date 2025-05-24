from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
from user_agents import parse 
import pycountry


@udf(StringType())
def extract_country_code(url: str) -> str:
    try:
        pattern = r"https?://(?:www\.)?[^\.]+\.([a-z]{2,3})"
        match = re.search(pattern, url)
        if match:
            return match.group(1).lower()
    except:
        pass
    return "unknown"


@udf(StringType())
def extract_country_name(url: str) -> str:
    try:
        pattern = r"https?://(?:www\.)?[^\.]+\.([a-z]{2,3})"
        match = re.search(pattern, url)
        if match:
            code = match.group(1).upper()
            country = pycountry.countries.get(alpha_2=code)
            if country:
                return country.name
    except:
        pass
    return "Unknown"


@udf(StringType())
def extract_product_name(url: str) -> str:
    if url is None:
        return "Unknown"
    match = re.search(r"https?://[^/]+/([^?]+)", url)
    if match:
        path = match.group(1)
        product_part = path.split(".html")[0]
        return product_part.replace("-", " ")
    return "Unknown"

@udf(StringType())
def extract_browser(user_agent: str) -> str:
    try:
        ua = parse(user_agent)
        return ua.browser.family
    except:
        return "Unknown"

@udf(StringType())
def extract_os(user_agent: str) -> str:
    try:
        ua = parse(user_agent)
        return ua.os.family
    except:
        return "Unknown"

