from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

@udf(StringType())
def get_product_name(url: str) -> str:
    if not url:
        return "Unknown"
    try:
        match = re.search(r'(?:glamira-)?([^/?]+?)(?:-sku|\.html|$)', url)
        if match:
            product = match.group(1)
            return product.replace("-", " ").title()
        return "Unknown"
    except Exception:
        return "Unknown"