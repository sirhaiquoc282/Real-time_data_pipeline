import user_agents
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(StringType())
def get_browser(user_agent: str) -> str:
    if not user_agent:
        return "Unknown"
    try:
        ua = user_agents.parse(user_agent)
        return ua.browser.family or "Unknown"
    except Exception as e:
        return "Unknown"
    

@udf(StringType())
def get_os(user_agent: str) -> str:
    if not user_agent:
        return "Unknown"
    try:
        ua = user_agents.parse(user_agent)
        return ua.os.family or "Unknown"
    except Exception as e:
        return "Unknown"