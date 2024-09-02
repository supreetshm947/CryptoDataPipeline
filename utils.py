from datetime import datetime
import functools
from pyspark.sql.utils import AnalysisException, QueryExecutionException

from mylogger import get_logger

logger = get_logger()

def convert_iso_to_datetime(iso_date_str):
    # Replace 'Z' with '+00:00' to make it compatible with datetime.fromisoformat()
    iso_format_string = iso_date_str.replace('Z', '+00:00')
    return datetime.fromisoformat(iso_format_string)

def convert_seconds_to_datetime(seconds):
    created_utc_dt = datetime.utcfromtimestamp(seconds)
    created_utc_iso = created_utc_dt.isoformat() + 'Z'
    return created_utc_iso

def exception_logger(func_name):
    """
    Decorator that logs exceptions for the wrapped function.

    Parameters:
    - func_name: Name of the function being wrapped.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (AnalysisException, QueryExecutionException) as e:
                logger.error(f"Error in {func_name}: {e}")
                raise  # Re-raise the exception so the caller can handle it
            except Exception as e:
                logger.error(f"Unexpected Error in {func_name}: {e}")
                raise  # Re-raise the exception so the caller can handle it

        return wrapper

    return decorator