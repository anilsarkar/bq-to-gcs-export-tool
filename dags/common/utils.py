from datetime import date, datetime, timedelta


def get_default_args(retries: int =1, provide_context: bool = False):
    """
    Returns the default args required to create a DAG.
    :param provide_context: If set to true, Airflow will pass a set of keyword arguments that can be 
    used in your function. For this to work, you need to define **kwargs in your function header.
    :param retries: number if retries default to 1
    :return default args dictionary
    """
    return {
        "start_date":datetime(2022, 4, 10),
        "depends_on_past": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=3),
        "provide_context": provide_context
    }
