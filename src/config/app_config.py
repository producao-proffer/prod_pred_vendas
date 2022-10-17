"""
    The moudle defined the class that loads the environment variables:
     - the database values that compose the connection string.
     - the aws connection access and processing bucket.
"""

from os import environ
from typing import get_type_hints, Union
from dotenv import load_dotenv

load_dotenv()


class AppConfigError(Exception):
    """
    Throw Exception when any env variables are missing
    """


def _parse_bool(val: Union[str, bool]) -> bool:  # pylint: disable=E1136
    return val if isinstance(val, bool) else val.lower() in ['true', 'yes', 1]


class AppConfig:
    """
        AppConfig class with required fields, default values, type checking, and typecasting for int and bool values
    """

    DEBUG: bool = False
    VERSION: str = '1.0.0'
    DATABASE_TENANT_HOSTNAME: str
    DATABASE_TENANT_PORT: int
    DATABASE_TENANT_DBNAME: str
    DATABASE_TENANT_USERNAME: str
    DATABASE_TENANT_PASSWORD: str
    DATABASE_TENANT_PEM_PATH: str

    DOCUMENTDB_USERNAME:str
    DOCUMENTDB_PASSWORD:str
    DOCUMENTDB_DB:str
    DOCUMENTDB_HOST:str
    DOCUMENTDB_PORT:int



    """
        Map environment variables to class fields according to these rules:
        - Field won't be parsed unless it has a type annotation
        - Field will be skipped if not in all caps
        - Class field and environment variable name are the same
    """

    def __init__(self):
        for field in self.__annotations__:
            if not field.isupper():
                continue

            if self._is_missing(field):
                raise AppConfigError(f'The {field} field is required.')

            try:
                value = self._get_env_value(field)
                self.__setattr__(field, value)
            except ValueError as value_error:
                raise AppConfigError(
                    f'Unable to cast value of "{environ[field]}" to type \
                         "{get_type_hints(AppConfig)[field]}" for "{field}" field') from value_error

    def _is_missing(self, field: str) -> bool:
        return getattr(self, field, None) is None and environ.get(field) is None

    def _get_env_value(self, field: str) -> Union[str, bool]:
        if get_type_hints(AppConfig)[field] == bool:
            result = _parse_bool(
                environ.get(field, getattr(self, field, None)))
        else:
            result = get_type_hints(AppConfig)[field](
                environ.get(field, getattr(self, field, None)))

        return result

    def __repr__(self):
        return str(self.__dict__)


Config = AppConfig()
