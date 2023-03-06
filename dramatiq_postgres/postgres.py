import datetime
import typing

import psycopg
from dramatiq.results.backend import Missing, ResultBackend
from psycopg import Connection, Notify
from psycopg.sql import SQL, Identifier

# Types
MessageData = typing.Dict[str, typing.Any]
Result = typing.Any


class PostgresBackend(ResultBackend):
    """A result backend for PostgreSQL.
    Parameters:
      namespace(str): A string with which to prefix result keys.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
      connection_params(dict): A dictionary of parameters to pass to the
        `psycopg.connect()` function.
      url(str): An optional connection URL.  If both a URL and
        connection parameters are provided, the URL is used.
      connection(Connection): A postgres connection to use, if provided
      will be used instead of connection_params or url. cant be an (AsyncConnection)
    """

    def __init__(
        self,
        *,
        namespace="dramatiq_results",
        encoder=None,
        connection_params=None,
        url=None,
        connection=None
    ):
        super().__init__(namespace=namespace, encoder=encoder)

        self.url = url
        self.connection_params = connection_params or {}
        self.connection: Connection
        self.gen: typing.Generator[Notify, None, None]

        if not connection:
            if self.url is not None:
                self.connection = psycopg.Connection.connect(self.url)
            else:
                self.connection = psycopg.Connection.connect(**self.connection_params)
        else:
            self.connection = connection

        # checks if the connection used is not a AsyncConnection
        if not isinstance(self.connection, Connection):
            raise Exception(
                "Please use an Connection to postgres and not an AsyncConnection"
            )

        # Create the result table if it doesn't exist
        try:
            self.connection.execute(
                SQL(
                    "CREATE TABLE IF NOT EXISTS {} ("
                    "message_key VARCHAR(256) PRIMARY KEY,"
                    "result BYTEA NOT NULL,"
                    "created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),"
                    "expires_at TIMESTAMP WITH TIME ZONE NULL"
                    ")"
                ).format(Identifier(self.namespace)),
            )
            self.connection.commit()
        except psycopg.errors.UniqueViolation as error:
            pass

    def _get(self, message_key: str):
        """Get the result from the backend.

        Args:
            message_key (str)

        Returns:
            data: the stored data

            missing : if the data is None then return this value
        """
        data = self.postgres_select(message_key)
        if data is not None:
            return self.encoder.decode(data)
        return Missing

    def postgres_select(self, message_key: str):
        """query the backend to get the result and expires_at
        if all_data is none or the message has expired then return None

        Args:
            message_key (str)

        Returns:
            data: the stored data
        """
        exe = self.connection.execute(
            SQL("SELECT result, expires_at FROM {} WHERE message_key=%s").format(
                Identifier(self.namespace)
            ),
            (message_key,),
        )
        self.connection.commit()
        all_data = exe.fetchone()

        if all_data is None:
            return None

        data = all_data[0]

        # check if the message has expired or not
        time_check = all_data[1]
        if time_check:
            if time_check < datetime.datetime.now().astimezone():
                return None
        return data

    def _store(self, message_key: str, result: Result, ttl: int):
        """Stores the message inside postgres

        Args:
            message_key (str): The message_key which is used as a primary key
            result (Result): The result of the task
            ttl (int): Time to live
        """

        expires_at = datetime.datetime.now().astimezone() + datetime.timedelta(
            milliseconds=ttl
        )
        self.connection.execute(
            SQL(
                "INSERT INTO {} (message_key, result, expires_at) VALUES (%s, %s, %s)"
            ).format(Identifier(self.namespace)),
            (
                message_key,
                self.encoder.encode(result),
                expires_at,
            ),
        )
        self.connection.commit()
