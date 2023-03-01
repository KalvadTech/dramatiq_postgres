import asyncio
import datetime
import typing

import psycopg
from psycopg import AsyncConnection, Notify
from psycopg.sql import SQL, Identifier

from ..backend import DEFAULT_TIMEOUT, ResultBackend, ResultMissing, ResultTimeout

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
      connection(AsyncConnection): A postgres connection to use, cant be a (connection)
      due to the heavy use of asyncio
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
        self.connection: AsyncConnection
        self.gen: typing.AsyncGenerator[Notify, None]
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.connect(connection))
        loop.close()

    async def connect(self, connection):
        """connects to postgres backend

        Args:
            connection (AsyncConnection): an AsyncConnection to postgres
            if none create a new connection to postgres

        Raises:
            Exception: if autocommit isn't enabled or
            if the postgres connection isn't an AsyncConnection
        """
        # creating connection
        if not connection:
            if self.url is not None:
                self.connection = await psycopg.AsyncConnection.connect(
                    self.url, autocommit=True
                )
            else:
                self.connection = await psycopg.AsyncConnection.connect(
                    **self.connection_params, autocommit=True
                )
        else:
            self.connection = connection

        # Because of the way sessions interact with notifications (see NOTIFY documentation),
        # you should keep the connection in autocommit mode
        # if you wish to receive or send notifications in a timely manner.
        if not self.connection.autocommit:
            raise Exception("Psycopg postgres connection must have autocommit=True")

        # Because of the heavy use of AsyncConnection the use
        # of a Connection to postgres would not work
        if not isinstance(self.connection, AsyncConnection):
            raise Exception("Please use an AsyncConnection to postgres")

        # postgres listener
        self.gen = self.connection.notifies()
        await self.connection.execute("LISTEN dramatiq")  # we need to keep requesting

        # Create the result table if it doesn't exist
        await self.connection.execute(
            SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "message_key VARCHAR(256) PRIMARY KEY,"
                "result BYTEA NOT NULL,"
                "created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),"
                "expires_at TIMESTAMP WITH TIME ZONE NULL"
                ")"
            ).format(Identifier(self.namespace)),
        )

    def get_result(self, message, *, block=False, timeout=None) -> MessageData:
        """Get a result from the backend.
        Parameters:
          message(Message)
          block(bool): Whether or not to block until a result is set.
          timeout(int): The maximum amount of time, in ms, to wait for
            a result when block is True.  Defaults to 10 seconds.
        Raises:
          ResultMissing: When the result isn't set.
          ResultTimeout: When waiting for a result times out.
        Returns:
          MessageData: The MessageData.
        """

        if timeout is None:
            timeout = DEFAULT_TIMEOUT

        message_key = self.build_message_key(message)

        # Run a asyncio event loop that exits until the timeout timer is reached
        # this allows for dynamically getting the message if block is True
        # as otherwise it would have to wait the timeout then look for the message
        try:
            loop = asyncio.new_event_loop()
            wait = asyncio.wait_for(
                self.async_get_result(message_key, block), timeout=timeout / 1000
            )
            data = loop.run_until_complete(wait)
            loop.close()

        except TypeError as error:
            raise ResultMissing(message) from error
        except asyncio.TimeoutError as error:
            raise ResultTimeout(message) from error

        return self.unwrap_result(self.encoder.decode(data))

    async def async_get_result(self, message_key: str, block: bool):
        """Runs the operation that retrieves the message from the backend

        Args:
            message_key (str): the message
            block (bool): Whether to block or not

        Returns:
            data: The data that was stored in postgres is none if the message has expired
        """

        # If block is True then listen for a notification from postgres
        # that has the same message_key as the one that we are trying
        # to retrieve, allowing dynamic message fetching
        if block:
            try:
                return await self.postgres_select(message_key)
            except TypeError:
                future = asyncio.Future()

                while not (
                    future.done()
                    and not future.cancelled()
                    and future.exception() is None
                ):
                    async for notify in self.gen:
                        if notify.payload == message_key:
                            future.set_result(True)
                            break

                await future

        return await self.postgres_select(message_key)

    async def postgres_select(self, message_key: str):

        exe = await self.connection.execute(
            SQL("SELECT result, expires_at FROM {} WHERE message_key=%s").format(
                Identifier(self.namespace)
            ),
            (message_key,),
        )
        all_data = await exe.fetchone()

        data = all_data[0]

        # check if the message has expired or not
        time_check = all_data[1]
        if time_check:
            if time_check < datetime.datetime.now().astimezone():
                data = None
        return data

    def _store(self, message_key: str, result: Result, ttl: int):
        """Stores the message inside postgres

        Args:
            message_key (str): The message_key which is used as a primary key
            result (Result): The result of the task
            ttl (int): Time to live
        """

        async def async_store(message_key: str, result: Result, ttl: int):
            expires_at = datetime.datetime.now().astimezone() + datetime.timedelta(
                milliseconds=ttl
            )
            await self.connection.execute(
                SQL(
                    "INSERT INTO {} (message_key, result, expires_at) VALUES (%s, %s, %s)"
                ).format(Identifier(self.namespace)),
                (
                    message_key,
                    self.encoder.encode(result),
                    expires_at,
                ),
            )
            await self.connection.execute(
                "SELECT pg_notify(%s, %s)", ["dramatiq", message_key]
            )

        loop = asyncio.new_event_loop()
        loop.run_until_complete(async_store(message_key, result, ttl))
        loop.close()
