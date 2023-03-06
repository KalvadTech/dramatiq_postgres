# dramatiq_postgres

A [Dramatiq] results backend that uses [postgres] to store results.

This results backend uses [psycopg].

## Usage

``` python
import dramatiq
from dramatiq.brokers.stub import StubBroker
from dramatiq.results import Results

from dramatiq_postgres import PostgresBackend
broker = StubBroker()
dramatiq.set_broker(broker)

pg_backend = PostgresBackend(
    connection_params={
        "user": "postgres",
        "password": "",
        "host": "localhost",
        "port": 5432,
        "dbname": "dramatiq_results",
    },
)

broker.add_middleware(Results(backend=pg_backend))
```

[Dramatiq]: https://dramatiq.io
[postgres]: https://www.postgresql.org/
[psycopg]: https://www.psycopg.org/