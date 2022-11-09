# Pagio

Pagio is a PostgreSQL client library for Python. It contains both a synchronous
and an asyncio version of the API based on the same codebase.

## Synchronous example

```python
from pagio import Connection

conn = Connection(
    host=<host>, port=<port>, database=<database>, user=<user>, password=<pwd>)
result = conn.execute("SELECT * FROM pg_type")
for row in result:
    print(row)
```

## Asynchronous example

```python
import asyncio
from pagio import AsyncConnection

async def main():
    conn = await AsyncConnection(
        host=<host>, port=<port>, database=<database>, user=<user>, password=<pwd>)
    result = await conn.execute("SELECT * FROM pg_type")
    for row in result:
        print(row)

asyncio.run(main())
```

## Parameters

It uses native PostgreSQL parameters.

```python
>>> import pagio
>>> conn = pagio.Connection()
>>> res = conn.execute("SELECT $1, $2", 3, "hello")
>>> print(res.rows[0])
(3, 'hello')
```

## Performance

The library contains both a pure python and a C accelerated version of the
internal Protocol class. The C accelerated version matches the speed of 
asyncpg, one of the fastest PostgreSQL client libs available. 
(In my own private test environment, no hard benchmarks yet),
