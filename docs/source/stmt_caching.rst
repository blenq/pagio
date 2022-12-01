.. _Statement caching:

Statement caching
=================

First of all, statement caching can misbehave with external connection pools.
If you encounter such problems, set the
:py:attr:`prepare_threshold <pagio.base_connection.BaseConnection>` to 0, to disable
statement caching.

PostgreSQL supports the use of prepared statements. A prepared statements can
be useful for performance reasons when it is executed multiple times. Such a
statement is tied to the client connection and not visible to others.
Because of this and the relatively large overhead of starting a new client
connection, it is good practice to use long living database connections.

There are
two versions of prepared statements. The first one is a statement that is
defined using the `PREPARE syntax
<https://www.postgresql.org/docs/current/sql-prepare.html>`_.

The second version is a `protocol level prepared statement
<https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY>`_.
This is implemented for example by the `PQprepare function
<https://www.postgresql.org/docs/15/libpq-exec.html#id-1.7.3.10.3.7.1.1.1.2>`_
in libpq.

Except for the creation method, these prepared statements are the same.
A `view
<https://www.postgresql.org/docs/current/view-pg-prepared-statements.html>`_
exists to inspect all prepared statements for the current session.

The pagio library also implements protocol level statement preparation. Not
explicitly, like libpq, but transparently when a threshold is reached for the
number of executions of a particular statement. The prepared statement is
used on subsequent requests.
This speeds up the process for two reasons:

- There is less protocol traffic and processing necessary for the client.
  It doesn't request nor receive metadata (like number of columns, and data
  types).
- PostgreSQL does not need to parse the statement.

This happens all transparently from the caller's perspective, but a bit of
insight in the inner workings might be useful.

A pagio connection maintains a cache of statements, with a default size of 100.
When a statement is executed successfully it is inserted or moved up to the
most recently used spot in the cache.
If a statement is executed successfully the number of
times the prepare_threshold is set to, then it will be prepared with a name
next time around. So setting the prepare_threshold to 1 will have created a
server side prepared statement when it is executed successfully twice. Using a
name, causes PostgreSQL to keep a reference to it, until the statement is
explicitly closed by the client.

The statements are identified by the combination of the SQL statement and the
input parameter database types. A statement text like "SELECT $1" with an
integer parameter represents a separate cache item from the same text with a
float parameter value.

When the cache is full, the least recent item is removed from the cache. If
that statement is prepared on the server it is marked for closure by the
pagio library. The actual
Close command will be sent to the server, when a following query statement is
executed, to prevent multiple roundtrips.

If PostgreSQL returns an error when executing a prepared statement, then the
statement will be marked for closure as well.


Things to keep in mind
----------------------

Statement preparation needs the Extended Query protocol, which does not support
command texts with multiple statements. Multi statement command texts will be
executed with the Simple Query protocol and will never be prepared or cached.

When a statement is executed for the first time, without parameters and with
the result format not set, pagio will use the Simple
Query protocol. This is necessary to accommodate for a possible multi-statement
command text. The Simple Query protocol supports only TEXT result format.
When a single statement is executed multiple times, it
will start using the Extended Query protocol. The Extended Query protocol
will set the result format to BINARY by default if not explicitly set otherwise
by the caller, for performance reasons.
This is not a problem if for both formats a converter is in place.
If you are writing a custom converter, whenever that is actually possible, you
must implement therefore both a text converter and a binary converter.

Besides automatic closure on cache expunge or error,
prepared statements can also be closed explicitly using a `DEALLOCATE
<https://www.postgresql.org/docs/current/sql-deallocate.html>`_ statement.
A `DISCARD ALL
<https://www.postgresql.org/docs/current/sql-discard.html>`_
statement will clear all statements on the server as well.
The pagio library tries to keep track of such deallocation statements, to keep
the server and client cache in sync,
but if those statements are executed from a user function or similar it will
not notice that a statement does not exist on the server anymore. When the
statement is executed again it will fail with an error. If the failing
statement is not running in a transaction, then the
:py:class:`Pagio Connection <pagio.base_connection.BaseConnection>` will
recognize this error and recover by just executing the statement again.

Another thing to keep in mind is the possible expiration of the statement due
to a schema change.
A statement like "SELECT * FROM table", might have different result columns or
types when a column is added to or removed from the table.
PostgreSQL will actually
notice this and not even try to return anything, but set an error instead.
This error, like any other error set by the server, causes pagio to mark the
statement for closure and the statement can be safely re-executed.
The library recognizes this particular error and if there is no current
transaction, the statement will be re-executed automatically without the caller
ever noticing. If there is a transaction in progress, then the library can not
recover from the error and the exception will be propagated to the caller.

So be careful with running processes while making schema changes to the
database. It does not mean that schema changes can not be performed while
there are connected pagio clients present. It depends on the queries and the
actual changes. For example, if SELECT statements never use '*' or don't run
in a transaction, columns can be added freely.
If errors do occur, they should be gone when statements are re-executed. So
calling code that survives an error, for example a web app, should recover
after a few hiccups. If that is not acceptable, either disable statement
caching by setting the prepare_threshold to 0 or restart the client
connections.
