Connecting to the database
==========================

Making a connection to the PostgreSQL server is accomplished by instantiating
a :py:class:`Connection <pagio.Connection>` object.

::

    from pagio import Connection

    conn = Connection(
        host="localhost", database="mydb", user="user", password="password")

The asyncio version needs to be awaited to actually make the connection.

::

    import asyncio
    from pagio import AsyncConnection

    async def main():
        conn = await AsyncConnection(
            host="localhost", database="mydb", user="user", password="pwd")
        ...

    asyncio.run(main())


None of the arguments are required. The ones listed here can also be provided
by setting a corresponding environment variable. And other fallbacks might
apply if that is empty as well. For example if the "database"
argument is empty, it will look for an environment variable "PGDATABASE". If
that is also not set, it will use the username as fallback.

If the host is not provided, pagio will try to connect to the local machine.
If Unix sockets are supported on the platform, it will first look for a
matching socket path in /var/run/postgresql and /tmp. If the Unix domain
socket is not found or available, a TCP connection is attempted to 'localhost'.

The use of Unix domain sockets can be enforced by providing the socket
directory as an absolute path, for example "/var/run/postgresql". It must be
an absolute path, because pagio just looks at the first character.


Authentication
--------------

Pagio supports the traditional PostgreSQL MD5 and SASL with SCRAM-SHA-256
`authentication mechanisms`_.

Note the following edge case. The server stores user name and password in the
encoding that was in use when the user was created or altered. Pagio uses UTF-8
for everything and will encode a user or password :external+py3:py:class:`str`
using UTF-8.
If that does not
correspond with the actual binary string on the server, authentication will
fail. Provide the user or password argument as a :external+py3:py:class:`bytes`
value instead in such a case, and pagio will use it unchanged in the
authentication exchange.


SSL encryption
--------------

Pagio supports SSL encryption. The :py:class:`ssl_mode <pagio.SSLMode>`
parameter of the
:py:class:`Connection <pagio.base_connection.BaseConnection>`
determines if SSL is actually used. The
:py:attr:`default <pagio.SSLMode.DEFAULT>` behavior is that for Unix
domain sockets, SSL is only used when the server requires it, and for TCP
sockets, SSL is used unless the server rejects it.

The ssl_context parameter, which is a
standard Python :external+py3:py:class:`SSLContext <ssl.SSLContext>`,
influences the type of validation that takes place.
The default SSLContext that is
used if none is provided, performs no validation at all, similar to the
standard PostgreSQL tools, like psql and libpq. If check_hostname is set to
True in the SSLContext, and the hostname to match against is not the same
as the host parameter argument of the Connection, make sure to provide the
valid value as the server_hostname parameter.

Current code for the default SSLContext:

::

    ssl = SSLContext(PROTOCOL_TLS_CLIENT)
    ssl.check_hostname = False
    ssl.verify_mode = VerifyMode.CERT_NONE

.. _authentication mechanisms: https://www.postgresql.org/docs/current/auth-password.html