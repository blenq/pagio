Timezones
=========

PostgreSQL sessions have a related timezone. It can be set at startup when
making a connection or by executing the "SET TIMEZONE TO ..." statement. This
timezone is used for interpreting timestamptz input without timezone
information, like naive datetime parameters or literals without a timezone
specifier (name or offset).
It is also used for presenting output in textual format, such as query results,
for example when using the psql utility.

PostgreSQL does not actually store the timezone with the timestamptz value in
the database. Instead it converts textual input values to UTC, and uses those
internally. When requested by a client using text format, the values are
converted to the timezone of the client session again.

This process happens mostly transparent to the client, but the
behavior can lead in certain edge cases to unexpected results.
For example, a problem can arise when prepared statements are used in
combination with a timestamp **without** timezone and a session timezone
change.
To illustrate this:

::

    SET TIMEZONE TO 'Europe/Berlin';
    PREPARE test AS
        SELECT '2021-03-15 14:10:03'::timestamptz;

This creates a prepared statement with the name "test". Now execute the
statement, once unprepared and once prepared with a different timezone:

::

    SET TIMEZONE TO 'America/Chicago';
    SELECT '2021-03-15 14:10:03'::timestamptz;

          timestamptz
    ------------------------
    2021-03-15 14:10:03-05

    EXECUTE test;

          timestamptz
    ------------------------
    2021-03-15 08:10:03-05

The prepared statement returns a timestamp which differs six hours from the
return value of the unprepared, but identical, statement. This is not a bug
https://www.postgresql.org/message-id/888267.1668435625%40sss.pgh.pa.us
Those six hours are the difference between the two timezones. This happens
because the timestamp is converted to UTC during the preparation stage. This
UTC value is again converted to the new timezone when the statement is
executed.

The pagio library uses transparent statement caching by default and is prone to
these type of confusing results as well.

Conversions
-----------

The pagio library converts timestamps with timezone to Python datetimes with
the tzinfo set, if possible.

When the textual result format is used, it will
only parse values when the DateStyle parameter is set to ISO. The pagio library
sets this on startup, but it might be overridden by executing a
"SET DateStyle TO ..." statement. In the ISO format, the timezone is
represented as a fixed offset, e.g. "+02:00". This is translated to a Python
timezone with a fixed offset.

No timezone conversion by the server takes place, when the binary result format
is used. The UTC value is sent to the client. The pagio library will try
to convert the timestamp to the session timezone to make the results similar
when using either text or binary format. It will return a Python datetime value
with tzinfo set to the session timezone if it is a IANA timezone i.e.
recognized by the zoneinfo module.
Otherwise the tzinfo will be set to UTC.

When a PostgreSQL timestamptz is outside the Python datetime range, a string
will be returned. In case of the textual format, that is the original string,
and when it uses binary the value will be converted to a similar ISO string.

.. table:: Conversions

  +--------+---------------+-----------------+---------+-------------------------------------+
  | Format | ISO DateStyle | In Python range | IANA tz | Result                              |
  +========+===============+=================+=========+=====================================+
  | text   |     yes       |      yes        |         | datetime with fixed offset timezone |
  +--------+---------------+-----------------+---------+-------------------------------------+
  | text   |      no       |                 |         | original PostgreSQL text            |
  +--------+---------------+-----------------+---------+-------------------------------------+
  | text   |               |       no        |         | original PostgreSQL text            |
  +--------+---------------+-----------------+---------+-------------------------------------+
  | binary |               |      yes        |   yes   | datetime with (ZoneInfo) timezone   |
  +--------+---------------+-----------------+---------+-------------------------------------+
  | binary |               |      yes        |    no   | datetime with UTC timezone          |
  +--------+---------------+-----------------+---------+-------------------------------------+
  | binary |               |       no        |         | pagio generated ISO format text     |
  +--------+---------------+-----------------+---------+-------------------------------------+


A pagio connection is using statement caching by default, which can cause the
result format to change, it is possible for a query to return a slightly
modified version of a datetime after multiple executions.
For example a query might return a datetime "2022-01-01 12:00:00" with tzinfo
set to "+01:00" the first time, while later on it will return that same
datetime "2022-01-01 12:00:00", but now with tzinfo set to "Europe/Amsterdam".
Note that these dates are actually equal, have the same UTC offset, can be
compared with each other and look the same when converted to string. But beware
of subtle consequences when relying on a certain timezone.

Recommended practice
--------------------

NOTE: this part contains opinions.

First of all it is best to set the session timezone to UTC and not change it.
Instead convert the timestamps to the desired timezone in the presentation
layer of the application, e.g. when rendering a web page, displaying a window
or printing to stdout. And convert timestamps to UTC before the input, like web
form data, is sent to the database.

This has, among other, the following advantages:

- No conversions will be performed by PostgreSQL. Conversion processing
  is done by the, probably better scalable, application code.
- It is important only on the client(s) that the tzdata is up to date.
- Less confusing application code.

This advice is actually a good practice for many databases. Even if the
database is lacking timezone support, the UTC approach can be used.

Also try to adhere to the following guidelines,
even if converting to and from UTC is not feasible.

- Do not change the session timezone.
- Do not use naive timestamps if those are used as timestamptz values in the
  database.
  Always specify the timezone. Either by providing an aware datetime as
  parameter or by using a timezone specifier, like "+01:30" or
  " Europe/Berlin", with literals.
- Do not change the DateStyle to anything else than ISO.

Of course there are use cases where these recommendations do not apply.
When raw values are used and presented directly to the user, like
psql and pgAdmin do, it does make sense to use a timezone other than UTC, and
maybe use another DateStyle.
