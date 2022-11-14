Timezones
=========

PostgreSQL sessions have a related timezone. It can be set at startup when
making a connection or by executing the "SET TIMEZONE TO ..." statement. This
timezone is used for interpreting timestamptz input without timezone
information, like naive datetime parameters or literals without a timezone
specifier (name or offset).
It is also used for presenting output, such as query results, but only when the
textual format is in use, for example when using the psql utility.

PostgreSQL does not actually store the timezone with the timestamptz value in
the database. Instead it converts input values to UTC, and uses those
internally. When requested by a client, the values are converted to the the
timezone of the client session again.

This process happens mostly transparent to the client, but sometimes the
behavior can lead to unsuspected results. For example, a problem can arise
when prepared statements are used and the session timezone is changed. To
illustrate this:

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
return value of the unprepared, but identical, statement.
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
and when it uses binary the value will be converted to an ISO string.

.. table:: Conversions

  +--------+---------------+--------------+---------+-------------------------------------+
  | Format | ISO DateStyle | Python range | IANA tz | Result                              |
  +========+===============+==============+=========+=====================================+
  | text   |     yes       |      yes     |         | datetime with fixed offset timezone |
  +--------+---------------+--------------+---------+-------------------------------------+
  | text   |      no       |              |         | original PostgreSQL text            |
  +--------+---------------+--------------+---------+-------------------------------------+
  | text   |               |       no     |         | original PostgreSQL text            |
  +--------+---------------+--------------+---------+-------------------------------------+
  | binary |               |      yes     |   yes   | datetime with (ZoneInfo) timezone   |
  +--------+---------------+--------------+---------+-------------------------------------+
  | binary |               |      yes     |    no   | datetime with utc timezone          |
  +--------+---------------+--------------+---------+-------------------------------------+
  | binary |               |       no     |         | pagio generated iso format text     |
  +--------+---------------+--------------+---------+-------------------------------------+


Because pagio is using statement caching by default, which can cause the result
format to change, it is possible for a query to return a slightly modified
version of a datetime after multiple executions. For example a query might
return a datetime "2022-01-01 12:00:00" with tzinfo set to "+01:00" the first
time, while later on it will return that same datetime "2022-01-01 12:00:00",
but now with tzinfo set to "Europe/Amsterdam". Note that these dates are
actually equal, have the same UTC offset, can be compared with each other and
look the same when converted to string. But beware of subtle consequences when
relying on a certain timezone.

Recommended practice
--------------------

First of all it is best to set the session timezone to UTC and not change it.
In that case, no conversions will be performed by PostgreSQL. Instead convert
the timestamps to the desired timezone in the presentation layer of the
application, e.g. when rendering a web page, displaying a window or
printing to stdout. And convert timestamps to UTC before the input, like web
form data, is sent to the database.

This advice is actually a good practice for many databases. Even if the
database is lacking timezone support, this approach can be used.

If converting to and from UTC is not feasible, at least stick to the following:

- Do not change the session timezone
- Do not use naive timestamps if those are used as timestamptz values. Always
  specify the timezone. Either by providing an aware datetime as parameter or
  by using a timezone specifier, like "+01:30" or " Europe/Berlin", with
  literals.
- Do not change the DateStyle to anything else than ISO

