.. Pagio documentation master file, created by
   sphinx-quickstart on Mon Nov 28 11:13:53 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pagio
=====

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: Contents:

   installation
   usage/index
   reference/index
   timezone
   stmt_caching

Pagio is a PostgreSQL client library for Python. It features both a synchronous
and a asyncio version of the API.

At the moment it is in an alpha stage. Should you use it? Probably not yet,
but if you want to give it a try, you are very welcome.

Notable features:

* Good performance
* Native PostgreSQL parameters ($1, $2, ...)
* Includes both a pure python implementation and an accelerated version with
  performance critical processing implemented in a C extension.
* Supports the :external+py3:py:class:`asyncio.ProactorEventLoop`, for best
  compatibility with Windows.

Index
-----

* :ref:`genindex`


