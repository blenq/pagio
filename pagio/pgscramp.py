""" PG specific SASL client """

from contextlib import contextmanager
from typing import Union, List, Tuple, Optional, Callable, Any, Generator

import scramp


class _FakePassword(str):
    """ Placeholder class used for patched version """


_fake_password = _FakePassword()


class PGScrampClient:
    """ SCRAM client for PostgreSQL including PG oddities """

    # SASL authentication standard says that the username and password need
    # to be Unicode (so it can be serialized using UTF-8) and must be prepared
    # using the saslprep algorithm, which prohibits the use of some characters.
    #
    # PostgreSQL uses arbitrary binary string for both username and password,
    # depending on the actual client encoding being used when creating or
    # altering the user, without storing information about the actual text
    # encoding. So it can happen that a username or password do not fit the
    # formal requirements for SASL authentication.
    #
    # PostgreSQL actually does not care about the username here. It is already
    # sent in the original Startup message. Whatever is used here, is not
    # taken into consideration. Therefore, it uses the string "user" which is
    # always safe.
    # Furthermore, PostgreSQL allows the use of the raw binary value if it
    # can't be prepared using the prescribed algorithm. This is done only when
    # the password can not be prepped.
    #
    # This implementation tries to use the standard implementation. It can fail
    # for two reasons.
    # * The password is not a UTF-8 valid binary string
    # * The UTF-8 valid password contains illegal characters for the saslprep
    #   algorithm
    #
    # The first condition is detected in the __init__. The second condition is
    # discovered in "get_client_final". In those cases, the original scramp
    # implementation is monkey patched (yes... ugly) to cope with raw binary
    # values.
    #
    # Note: This is no criticism on the scramp library being used. It follows
    # standards as it should. PostgreSQL needs to deal with legacy and I am too
    # lazy to build a special SASL client myself, when a fully functional
    # lib already exists.
    # That's why this library needs this ugly monkey patching, but it is only
    # activated when the particular edge cases are encountered.
    #
    # See: https://www.postgresql.org/docs/current/sasl-authentication.html

    def __init__(
            self,
            mechanisms: Union[List[str], Tuple[str]],
            password: bytes,
            channel_binding: Optional[Tuple[str, bytes]]):
        self._password = password
        try:
            password_str = password.decode()
        except UnicodeError:
            # Password is not valid UTF-8, patching is necessary later on.
            password_str = _fake_password
        self._client = scramp.ScramClient(
            mechanisms, "user", password_str, channel_binding)
        self._server_first_msg = ""

    @property
    def mechanism_name(self) -> str:
        """ Name of mechanism in use """
        return self._client.mechanism_name

    def get_client_first(self) -> str:
        """ Gets the client first message """
        return self._client.get_client_first()

    def set_server_first(self, message: str) -> None:
        """ Set the server first message """
        self._server_first_msg = message
        self._client.set_server_first(message)

    def _make_salted_pwd(
            self,
            hf: Callable[[], Any],  # pylint: disable=invalid-name
            password: str,
            salt: bytes,
            iterations: int,
    ) -> str:
        # Patched version of _make_salted_password. It uses the raw binary
        # password value if it is marked as a FakePassword, which happens when
        # the regular prep algorithm can not be applied.
        # For other values, existing functionality is not changed, so it can be
        # used in multithreaded systems where other SASL authentication
        # dialogues take place.
        if password is _fake_password:
            pwd = self._password
        else:
            pwd = scramp.core.saslprep(password).encode()
        return scramp.utils.hi(hf, pwd, salt, iterations)

    @contextmanager
    def _patch_make_pwd(self) -> Generator[None, None, None]:
        """ Monkey patch scramp library """

        # pylint: disable-next=protected-access
        old_make_pwd = scramp.core._make_salted_password
        # pylint: disable-next=protected-access
        scramp.core._make_salted_password = self._make_salted_pwd
        try:
            # Patch in place, yield control to caller
            yield None
        finally:
            # Done, revert monkey patch
            # pylint: disable-next=protected-access
            scramp.core._make_salted_password = old_make_pwd

    def get_client_final(self) -> str:
        """ Gets the final client message. """

        if self._client.password is not _fake_password:
            # password is valid UTF-8, try to use it
            try:
                return self._client.get_client_final()
            except scramp.ScramException as ex:
                if ex.server_error != "invalid-encoding":
                    # Something else went wrong, just raise
                    raise
                # Maybe caused by illegal characters for saslprep in
                # password. Retry with patched version.
                # Current client can not be used again, so reset the client and
                # replay auth dialog.
                self._client = scramp.ScramClient(
                    [self._client.mechanism_name], "user", _fake_password,
                    self._client.channel_binding, self._client.c_nonce)
                self._client.get_client_first()
                self._client.set_server_first(self._server_first_msg)

        with self._patch_make_pwd():
            # get the message using the patched variant where the raw password
            # version is used
            return self._client.get_client_final()

    def set_server_final(self, message: str) -> None:
        """ Sets final server message """
        self._client.set_server_final(message)
