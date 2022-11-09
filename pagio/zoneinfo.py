""" ZoneInfo with fallback """

import sys

if sys.version_info < (3, 9):
    from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError
else:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

__all__ = ["ZoneInfo", "ZoneInfoNotFoundError"]
