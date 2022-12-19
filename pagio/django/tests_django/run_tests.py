#!/usr/bin/env python
import os
import sys

import django
from django.conf import settings
from django.test.utils import get_runner


def setup_django():
    os.environ['DJANGO_SETTINGS_MODULE'] = 'tests_django.test_settings'
    django.setup()


if __name__ == "__main__":
    setup_django()
    TestRunner = get_runner(settings)
    test_runner = TestRunner()
    failures = test_runner.run_tests(["tests_django"])
    sys.exit(bool(failures))
