#!/usr/bin/env python

from setuptools import setup, Extension


pagio_ext = Extension(
    'pagio._pagio',
    sources=[
        'pagio/extension/protocol.c',
        'pagio/extension/stmt.c',
        'pagio/extension/numeric.c',
        'pagio/extension/utils.c',
        'pagio/extension/field_info.c',
        'pagio/extension/network.c',
        'pagio/extension/text.c',
        'pagio/extension/uuid.c',
        'pagio/extension/datetime.c',
        'pagio/extension/json.c',
        'pagio/extension/complex.c',
    ],
    depends=[
        'pagio/extension/pagio.h',
        'pagio/extension/portable_endian.h',
        'pagio/extension/protocol.h',
        'pagio/extension/stmt.h',
        'pagio/extension/numeric.h',
        'pagio/extension/utils.h',
        'pagio/extension/field_info.h',
        'pagio/extension/network.h',
        'pagio/extension/text.h',
        'pagio/extension/uuid.h',
        'pagio/extension/datetime.h',
        'pagio/extension/json.h',
        'pagio/extension/complex.h',
    ],
    optional=True,
)


setup(ext_modules=[pagio_ext])
