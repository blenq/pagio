from setuptools import setup, Extension


pagio_ext = Extension(
    'pagio._pagio',
    sources=[
        'pagio/extension/protocol.c',
        'pagio/extension/stmt.c',
        'pagio/extension/numeric.c',
        'pagio/extension/utils.c',
        'pagio/extension/field_info.c',
    ],
    depends=[
        'pagio/extension/pagio.h',
        'pagio/extension/portable_endian.h',
        'pagio/extension/protocol.h',
        'pagio/extension/stmt.h',
        'pagio/extension/numeric.h',
        'pagio/extension/utils.h',
        'pagio/extension/field_info.h',
    ],
)


setup(name='pagio',
      version='0.1',
      description='PostgreSQL client library',
      ext_modules=[pagio_ext],
      packages=['pagio'])
