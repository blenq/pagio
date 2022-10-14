from setuptools import setup, Extension


pagio_ext = Extension(
    'pagio._pagio',
    sources=['pagio/extension/protocol.c'],
    depends=['pagio/extension/pagio.h'],
)


setup(name='pagio',
      version='0.1',
      description='PostgreSQL client library',
      ext_modules=[pagio_ext],
      packages=['pagio'])
