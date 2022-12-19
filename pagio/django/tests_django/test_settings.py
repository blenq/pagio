SECRET_KEY = 'fake-key'
INSTALLED_APPS = [
    "tests_django",
]

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

DATABASES = {
    "default": {
        "ENGINE": "pagio.django",
        "NAME": "pagio_django",
    },
}
