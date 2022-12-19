from django.db.models import Value
from django.db.models.functions import Coalesce
from django.test import TransactionTestCase

from .models import TestModel



class ExpressionTest(TransactionTestCase):

    def test_coalesce(self):
        list(TestModel.objects.filter(
                value=Coalesce(Value(None), Value(None))))
        list(TestModel.objects.all().annotate(extra=Coalesce(Value('hi'), Value('hello'))))
