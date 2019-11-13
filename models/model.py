import abc
from operators.operator import Operator


class Model(Operator):
    @classmethod
    @abc.abstractmethod
    def cache(cls, date, code_list=None):
        pass
