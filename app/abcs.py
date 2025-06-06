from abc import ABC, abstractmethod
from .utils import camel_to_snake

class DataProduct(ABC):

    @abstractmethod
    def handle(self, event):
        pass

    @property
    def name(self):
        return camel_to_snake(self.__class__.__name__)

class DataModel(ABC):

    @property
    def name(self):
        return camel_to_snake(self.__class__.__name__)
    