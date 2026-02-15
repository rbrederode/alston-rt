from abc import ABC, abstractmethod

class UIDriver(ABC):

    @abstractmethod
    def publish(self, model_dict: dict) -> None:
        pass