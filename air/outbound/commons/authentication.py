from abc import ABC, abstractmethod


class AuthenticationStrategy(ABC):
    @abstractmethod
    def get_connection_props_list(self) -> dict:
        pass

    @abstractmethod
    def get_connection_props_dict(self) -> dict:
        pass


class SimpleAuth(AuthenticationStrategy):
    def __init__(self, user: str, password: str):
        self.user = user
        self.password = password

    def get_connection_props_list(self) -> dict:
        return [self.user, self.password]

    def get_connection_props_dict(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "encrypt": "true",
            "trustServerCertificate": "true"
        }
