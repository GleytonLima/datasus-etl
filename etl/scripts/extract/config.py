import dataclasses
import json
import os

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


@dataclasses.dataclass
class Config:
    urls: dict
    system_config: dict
    ufs: list
    anos: list
    meses: list


@dataclasses.dataclass
class LoadConfig:
    config: Config

    def __init__(self):
        self.config = self.load_config()

    def load_config(self):
        with open(os.path.join(BASE_PATH, "config.json")) as json_file:
            config = Config(**json.load(json_file))
        return config

    def get_config(self):
        return self.config


if __name__ == "__main__":
    config = LoadConfig()
    print(config.get_config().meses)
    print(config.get_config().anos)
