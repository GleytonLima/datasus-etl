import dataclasses
import json
import os

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


@dataclasses.dataclass
class LoadConfig:
    def __init__(self):
        self.config = self.load_config()

    def load_config(self):
        with open(os.path.join(BASE_PATH, 'config.json')) as json_file:
            config = json.load(json_file)
        return config

    def get_config(self):
        return self.config


if __name__ == '__main__':
    config = LoadConfig()
    print(config.get_config())
