import json
from pprint import pformat
from munch import Munch


class EditorConfig(Munch):
    def __str__(self):
        return pformat(self.__dict__, width=120)

    @classmethod
    def load(cls, filename: str):
        with open(filename) as fp:
            return cls.fromDict(json.load(fp))


if __name__ == '__main__':
    cfg = EditorConfig.load('editorconfig.json')
    print(cfg)
