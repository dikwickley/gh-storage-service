import json
import dataclasses
import typing as t


@dataclasses.dataclass
class Meta:
    original_file_name: str
    chunk_order: t.List[str]
    chunk_map: t.Dict[str, str]

    def save(self, outfile: str):
        with open(outfile, 'w') as f:
            json.dump(self.__dict__, f)
