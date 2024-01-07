import json
import dataclasses
import typing as t


class Meta:
    def __init__(self,
                 original_file_name: str,
                 chunk_order: t.List[str],
                 chunk_map: t.Dict[str, str]):
        self.original_file_name = original_file_name
        self.chunk_order = chunk_order
        self.chunk_map = chunk_map

    def __init__(self, infile):
        with open(infile) as f:
            data = json.load(f)
        self.original_file_name = data['original_file_name']
        self.chunk_order = data['chunk_order']
        self.chunk_map = data['chunk_map']

    def save(self, outfile: str):
        with open(outfile, 'w') as f:
            json.dump(self.__dict__, f)
