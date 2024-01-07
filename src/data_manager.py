import abc
import uuid
import os
import json
from src.models import Meta


class DataManager(abc.ABC):

    @abc.abstractmethod
    def chunk_file(self, file_path: str, chunk_dir: str, chunk_size_mb: int = 45) -> None:
        """Chunks a given file into multiple chunks of max size `chunk_size_mb`."""
        pass

    @abc.abstractmethod
    def rechunk_file(self, output_file: str, chunk_dir: str) -> None:
        pass


class DataManagerSubprocess(DataManager):

    def chunk_file(self, file_path: str, chunk_dir: str, chunk_size_mb: int = 45) -> Meta:
        file_name = os.path.basename(file_path)
        chunk_size = 1024*1024*chunk_size_mb
        chunk_prefix = str(uuid.uuid4())
        chunk_order = []

        with open(file_path, 'rb') as file:
            chunk_number = 0
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                output_file = f"{chunk_dir}/{chunk_prefix}_{chunk_number}"
                chunk_name = os.path.basename(output_file)
                with open(output_file, 'wb') as output:
                    output.write(chunk)
                chunk_order.append(chunk_name)
                chunk_number += 1

        return Meta(
            original_file_name=file_name,
            chunk_order=chunk_order,
            chunk_map=[]
        )

    def rechunk_file(self, meta: Meta, chunk_dir: str) -> None:
        output_file = meta.original_file_name
        with open(output_file, 'wb') as output:
            for chunk in meta.chunk_order:
                chunk_file_path = f"{chunk_dir}/{chunk}"
                try:
                    with open(chunk_file_path, 'rb') as chunk_file:
                        chunk = chunk_file.read()
                        if not chunk:
                            break
                        output.write(chunk)
                except FileNotFoundError:
                    break
