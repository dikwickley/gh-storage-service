import abc
import os
import uuid
import shutil
from src.models import Meta


class RepoManger(abc.ABC):

    @abc.abstractmethod
    def create_repo(self, name: str):
        pass

    @abc.abstractmethod
    def save_chunk(self, repo_name: str, chunk_path: str):
        pass

    @abc.abstractmethod
    def upload(self, meta: Meta, chunk_dir: str):
        pass

    @abc.abstractmethod
    def download(self, meta: Meta, download_dir: str):
        pass


class RepoMangerFS(RepoManger):

    def __init__(self, root_folder: str) -> None:
        self.root_folder = root_folder

    def create_repo(self, name: str):
        os.makedirs(name)

    def save_chunk(self, chunk_path: str, repo_name: str):
        shutil.copy(chunk_path, repo_name)

    def upload(self, meta: Meta, chunk_dir: str) -> Meta:
        chunk_map = {}
        for chunk_name in meta.chunk_order:
            repo_name = str(uuid.uuid4())
            repo_path = os.path.join(self.root_folder, repo_name)
            chunk_path = os.path.join(chunk_dir, chunk_name)
            self.create_repo(repo_path)
            self.save_chunk(chunk_path, repo_path)
            chunk_map[chunk_name] = repo_path
        meta.chunk_map = chunk_map
        return meta

    def download(self, meta: Meta, download_dir: str):
        for chunk_name, repo_path in meta.chunk_map.items():
            remote_chunk_path = os.path.join(repo_path, chunk_name)
            shutil.copy(remote_chunk_path, download_dir)
