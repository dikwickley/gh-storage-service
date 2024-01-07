import os
import requests
import uuid
from github import Github, Auth, Repository
from src.models import Meta
from src.repo_manger import RepoManger


class RepoMangerGithub(RepoManger):

    def __init__(self, token: str) -> None:
        auth = Auth.Token(token)
        self.g = Github(auth=auth)
        self.user = self.g.get_user()

    def create_repo(self, name: str) -> Repository:
        repo = self.user.create_repo(name)
        return repo

    def save_chunk(self, repo_name: str, chunk_path: str):
        with open(chunk_path, 'rb') as file:
            data = file.read()
        repo = self.user.get_repo(repo_name)
        chunk_name = os.path.basename(chunk_path)
        repo.create_file(
            chunk_name, f'saving chunk {chunk_name}', data, branch='main')

    def get_repo_full_path(self, repo_name):
        return self.user.get_repo(repo_name).full_name

    def upload(self, meta: Meta, chunk_dir: str):
        chunk_map = {}
        for chunk_name in meta.chunk_order:
            chunk_path = os.path.join(chunk_dir, chunk_name)
            repo_name = str(uuid.uuid4())
            self.create_repo(repo_name)
            self.save_chunk(repo_name, chunk_path)
            chunk_map[chunk_name] = self.get_repo_full_path(repo_name)
        meta.chunk_map = chunk_map
        return meta

    def download_file(self, url, destination_dir):
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                filename = url.split("/")[-1]

                destination_path = os.path.join(destination_dir, filename)

                with open(destination_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=128):
                        file.write(chunk)
            else:
                print(
                    f"Failed to download file. Status code: {response.status_code}")
        except Exception as e:
            print(f"An error occurred: {e}")

    def download(self, meta: Meta, download_dir: str):
        for chunk_name, repo_path in meta.chunk_map.items():
            downloaded_chunk_path = os.path.join(download_dir, chunk_name)
            repo = self.g.get_repo(repo_path)
            contents = repo.get_contents(chunk_name)
            download_url = contents.download_url
            self.download_file(download_url, download_dir)
