from src.data_manager import DataManagerSubprocess
from src.repo_manger import RepoMangerFS

data_manager = DataManagerSubprocess()
repo_manager = RepoMangerFS("data/repo-fs")

chunk_dir = "data/chunked-data"
download_dir = "data/downloaded-data"

meta_data = data_manager.chunk_file(
    "data/a_very_large_file.txt", chunk_dir=chunk_dir, chunk_size_mb=10)

repo_manager.upload(meta_data, chunk_dir=chunk_dir)

repo_manager.download(meta_data, download_dir=download_dir)

data_manager.rechunk_file(meta_data, chunk_dir=download_dir)

meta_data.save("test.json")
