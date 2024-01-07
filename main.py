import os
from src.data_manager import DataManagerSubprocess
from src.repo_manger import RepoMangerFS
from src.models import Meta
from src.github_repo_manager import RepoMangerGithub

data_manager = DataManagerSubprocess()
# repo_manager = RepoMangerFS("data/repo-fs")
repo_manager = RepoMangerGithub("personal_access_token_here")

chunk_dir = "data/chunked-data"
download_dir = "data/downloaded-data"
meta_dir = "data/meta-data"
meta_file = os.path.join(meta_dir, "rick-roll.json")

# chunk the file
meta_data = data_manager.chunk_file(
    "data/rick-roll.mp4", chunk_dir=chunk_dir, chunk_size_mb=5)

# upload chunks
repo_manager.upload(meta_data, chunk_dir=chunk_dir)

# save meta data
meta_data.save(meta_file)

# load meta data from file
meta_data = Meta(infile=meta_file)

# download chunks in different location
repo_manager.download(meta_data, download_dir=download_dir)

# rechunk the data
data_manager.rechunk_file(meta_data, chunk_dir=download_dir)
