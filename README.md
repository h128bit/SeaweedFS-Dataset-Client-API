# SeaweedFS-Dataset-Client-API

## Simple python SeaweedFS client for remote acesst and control to you datasets

This library provides an opportunity to upload you datasets to remote SeaweedFS server, saved folder structure in you dataset.
After upload to remote SeaweedFS server you can load data to you compute node from dataset by batch unnecessarily to save all data on compute node.
If you need you also can download all dataset to locally, saved folder structure in you remote dataset.

## Usage example 

SeaweedFS-Dataset-Client-API accesses Filer and uses http for interaction.
By default client create <code>data/datasets</code> folder in root and load all data to there.
For load more then one file client will use asynchronys operation.
For more see DOCS.md file.

### Import 

~~~python
from SeaweedFSDatasetClient.client import SeaweedFSDataClient

seaweedfs_filer_url = ...
client = SeaweedFSDataClient(filer_url=seaweedfs_filer_url)

~~~

### Push files to remote storage
~~~python

# load file to remote storage
# load one file

path_to_file = "path/to/file/example.txt"
path_to_remote_folder = "remote/folder/to_save"
status = client.push(path_to_files, path_to_remote_folder) # return int

# load more one file

paths_to_files = [
    "path/to/file.txt",
    "path/to/file.png",
    ...
]
path_to_remote_folder = "remote/folder/to_save"
status = client.push(path_to_files, path_to_remote_folder) # return list of int

# load folder to remote storage

path_to_local_folder = "local/folder/"
path_to_remote_folder = "remote/folder/"

status1 = client.push_folder(
    path_to_local_folder, 
    path_to_remote_folder
) # will ignore empty folders

~~~

### Load files from remote storage
~~~python
# download files from remote storage
# download one file 
remote_file = "path/to/remote/file/example.txt"

file = client.pull(remote_file) # return bytes

# download batch
remote_files = [
    "remote/file/exampe.txt",
    "remote/file/example.png",
    ...
]
files = client.pull(remote_file) # return list of bytes

# download folder 

remote_filder_to_download = "remote/folder"
locally_folder_to_save = "locally/folder"

client.pull_folder(
    remote_filder_to_download,
    locally_folder_to_save
)

~~~