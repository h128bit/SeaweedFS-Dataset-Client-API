# SeaweedFS-Dataset-Client-API documentation


## Client
```python
class: SeaweedFSDatasetClient.client.SeaweedFSDataClient(filer_url: str, root: str="data/datasets", max_active_taks: int=100):
```
- filer_url: url for SeaweedFS Filer
- root: client work directory, create in filer root automatically. By default: "data/datasets"
- max_active_taks: max active asynchronys task per time. By default: 100 task

### Methods

#### General

```python
listdir(file_location: str|Path, get_all: bool=True, raw: bool=False, last_name: str|None=None)
```
Method for getting list of files in specified folder. Support using pagination, if get_all set to Fasle, return first 1000 files in folder.
- file_location: remote path to folder
- get_all: if set to True return all files in folder else, return first 1000 files. By default: True
- raw: if set to True, then return metadata of files. By default: False
- last_name: name of last file in returned list, using for pagination. By default: None

**return:**
list of string or list of dictionary if <code>raw</code> set to True.


```python
mkdirs(file_location: str|Path)
```
Create folder in remote storage. If was passed chain of path? then will created all folders

- file_location: path to create folder


```python
exists(file_location: str|Path)
```
Method for checking existiong file or folder

- file_location: location file to check in remote storage

**return:**
True if exists, Fasle else.


```python
remove(file_location: str|Path, recursive: bool=False)
```
Method for remove file or folder

- file_location: path to file for remove
- recursive: Flag allowed remove directories. By default: False

**return:**
True if was removed else Fasle


```python
get_remote_dir_structure(remote_dir: str|Path)
```
Method return full paths all files in passed folder. Ignore empty folders. 

- remote_dir: path to target directory in remote storage 

**return:**
Iterator like.

Example
~~~python
res = client.get_remote_dir_structure("remote/path")
for folder_with_files, full_path_to_files in res:
    print(f"folder {folder_with_files} containce these files")
    print(*list(full_path_to_files), sep="\n")

# folder remote/path/images containce these files
# image1.png
# image2.png
# ...
~~~


#### Upload to storage

```python
push(files: bytes|str|Path|Sequence[Union[bytes, str, Path]], file_location: str|Path, file_names: str|Path|Sequence[Union[str, Path]]|None=None)
```
Method to upload files to remote storage. Before upload files make sure what target folder is exists. 
Don't created folders when upload files. Can upload files as bytes or as paths to locally files.
If files passed bytes when need pass names for each file. If files passed like paths when names will same like locally.
Used asinchronus http requests if passed list of files.

- files: file or files to upload. Can be one file or list of files.
- file_location: location on remote storage to upload files. Make sure what target folder is exists
- file_names: name or list names for files if files was passed like bytes. By default: None

**return:**
Upload file status if passed one file or list of status if passed list of files


```python
push_folder(path_to_folder: str|Path, remote_folder_name: str|None=None)
```

Method for upload files from local directory save folder structure. 
Used asinchronus http requests for upload files.
Ignore empty folders.

- path_to_folder: path to local folder
- remote_folder_name: path to remote folder. If remote folder name not passed when will used folder name from 
path_to_folder parametr. By default: None


#### Download from storage

```python
pull(files: str|Path|Sequence[Union[str, Path]], raise_if_not_200: bool=True)
```
Method for download files from remote storage.
Used asinchronus http requests if passed list of files.

- files: path to file in remote storage or list of paths
- raise_if_not_200: flag, if set to True, when will raise exception if file download with not 200 status. By default: True

**return:**
Bytes if passed one file or list of bytes if passed list of files


```python
pull_folder(file_location: str|Path, local_path: str|Path)
```
The same what method push_folder, but for download files from remote storage to locl folder.
Used asinchronus http requests for download files.
Ignore empty folders.

- file_location: path to remote folder
- local_path: path to local folder to save files




