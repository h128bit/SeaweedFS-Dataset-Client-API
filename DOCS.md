# SeaweedFS-Dataset-Client-API documentation


## client
```python
SeaweedFSDatasetClient.client.SeaweedFSDataClient(filer_url: str, root: str="data/datasets", max_active_taks: int=100):
```
- filer_url: url to filer
- root: client work directory, create in filer root automatically. By default: "data/datasets"
- max_active_taks: max active asynchronys task per time. By default: 100 task.

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

return:
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

return:
True if exists, Fasle else.

```python
remove(file_location: str|Path, recursive: bool=False)
```
Method for remove file or folder

- file_location: path to file for remove
- recursive: Flag allowed remove directories. By default: False
return:
True if was removed else Fasle

