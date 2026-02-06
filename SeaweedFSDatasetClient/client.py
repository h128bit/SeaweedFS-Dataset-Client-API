import nest_asyncio
nest_asyncio.apply()

import asyncio
import aiohttp
import aiofiles
from tqdm.asyncio import tqdm
import requests
from yarl import URL
from pathlib import Path
from typing import Union
from collections.abc import Sequence
import itertools



class SeaweedFSDataClient:
    def __init__(self, 
                 filer_url: str,
                 root: str="data/datasets"):
        self.filer_url = URL(filer_url)
        self.root = root
        self.base_location = self.filer_url / self.root


    async def _async_upload(self,
                            files: Sequence[Union[bytes, str, Path]],
                            file_names: Sequence[Union[str, Path]]|None,
                            target_location: URL):
        tasks = []
        async with aiohttp.ClientSession() as session:
            if isinstance(files[0], bytes):
                tasks = [self._async_upload_one(session, file, target_location, filename) for file, filename in zip(files, file_names)]
            elif isinstance(files[0], (str, Path)):
                for file in files:
                    file = Path(file)
                    name = file.name 
                    async with aiofiles.open(file, "rb") as f:
                        file = await f.read()
                    task = self._async_upload_one(session, file, target_location, name)
                    tasks.append(task)
            return await tqdm.gather(*tasks, desc="Pushing files")


    async def _async_upload_one(self, 
                      session: aiohttp.ClientSession,
                      file: bytes, 
                      target_location: URL,
                      file_name: str):
        
        data = aiohttp.FormData()
        data.add_field("file", file, filename=file_name)
        target_location = target_location / file_name
        async with session.post(target_location, data=data) as response:
            return response.status 


    def _sync_upload(self, 
                     file: bytes|str|Path,
                     target_location: URL,
                     file_name: str):
        
        if isinstance(file, (str, Path)):
            file = Path(file)
            file_name = file.name 
            with open(file, "rb") as f:
                file = f.read()

        target_location = target_location / file_name
        package = {"file": (file_name, file)}
        response = requests.post(url=str(target_location), files=package)
        return response.status_code


    def _create_remote_directory(self, path:URL):
        path = path / ""
        requests.post(str(path))


    def mkdirs(self, 
               path:str|Path):
        if not Path(path).is_dir():
            raise ValueError(f"Was passed is not a dir\n{path}")
        
        path = str(path).replace("\\", "/")
        location = self.base_location / path 
        self._create_remote_directory(location)


    def exists_directory(self,
                         path:str|Path):
        path = str(path).replace("\\", "/")
        location = self.base_location / path

        status = requests.get(str(location)).status_code 

        return status in [200, 201]


    def listdir(self, 
                path: str|Path,
                get_all=True,
                raw: bool=False):
        
        if not self.exists_directory(path):
            raise FileNotFoundError(f"Directory is not exists\n{path}")
        
        target_location = self.base_location / str(path).replace("\\", "/")
        target_location = str(target_location)
        file_list = []

        response = requests.get(target_location, headers={"Accept": "application/json"})
        response.raise_for_status()

        response = response.json()

        entry = response["Entries"]
        if entry:
            file_list.extend(entry)
        last_name = response["LastFileName"]

        while get_all and entry:
            response = requests.get(target_location, 
                                    params={"lastFileName": last_name}, 
                                    headers={"Accept": "application/json"})
            response.raise_for_status()
            response = response.json()

            entry = response["Entries"]
            if entry:
                file_list.append(entry)

        if raw:
            return file_list
        else:
            return [Path(item["FullPath"]).name for item in file_list]


    def push(self, 
             files: bytes|str|Path|Sequence[Union[bytes, str, Path]],
             file_location: str|Path,
             file_names: str|Path|Sequence[Union[str, Path]]|None=None) -> int | list[int]:

        if not self.exists_directory(file_location):
            raise FileNotFoundError(f"Directory is not exists\n{file_location}\nIf you want create folder in file uploading use `push_folder` method")

        target_location = self.base_location / str(file_location).replace("\\", "/")

        if isinstance(files, list):
            if not sum([isinstance(obj, type(files[0])) for obj in files]) == len(files):  # checking on same dtype in input
                raise Exception("All object in files attribute must have same type")
            if isinstance(files[0], bytes):  # checking on corresponds number of filenames and input files
                if file_names is None or len(files) != len(file_names):
                    raise Exception("If files are passed to the function as bytes, then a filename should be assigned to each file.")
            elif isinstance(files[0], (str, Path)): # plug
                pass
            else:
                raise Exception("Unsupported type in list of files. Can containce bytes or str or pathlib.Path")
            
            coroutine = self._async_upload(files, file_names, target_location)
            status = asyncio.run(coroutine)

        else:
            if isinstance(files, bytes):
                if file_names is None: 
                    raise Exception("If files are passed to the function as bytes, then a filename should be assigned")
            elif isinstance(files, (str, Path)):
                pass
            else: 
                raise Exception(f"Unsupported type for files attribute. Expected bytes, str, pathlib.Path, but got: {type(files)}")

            status = self._sync_upload(files, target_location, file_names)

        return status
        

    def push_folder(self, 
                     path_to_folder: str|Path,
                     remote_folder_name: str|None=None):
        
        path_to_folder = Path(path_to_folder)
        if not path_to_folder.is_dir():
            raise ValueError(f"Directory\n{path_to_folder}\nis not a folder")

        if remote_folder_name is None:
            remote_folder_name = path_to_folder.name

        files = [f for f in path_to_folder.rglob("*") if f.is_file()]
        grouped_paths_iter = itertools.groupby(files, key=lambda p: p.parent)

        report = {}
        for folder, src_paths in grouped_paths_iter:
            location = str(folder.relative_to(path_to_folder)).replace("\\", "/")
            target_location = self.base_location / remote_folder_name / location

            src_paths = list(src_paths)
            
            self._create_remote_directory(target_location)
            coroutine = self._async_upload(src_paths, None, target_location)
            status = asyncio.run(coroutine)
            report[location] = dict(zip(src_paths, status))

        return report


    def _sync_load(self, 
                   file_location: str|Path, 
                   raise_if_not_200:bool) -> bytes:

        location = self.base_location/str(file_location).replace("\\", "/")
        res = requests.get(str(location))

        if raise_if_not_200:
            res.raise_for_status()

        return res.content
    

    async def _async_load(self, 
                          file_location: list[str|Path], 
                          raise_if_not_200: bool) -> list[bytes]:
        tasks = []
        async with aiohttp.ClientSession() as session:
            tasks = [self._async_load_one(session, file_loc, raise_if_not_200) for file_loc in file_location]
            return await tqdm.gather(*tasks, desc="Download files", leave=True)


    async def _async_load_one(self,
                              session: aiohttp.ClientSession,
                              file_location: str|Path,
                              raise_if_not_200: bool):
        location = self.base_location/str(file_location).replace("\\", "/")
        async with session.get(location) as response:
            if raise_if_not_200:
                response.raise_for_status()
            content = await response.read()
            return content


    def pull(self, 
             files: str|Path|Sequence[Union[str, Path]], 
             raise_if_not_200: bool=True) -> bytes|list[bytes]:
        
        if isinstance(files, (str, Path)):
            content = self._sync_load(files, raise_if_not_200)
        elif isinstance(files, list):
            coroutine = self._async_load(files, raise_if_not_200) 
            content = asyncio.run(coroutine)
        else:
            raise TypeError(f"Unsupported type of files. Expected str, pathlib.Path or list of str or pathlib.Path, but got {type(files)}")

        return content 

