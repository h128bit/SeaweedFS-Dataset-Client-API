import nest_asyncio
nest_asyncio.apply()

import logging
import itertools
from pathlib import Path
from typing import Union
from contextlib import nullcontext
from collections.abc import Sequence

import asyncio
import aiohttp
import aiofiles
import requests
from yarl import URL
from tqdm.asyncio import tqdm




class SeaweedFSDataClient:
    def __init__(self, 
                 filer_url: str,
                 root: str="data/datasets",
                 max_active_taks: int=100):
        self.filer_url = URL(filer_url)
        self.root = root
        self.base_location = self.filer_url / self.root
        self.MAX_ACTIVE_TASK = max_active_taks


    def _get_url(self, file_location: str|Path):
        return self.base_location / str(file_location).replace("\\", "/")


    def _create_remote_directory(self, path:URL):
        path = path / ""
        requests.post(str(path))


    def _get_remote_listdir(self, file_location: str, last_name: str|None):
            param = {"lastFileName": last_name} if last_name else None

            response = requests.get(file_location, headers={"Accept": "application/json"}, params=param)
            response.raise_for_status()

            response = response.json()
            return response


    def _get_remote_dir_structure(self, 
                                  remote_dir: str|Path):
        queue = [remote_dir]
        paths = []

        while queue:
            path = Path(queue.pop(0))
            files = self.listdir(path, raw=True)
            for item in files:
                pp = path / Path(item["FullPath"]).name
                if item["Md5"]:  #  if Md5 is not None, then is file else is folder
                    paths.append(pp)
                else:
                    queue.append(pp)

        grouped_paths_iter = itertools.groupby(paths, key=lambda p: p.parent)
        return grouped_paths_iter


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


    async def _async_upload(self,
                            files: Sequence[Union[bytes, str, Path]],
                            file_names: Sequence[Union[str, Path]]|None,
                            target_location: URL):
        semaphore = asyncio.Semaphore(self.MAX_ACTIVE_TASK)
        async with aiohttp.ClientSession() as session:
            if isinstance(files[0], bytes):
                tasks = [self._async_upload_one(session, semaphore, file, target_location, filename) for file, filename in zip(files, file_names)]
            else:# isinstance(files, (str, Path)):
                tasks = [self._async_upload_one(session, semaphore, file, target_location, None) for file in files]
            return await tqdm.gather(*tasks, desc="Pushing files")


    async def _async_upload_one(self, 
                      session: aiohttp.ClientSession,
                      semaphore: asyncio.Semaphore,
                      file: bytes|str|Path, 
                      target_location: URL,
                      file_name: str|None):
        data = aiohttp.FormData()

        async with semaphore:
            if isinstance(file, (str, Path)):
                file_name = Path(file).name 
                async with aiofiles.open(file, "rb") as f:
                    file = await f.read() 
            
            data.add_field("file", file, filename=file_name)
            target_location = target_location / file_name
            async with session.post(target_location, data=data) as response:
                return response.status 


    def _sync_load(self, 
                   file_location: str|Path, 
                   raise_if_not_200:bool) -> bytes:

        location = self._get_url(file_location)
        res = requests.get(str(location))

        if raise_if_not_200:
            res.raise_for_status()

        return res.content


    async def _async_load(self, 
                          file_location: list[str|Path], 
                          raise_if_not_200: bool) -> list[bytes]:
        semaphore = asyncio.Semaphore(self.MAX_ACTIVE_TASK)
        async with aiohttp.ClientSession() as session:
            tasks = [self._async_load_one(session, semaphore, file_loc, raise_if_not_200) for file_loc in file_location]
            return await tqdm.gather(*tasks, desc="Download files", leave=True)


    async def _async_load_one(self,
                              session: aiohttp.ClientSession,
                              semaphore: asyncio.Semaphore|nullcontext,
                              file_location: str|Path,
                              raise_if_not_200: bool) -> bytes:
        location = self._get_url(file_location)
        async with semaphore:
            async with session.get(location) as response:
                if raise_if_not_200:
                    response.raise_for_status()
                content = await response.read()
                return content
        

    async def _async_save_after_load_one(self, 
                                         session: aiohttp.ClientSession, 
                                         semaphore: asyncio.Semaphore, 
                                         file_location: str|Path, 
                                         path_to_save: str|Path, 
                                         raise_if_not_200: bool):
        
        async with semaphore:
            content = await self._async_load_one(session, nullcontext(), file_location, raise_if_not_200)
            file_location = Path(file_location)
            path_to_save = Path(path_to_save)

            file_location.parent.mkdir(exist_ok=True, parents=True)
            async with aiofiles.open(path_to_save / file_location, "wb") as f:
                await f.write(content)


    async def _async_save_after_load(self, file_location, path_to_save, raise_if_not_200):
        semaphore = asyncio.Semaphore(self.MAX_ACTIVE_TASK)
        async with aiohttp.ClientSession() as session:
            tasks = [self._async_save_after_load_one(session, semaphore, file, path_to_save, raise_if_not_200) for file in file_location]
            return await tqdm.gather(*tasks, desc="Download files to folder", leave=True)


    def listdir(self, 
                file_location: str|Path,
                get_all: bool=True,
                raw: bool=False,
                last_name: str|None=None) -> list[str]|list[dict]:
        
        if not self.exists(file_location):
            raise FileNotFoundError(f"Directory is not exists\n{file_location}")
        
        target_location = self._get_url(file_location) / ""
        target_location = str(target_location)
        file_list = []

        response = self._get_remote_listdir(target_location, None)

        entry = response["Entries"]
        if entry:
            file_list.extend(entry)
        last_name = response["LastFileName"]

        while get_all and entry:
            response = self._get_remote_listdir(target_location, last_name)

            entry = response["Entries"]
            if entry:
                file_list.extend(entry)

            last_name = response["LastFileName"]

        if raw:
            return file_list
        else:
            return [Path(item["FullPath"]).name for item in file_list]


    def mkdirs(self, 
               file_location: str|Path) -> None:
        
        ch = [".", "*", "?", ":", "<", ">", "|", "\""]
        if any(char in str(file_location) for char in ch):
            raise ValueError(f"Was passed is not a dir\n{file_location}")

        location = self._get_url(file_location)
        self._create_remote_directory(location)


    def exists(self, file_location: str|Path) -> bool:
        location = self._get_url(file_location)

        status = requests.get(str(location)).status_code 

        return status in [200, 201] 


    def remove(self, 
                file_location: str|Path, 
                recursive: bool=False) -> int:
            
            location = self._get_url(file_location)

            param = {"recursive": "true"} if recursive else {}

            response = requests.delete(str(location), params=param)

            status = response.status_code

            if status == 500:
                raise ValueError("File is not exists or directory is not empty. If you want remove not empty directory set recursive=True")

            return status


    def push(self, 
             files: bytes|str|Path|Sequence[Union[bytes, str, Path]],
             file_location: str|Path,
             file_names: str|Path|Sequence[Union[str, Path]]|None=None) -> int | list[int]:

        if not self.exists(file_location):
            raise FileNotFoundError(f"Directory is not exists\n{file_location}\nIf you want create folder in file uploading use `push_folder` method")

        target_location = self._get_url(file_location)

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
                     remote_folder_name: str|None=None) -> dict:
        
        path_to_folder = Path(path_to_folder)
        if not path_to_folder.is_dir():
            raise ValueError(f"Directory\n{path_to_folder}\nis not a folder")

        if remote_folder_name is None:
            remote_folder_name = path_to_folder.name

        logging.info("Analise the local directory ...")
        files = [f for f in path_to_folder.rglob("*") if f.is_file()]
        grouped_paths_iter = itertools.groupby(files, key=lambda p: p.parent)

        report = {}
        for folder, src_paths in grouped_paths_iter:
            location = Path(remote_folder_name) / folder.relative_to(path_to_folder)
            target_location = self._get_url(location)

            src_paths = list(src_paths)
            
            self._create_remote_directory(target_location)
            coroutine = self._async_upload(src_paths, None, target_location)
            status = asyncio.run(coroutine)
            report[location] = dict(zip(src_paths, status))

        return report
 

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
    

    def pull_folder(self, 
                    file_location: str|Path, 
                    local_path: str|Path):
        logging.info(("Analise the remote directory ...")) 

        remote_dir_structure = self._get_remote_dir_structure(file_location)

        local_path = Path(local_path)
        for parent, files in remote_dir_structure:
            files = list(files) 
            local = local_path / parent
            local.mkdir(parents=True, exist_ok=True)
            coroutine = self._async_save_after_load(file_location=files, 
                                                    path_to_save=local_path, 
                                                    raise_if_not_200=True)
            asyncio.run(coroutine)

