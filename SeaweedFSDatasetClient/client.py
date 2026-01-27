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

from SeaweedFSDatasetClient.utils import is_notebook



class SeaweedFSDataClient:
    def __init__(self, 
                 filer_url: str,
                 root: str="data/datasets"):
        self.filer_url = URL(filer_url)
        self.root = root
        self.base_location = self.filer_url / self.root


    def push(self, 
             files: bytes|str|Path|list[Union[bytes, str, Path]],
             file_location: str|Path,
             file_names: str|Path|list[Union[str, Path]]|None=None) -> int | list[int]:

        target_location = self.base_location / str(file_location)

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
        

    async def _async_upload(self,
                            files: list[Union[bytes, str, Path]],
                            file_names: list[Union[str, Path]]|None,
                            target_location:URL):
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
