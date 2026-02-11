from setuptools import setup, find_packages


setup(
    name="SeaweedFSDatasetClient",                  
    version="1.0.0",                          
    author="h128bit",
    description="Simple SeaweedFS client for storage and use datasets in mashine learning",

    packages=find_packages(),                 
   
    install_requires=[
        "nest_asyncio",
        "asyncio",
        "aiohttp",
        "aiofiles",
        "requests",
        "yarl",
        "tqdm"
    ],

    python_requires=">=3.10",

    classifiers=[
        "Programming Language :: Python :: 3",
        'Operating System :: OS Independent'
    ],

    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
)