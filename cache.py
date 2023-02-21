import duckdb
import adlfs ,os
from fsspec.implementations.cached import WholeFileCacheFileSystem
from dotenv import load_dotenv
load_dotenv()
AZURE_STORAGE_ACCOUNT_NAME = os.getenv('AZURE_STORAGE_ACCOUNT_NAME') 
AZURE_STORAGE_ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY') 
table_path = os.getenv('table_path') 
azure_file_system = adlfs.AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT_NAME, account_key=AZURE_STORAGE_ACCOUNT_KEY )
fs = WholeFileCacheFileSystem(fs=azure_file_system, cache_storage="./tmp/")
duckdb.register_filesystem(fs)
duckdb.sql(f'''
    select distinct filename   from read_parquet('{table_path}/directory/*/*.parquet',
    HIVE_PARTITIONING=1,filename = 1)
    where year='1992'
    '''
).show()
