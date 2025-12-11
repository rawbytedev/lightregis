from collections import OrderedDict
import json
import lmdb as tool


CACHESIZE = 30

class DBError(Exception):
    pass
class DBClass:
    def __init__(self) -> None:
        pass
    def close(self):
        pass
    def get(self, key):
        pass
    def put(self, key, value):
        pass
    def iterate(self,prefix):
        return [""] ## dummy value
class LMDBDBService(DBClass):
    def __init__(self, path="trustmesh.db", index_path="trustmesh_index.db", max_dbs=2):
        self.cache = OrderedDict()
        self.cache_size = CACHESIZE
        self.db = tool.open(path, max_dbs=max_dbs)
        self.index = tool.open(index_path, max_dbs=max_dbs)

    def _cache_set(self, key, value):
        if len(self.cache) >= self.cache_size:
            self.cache.popitem(last=False)
        self.cache[key] = value

    def get(self, key: str):
        if not key:
            raise DBError("Key can't be empty")
        if key in self.cache:
            return self.cache[key]
        with self.db.begin(write=False) as txn:
            hash_key = dighash(key.encode())
            value = txn.get(hash_key)
            if value is None:
                raise DBError(f"Value for key {key} not found")
            decoded = json.loads(value.decode())
            self._cache_set(key, decoded)
            return decoded

    def put(self, key: str, value: str):
        if not key:
            raise DBError("Key can't be empty")
        if not value:
            raise DBError("Value can't be empty")
        self._cache_set(key, value)
        val = json.dumps(value)
        hash_key = dighash(key.encode())
        try:
            with self.db.begin(write=True) as txn:
                txn.put(hash_key, val.encode())
            with self.index.begin(write=True) as txn:
                txn.put(key.encode(), hash_key)
        except Exception as e:
            print(e)
            raise DBError(f"Can't insert item: {key}:{value}")

    def iterate(self, prefix: str):
        """
        Iterate over all keys in the index database with a given prefix (e.g. 'ec:').

        Returns:
            prefix_bytes = prefix.encode()  # LMDB keys must be bytes, so encode the prefix
        """
        results = []
        with self.index.begin(write=False) as txn:
            cursor = txn.cursor()
            prefix_bytes = prefix.encode()
            if cursor.set_range(prefix_bytes):
                with self.db.begin(write=False) as dtxn:
                    # iterate from the current cursor position and stop when keys no longer match the prefix
                    for k, v in cursor:
                        if not k.startswith(prefix_bytes):
                            break
                        # v is the hash_key, fetch from main DB
                        val = dtxn.get(v)
                        if val:
                            # Decode key and value before appending for clarity
                            decoded_key = k.decode()
                            decoded_val = json.loads(val.decode())
                            results.append((decoded_key, decoded_val))
        return results

    def close(self):
        self.cache.clear()
        self.db.close()
        self.index.close()

class SimpleDBService(DBClass):
    def __init__(self):
        self.store = {}
    def get(self, key: str):
        if key not in self.store:
            raise DBError(f"Key {key} not found")
        return self.store[key]
    def put(self, key: str, value: str):
        self.store[key] = value
    def iterate(self, prefix: str):
        results = []
        for k, v in self.store.items():
            if k.startswith(prefix):
                results.append((k, v))
        return results
    def close(self):
        self.store.clear()
#### SQLAlchemy setup ####
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import settings

engine = create_engine(
    settings.DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
#### Helpers ####
import uuid
import hashlib

"""Those are small helpers used by other functions or methods"""
def newuuid() -> uuid.UUID:
    return uuid.uuid4()

def dighash(data:bytes) -> bytes:
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).digest()

def hexhash(data:bytes) -> str:
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).hexdigest()

def encode_id(escrow_id: int) -> str:
    """Convert internal int ID to string for JSON/API."""
    return str(escrow_id)

def decode_id(raw: str | int) -> int:
    """Convert external string/int back to internal int."""
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        return int(raw)
    raise ValueError(f"Unsupported escrow_id type: {type(raw)}")