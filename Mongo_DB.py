from pymongo.mongo_client import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv


class Mongo_DB:

    def __init__(self):
        load_dotenv()
        url = os.getenv("MONGO_URL")
        self.client = MongoClient(url)
        self._db = None
        self._collection = None

    # Database property
    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, name: str):
        assert isinstance(name, str), "Database name must be a string."
        self._db = self.client[name]

    # Collection property
    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, name: str):
        assert self._db is not None, "Select a database first."
        assert isinstance(name, str), "Collection name must be a string."
        self._collection = self._db[name]

    # Database operations
    def show_databases(self):
        return self.client.list_database_names()

    def show_collections(self):
        assert self._db is not None, "Select a database first."
        return self._db.list_collection_names()

    # CRUD(Create-Read-Update-Delete) operations
    def fetch(self, query=None, projection=None, limit=0):
        assert self._collection is not None, "Select a collection first."
        query = query or {}
        cursor = self._collection.find(query, projection)
        return list(cursor.limit(limit)) if limit else list(cursor)

    def insert(self, data):
        assert self._collection is not None, "Select a collection first."
        if isinstance(data, dict):
            return self._collection.insert_one(data)
        elif isinstance(data, list):
            return self._collection.insert_many(data)
        else:
            raise TypeError("Data must be dict or list of dicts.")

    def update(self, query, new_values):
        assert self._collection is not None, "Select a collection first."
        return self._collection.update_many(query, {"$set": new_values})

    def delete(self, query):
        assert self._collection is not None, "Select a collection first."
        return self._collection.delete_many(query)

    # Dataframe integration
    def to_dataframe(self, query=None, projection=None):
        data = self.fetch(query, projection)
        return pd.DataFrame(data)

    def from_dataframe(self, df: pd.DataFrame):
        records = df.to_dict("records")
        return self.insert(records)

    # Giving info about our database
    def __repr__(self):
        db = self._db.name if self._db else None
        col = self._collection.name if self._collection else None
        return f"MongoDB:\n - Selected DB: {db}\n - Selected Collection: {col}"
