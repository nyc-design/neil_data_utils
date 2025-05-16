from neil_logger import UniversalLogger
import tldextract
import re
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import math
import ast
import json
from types import SimpleNamespace

class DataUtils:
    def __init__(self, logger: UniversalLogger = None):
        self.logger = logger or SimpleNamespace(info = print, warning = print, error = print, debug = print)

    # Function to extract data from a list of responses
    def extract_json_data(self, json: dict, keys: dict, keyset: list[str] | str = []) -> dict:
        data = {}
        if isinstance(keyset, str):
            keyset = [keyset]
        
        for key, path in keys.items():
            if keyset and key not in keyset:
                continue
            value = self.get_nested_value(json, path)
            if value is not None:
                data[key] = value

        return data
        

    # Function to get a nested value from a dictionary
    def get_nested_value(self, data, path, default = None):
        for key in path.split("."):
            if isinstance(data, dict):
                data = data.get(key)
            elif isinstance(data, (list, tuple)):
                try:
                    data = data[int(key)]
                except (IndexError, ValueError, TypeError):
                    return default
            else:
                return default
            
            if data is None:
                return default
        
        return data
    

    # Function to extract a domain from a URL
    def extract_domain(self, url: str) -> str:
        return tldextract.extract(url).registered_domain
    

    # Function to convert a csv into uploaded documents to MongoDB
    def csv_to_mongodb(self, csv_path: str, mongo_uri: str, mongo_db: str, mongo_collection: str, dtype_as_str: bool = True, include_empty: bool = False, batch_size: int = 1000, upsert_keys: str | list[str] = None) -> None:
        df = pd.read_csv(csv_path, dtype=str if dtype_as_str else None)

        records = df.where(pd.notna(df), None).to_dict(orient="records")

        docs = []
        for record in records:
            if not include_empty:
                record = {k: v for k, v in record.items() if v not in [None, "", "nan"]}

            if not record:
                continue
        
            for k, v in list(record.items()):
                if not isinstance(v, str):
                    continue

                s = v.strip()
                if not (s.startswith("[") and s.endswith("]")):
                    continue

                try:
                    record[k] = json.loads(s)
                    continue
                except Exception:
                    pass

                try:
                    record[k] = ast.literal_eval(s)
                    continue
                except Exception:
                    pass

                inner = s[1:-1]
                parts = [item.strip() for item in inner.split(",") if item.strip()]
                record[k] = parts

            docs.append(record)

        if not docs:
            self.logger.warning(f"No documents to insert into {mongo_collection}")
            return

        client = MongoClient(mongo_uri)
        collection = client[mongo_db][mongo_collection]

        total = len(docs)
        batches = math.ceil(total / batch_size)
        inserted = 0

        upsert_keys = [upsert_keys] if isinstance(upsert_keys, str) else upsert_keys

        for i in range(batches):
            batch = docs[i * batch_size:(i + 1) * batch_size]
            try:
                if upsert_keys and all(key in batch[0] for key in upsert_keys):
                    ops = []
                    for doc in batch:
                        filter = {key: doc[key] for key in upsert_keys}
                        update = {"$set": doc}
                        ops.append(UpdateOne(filter, update, upsert=True))
                    result = collection.bulk_write(ops)
                    inserted += result.upserted_count + result.modified_count
                else:
                    result = collection.insert_many(batch)
                    inserted += len(result.inserted_ids)
            
            except BulkWriteError as bwe:
                self.logger.error(f"BulkWriteError on batch {i + 1} of {batches}: {bwe.details}")
            except Exception as e:
                self.logger.error(f"Error on batch {i + 1} of {batches}: {e}")
            
        self.logger.info(f"Finished: {inserted}/{total} documents processed into {mongo_collection}")


    # Function to convert a MongoDB collection to a csv
    def mongodb_to_csv(self, csv_path: str, mongo_uri: str, mongo_db: str, mongo_collection: str, batch_size: int = 1000, filter_query: dict = None, projection: dict = None) -> None:
        client = MongoClient(mongo_uri)
        collection = client[mongo_db][mongo_collection]

        query = filter_query or {}

        cursor = collection.find(query, projection) if projection is not None else collection.find(query)

        docs = list(cursor)

        if not docs:
            self.logger.warning(f"No documents to export from {mongo_collection}")
            return

        for doc in docs:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        df = pd.json_normalize(docs)

        df.to_csv(csv_path, index=False)

        self.logger.info(f"Exported {len(docs)} documents from {mongo_collection} to {csv_path}")
