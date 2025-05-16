from neil_logger import UniversalLogger
import tldextract
import re
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import math
import ast
import json
import logging

class DataUtils:
    def __init__(self, logger: UniversalLogger = None):
        self.logger = logger or logging.getLogger(__name__)


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
    def csv_to_mongodb(self, csv_path: str, mongo_uri: str, mongo_db: str, mongo_collection: str, dtype_as_str: bool = True, include_empty: bool = False, batch_size: int = 1000, upsert_key: str = None) -> None:
        df = pd.read_csv(csv_path, dtype=str if dtype_as_str else None)

        records = df.where(pd.notna(df), None).to_dict(orient="records")

        docs = []
        for record in records:
            if not include_empty:
                record = {k: v for k, v in record.items() if v not in [None, "", "nan"]}

            if not record:
                continue
        
            for k, v in list(record.items()):
                if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                    try:
                        record[k] = json.loads(v)
                    except (json.JSONDecodeError, TypeError):
                        try:
                            record[k] = ast.literal_eval(v)
                        except (ValueError, SyntaxError):
                            pass

            docs.append(record)

        if not docs:
            self.logger.warning(f"No documents to insert into {mongo_collection}")
            return

        client = MongoClient(mongo_uri)
        collection = client[mongo_db][mongo_collection]

        total = len(docs)
        batches = math.ceil(total / batch_size)
        inserted = 0

        for i in range(batches):
            batch = docs[i * batch_size:(i + 1) * batch_size]
            try:
                if upsert_key and upsert_key in batch[0]:
                    ops = [UpdateOne({upsert_key: doc[upsert_key]}, {"$set": doc}, upsert=True) for doc in batch]
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