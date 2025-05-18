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
from typing import Any
from flatten_json import flatten
from datetime import datetime
from dateutil.parser import parse as parse_date
from collections import deque

class DataUtils:
    def __init__(self, logger: UniversalLogger = None):
        self.logger = logger or SimpleNamespace(info = print, warning = print, error = print, debug = print)


    # Master function to extract data from a JSON
    def extract_json(self, json_object: dict, keys: dict, keyset: list[str] | str | dict = [], convert_timekeys: bool = True, timekey_str: str = "timekey", out_key: str = "timestamp") -> dict:
        data: dict[str, list[dict]] = {}

        if not keyset:
            keyset = list(keys.keys())
        elif isinstance(keyset, str):
            keyset = [keyset]
        elif isinstance(keyset, dict):
            keyset = list(keyset.keys())

        flat_keys, nested_keys = self.organize_keyset(keys)

        flat_data = self.extract_json_data(json_object, flat_keys)
        nested_data = self.extract_json_list(json_object, nested_keys)

        
        unconverted_data = {**flat_data, **nested_data}

        if convert_timekeys:
            data = self.convert_timekeys(unconverted_data, timekey_str = timekey_str, out_key = out_key)
        else:
            data = unconverted_data

        return data


    # Function to extract data from a JSON with a keyset
    def extract_json_data(self, json_object: dict, keys: dict, keyset: list[str] | str | dict = []) -> dict:
        data: dict[str, list[dict]] = {}
        
        if not keyset:
            keyset = list(keys.keys())
        elif isinstance(keyset, str):
            keyset = [keyset]
        elif isinstance(keyset, dict):
            keyset = list(keyset.keys())

        for key, path in keys.items():
            if keyset and key not in keyset:
                continue
            value = self.get_nested_value(json_object, path)
            if value is not None:
                data[key] = value

        return data


    # Function to extract data from a JSON with a list in the keyset
    def extract_json_list(self, json_object: dict, listed_keys: dict, keyset: list[str] | str | dict = []) -> dict:
        data: dict[str, list[dict]] = {}

        if not keyset:
            keyset = list(listed_keys.keys())
        elif isinstance(keyset, str):
            keyset = [keyset]
        elif isinstance(keyset, dict):
            keyset = list(keyset.keys())

        for key, value in listed_keys.items():
            if keyset and key not in keyset:
                continue
            numbered_list = self.get_nested_value(json_object, value["master_path"])
            if not isinstance(numbered_list, list):
                continue

            rows: list[dict] = []
            flat_keys, nested_keys = self.organize_keyset(value["fields"])

            for list_item in numbered_list:
                flat_data = self.extract_json_data(list_item, flat_keys)

                if nested_keys:
                    nested_data = self.extract_json_list(list_item, nested_keys)
                    for second_rows in nested_data.values():
                        for second_row in second_rows:
                            rows.append({**flat_data, **second_row})
                else:
                    rows.append(flat_data)

            if rows:
                data[key] = rows

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
    

    # Function to organize a keyset
    def organize_keyset(self, keyset: dict) -> tuple[dict, dict]:
        flat_keyset = {}
        grouped = {}
        listed_keyset = {}

        for key, path in keyset.items():
            if "numbered_list" in path:
                try:
                    master_path, sub_path = path.split("numbered_list", 1)
                    master_path = master_path.rstrip(".")
                    sub_path = sub_path.lstrip(".")
                    grouped.setdefault(master_path, {})[key] = sub_path
                except ValueError:
                    self.logger.error(f"Invalid path: {path}")
            else:
                flat_keyset[key] = path

        for master_path, fields in grouped.items():
            match = next((flat_key for flat_key, flat_path in flat_keyset.items() if flat_path == master_path), None)
            bucket = match or master_path.rsplit(".", 1)[-1]

            if match:
                flat_keyset.pop(match)

            listed_keyset[bucket] = {
                "master_path": master_path,
                "fields": fields
            }

        return flat_keyset, listed_keyset
    

    # Function to convert timekeys to a timestamp
    def convert_timekeys(self, json_object: dict | list, timekey_str: str = "timekey", combine_function = None, out_key: str = "timestamp") -> dict | list:
        if combine_function is None:
            combine_function = self.time_combine

        queue = deque([json_object])

        while queue:
            node = queue.popleft()
            if isinstance(node, dict):
                fields: dict[str, object] = {}
                for key in list(node):
                    if timekey_str in key:
                        fields[key] = node.pop(key)

                if fields:
                    timestamp = None
                    try:
                        timestamp = combine_function(fields)
                    except Exception as e:
                        self.logger.error(f"Error combining timekeys on {fields}: {e}")

                    if timestamp is None:
                        node.update(fields)
                    else:
                        node[out_key] = timestamp
                
                for value in node.values():
                    if isinstance(value, (dict, list)):
                        queue.append(value)

            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, (dict, list)):
                        queue.append(item)

        return json_object
    

    # Function to combine timekeys
    def time_combine(self, fields: dict) -> datetime | None:
        for key, value in fields.items():
            if "date" in key.lower() and isinstance(value, str):
                try:
                    date_try = parse_date(value)
                    return date_try
                except Exception:
                    continue

        comps: dict[str, int] = {}
        for part in ["year", "month", "day", "hour", "minute", "second"]:
            for key, value in fields.items():
                if part in key.lower():
                    try:
                        comps[part] = int(value)
                    except (ValueError, TypeError):
                        pass
                    break

        if not any(part in comps for part in ["year", "month", "day"]):
            return None
        
        timestamp = datetime(
            year = comps.get("year", datetime.utcnow().year),
            month = comps.get("month", 1),
            day = comps.get("day", 1),
            hour = comps.get("hour", 0),
            minute = comps.get("minute", 0),
            second = comps.get("second", 0),
            )
        
        return timestamp
            

    # Function to extract a domain from a URL
    def extract_domain(self, url: str) -> str:
        return tldextract.extract(url).registered_domain
    

    # Function to flatten a JSON object
    def flatten_json(self, json: dict, separator: str = ".") -> dict:
        return flatten(json, separator = separator)


    # Function to compare two sets of JSON keys
    def compare_jsons(self, json1: dict, json2: dict, compare_values: bool = False) -> dict:
        if compare_values:
            old_set = set(json1.values())
            new_set = set(json2.values())
        else:
            old_set = set(json1.keys())
            new_set = set(json2.keys())

        added = new_set - old_set
        removed = old_set - new_set
        common = old_set & new_set
        percent_change = (len(added) + len(removed)) / max(len(old_set), 1) * 100

        return {
            "added": added,
            "removed": removed,
            "common": common,
            "percent_change": percent_change
        }


    # Function to identify JSON blob with search string
    def find_json_by_string(self, json_list: list[Any], search: str) -> Any | None:
        if isinstance(json_list, dict):
            json_list = [json_list]
        
        for blob in json_list:
            val = self.deep_find_search(blob, search)
            if val is not None:
                return val
            
        return None
    

    # Function to find a value in a nested object
    def deep_find_search(self, obj: Any, search: str) -> Any | None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if search in key:
                    return value
                found = self.deep_find_search(value, search)
                if found is not None:
                    return found
                
        elif isinstance(obj, list):
            for item in obj:
                found = self.deep_find_search(item, search)
                if found is not None:
                    return found
                
        return None


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
