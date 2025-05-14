import tldextract
import re
from neil_logger import UniversalLogger



class DataUtils:
    def __init__(self, logger: UniversalLogger = None):
        self.logger = logger or print


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
    

    # Function to extract a domain from a URL:
    def extract_domain(self, url: str) -> str:
        return tldextract.extract(url).registered_domain
