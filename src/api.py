import requests
import pandas as pd
from auth import BaseApiAuth

class BaseExtractor:
    """
    Class responsible for fetching and processing movie data
    from the OMDb API.
    """

    def __init__(self,base_params: dict = None ):
        """
        Initializes the extractor with base query parameters and API token.

        base_params: Optional dictionary of additional query parameters.
        """
        auth = BaseApiAuth()
        self.url = auth.full_url
        self.token = auth.get_token()
        self.base_params = base_params or {}

    def fetch_titles_param(self):
        """
        Fetches unique movie titles from a local CSV file (revenues_per_day.csv)
        to be used as query parameters for the API requests.

        :return: List of unique movie titles (limited to 5).
        """
        df = pd.read_csv("revenues_per_day.csv")
        titles = df["title"].dropna().unique().tolist()
        return titles[:5]
    
    def get_all_params(self):
        """
        Creates a list of parameter dictionaries for API requests
        based on movie titles and API token.

        :return: List of parameter dictionaries for each title.
        """
        all_params = []
        titles = self.fetch_titles_param()
        for title in titles:
            params = self.base_params.copy()
            params["t"] = title
            params["apikey"] = self.token
            all_params.append(params)
        return all_params
    
    def _flatten_nested_dict(self, raw, prefix="", data=None):
        """
        Recursively flattens nested dictionaries and lists into a single-level dictionary,
        creating keys in the format "prefix_key" or "prefix_index" for lists.

        :param raw: The original nested dictionary or list,
        :param prefix: The current prefix for keys (used during recursion),
        :param data: The resulting dictionary to which flattened elements are added.

        :return: Flattened dictionary.
        """
        if data is None:
            data = {}

        if isinstance(raw, dict):
            for key, value in raw.items():
                self._flatten_nested_dict(value, f"{prefix}_{key}" if prefix else key, data)
        elif isinstance(raw, list):
            for idx, item in enumerate(raw):
                new_prefix = f"{prefix}_{idx}" if prefix else str(idx)
                self._flatten_nested_dict(item, new_prefix, data)


        else:
            data[prefix] = raw

        return data

    def fetch_data(self):
        all_params = self.get_all_params()
        results = []
        for params in all_params:
            response = requests.get(self.url,params=params, timeout=30)
            if response.status_code == 200:
                raw = response.json()
            if isinstance(raw, dict):
                flat = self._flatten_nested_dict(raw)
                results.append(flat)    
        return results
