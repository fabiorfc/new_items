import pandas as pd
import zipfile
import io
import requests
import ast
import numpy as np

class DataAnalyzer:
    def __init__(self, url: str):
        self.url = url
        self.data = None

    def load_data_set(self) -> pd.DataFrame:
        """Downloads the ZIP from the URL and returns the DataFrame"""
        response = requests.get(self.url)
        response.raise_for_status()  # Gera erro se a requisição falhar

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open("new_items_dataset.csv") as f:
                self.data = pd.read_csv(f)
        return self.data

    def statistical_summary(self) -> pd.DataFrame:

        if self.data is None:
            raise ValueError('The DataFrame has not been loaded yet. Use load_data_set() first.')

        df = self.data
        summary = pd.DataFrame(df[['price','base_price','sold_quantity']].sum(), columns = ['Total'])
        summary['Average'] = df[['price','base_price','sold_quantity']].mean()
        summary['coef of var'] = 100* df[['price','base_price','sold_quantity']].std()/ summary['Average']
        return summary


    def parse_list_column(self, column_name: str) -> pd.DataFrame:
        """
        Converts a column that may contain strings, lists or NaNs into real Python lists.
        Returns:
        - <column_name>_extracted : the real list
        """
        if self.data is None:
            raise ValueError("The DataFrame has not been loaded yet. Use load_data_set() first.")

        def safe_parse(x):
            # Return [] if value is NaN
            if pd.isna(x):
                return []
            # If the value looks like a list, try to parse
            if isinstance(x, str) and x.strip().startswith("[") and x.strip().endswith("]"):
                try:
                    return ast.literal_eval(x)
                except:
                    return []
            # Otherwise, wrap the value into a list
            return [x]

        parsed_data = self.data[column_name].apply(safe_parse).tolist()
        extracted_values_df = pd.DataFrame(parsed_data)
        extracted_values_df.columns = [f"{column_name}_{i}" for i in extracted_values_df.columns]

        return extracted_values_df


    def parse_nested_column(self, column_name: str) -> pd.DataFrame:
        """
        Parses a nested column that may contain:
        - 1: list of dicts with 'name' and 'value_name'
        - 2: list of dicts containing 'attribute_combinations', plus price and available_quantity
        - 3: list of dicts with arbitrary keys (e.g. size, secure_url)

        Returns a flattened DataFrame with the extracted information.
        """

        if self.data is None:
            raise ValueError("The DataFrame has not been loaded yet. Use load_data_set() first.")

        def safe_parse(cell):
            # Ignore empty or invalid cells
            if not isinstance(cell, str) or cell.strip() == "[]":
                return {}

            try:
                parsed_list = ast.literal_eval(cell)
            except:
                return {}

            final_dict = {}

            for i, item in enumerate(parsed_list):
                # Case 1: attributes (simple list of dicts)
                if "name" in item and "value_name" in item:
                    final_dict[item["name"]] = item["value_name"]

                # Case 2: variations (dict with attribute_combinations)
                elif "attribute_combinations" in item:
                    for combo in item.get("attribute_combinations", []):
                        name = combo.get("name")
                        value = combo.get("value_name")
                        if name:
                            final_dict[name] = value
                    if "price" in item:
                        final_dict[f"price_{i+1}"] = item["price"]
                    if "available_quantity" in item:
                        final_dict[f"available_quantity_{i+1}"] = item["available_quantity"]

                # Case 3: generic list of dicts (e.g. pictures)
                else:
                    for key, value in item.items():
                        final_dict[f"{key}_{i+1}"] = value

            return final_dict

        # Apply parsing
        df_parsed = self.data[column_name].apply(safe_parse)

        # Flatten to columns
        parsed_df = pd.json_normalize(df_parsed)

        return parsed_df


    def marking_outliers(self, column_name: str):
        """function that calculates the zcore and defines outliers that correspond to the 99.865% percentile"""
        price = self.data[column_name]
        z_score = (price - price.mean()) / price.std()
        outlier_rule = z_score.apply(lambda x: 'Outlier: High price' if x > 3 else ('Outlier: Low price' if x < -3 else 'Normal'))
        self.data[f'outlier_rule_for_{column_name}'] = outlier_rule

        return print(f'Calculation completed for {column_name}. Column with the outlier rule added to the original dataset.')


import pandas as pd
import zipfile
import io
import requests
import ast
import numpy as np

class DataAnalyzer:
    def __init__(self, url: str):
        self.url = url
        self.data = None

    def load_data_set(self) -> pd.DataFrame:
        """Downloads the ZIP from the URL and returns the DataFrame"""
        response = requests.get(self.url)
        response.raise_for_status()  # Gera erro se a requisição falhar

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open("new_items_dataset.csv") as f:
                self.data = pd.read_csv(f)
        return self.data

    def statistical_summary(self) -> pd.DataFrame:

        if self.data is None:
            raise ValueError('The DataFrame has not been loaded yet. Use load_data_set() first.')

        df = self.data
        summary = pd.DataFrame(df[['price','base_price','sold_quantity']].sum(), columns = ['Total'])
        summary['Average'] = df[['price','base_price','sold_quantity']].mean()
        summary['coef of var'] = 100* df[['price','base_price','sold_quantity']].std()/ summary['Average']
        return summary


    def parse_list_column(self, column_name: str) -> pd.DataFrame:
        """
        Converts a column that may contain strings, lists or NaNs into real Python lists.
        Returns:
        - <column_name>_extracted : the real list
        """
        if self.data is None:
            raise ValueError("The DataFrame has not been loaded yet. Use load_data_set() first.")

        def safe_parse(x):
            # Return [] if value is NaN
            if pd.isna(x):
                return []
            # If the value looks like a list, try to parse
            if isinstance(x, str) and x.strip().startswith("[") and x.strip().endswith("]"):
                try:
                    return ast.literal_eval(x)
                except:
                    return []
            # Otherwise, wrap the value into a list
            return [x]

        parsed_data = self.data[column_name].apply(safe_parse).tolist()
        extracted_values_df = pd.DataFrame(parsed_data)
        extracted_values_df.columns = [f"{column_name}_{i}" for i in extracted_values_df.columns]

        return extracted_values_df


    def parse_nested_column(self, column_name: str) -> pd.DataFrame:
        """
        Parses a nested column that may contain:
        - 1: list of dicts with 'name' and 'value_name'
        - 2: list of dicts containing 'attribute_combinations', plus price and available_quantity
        - 3: list of dicts with arbitrary keys (e.g. size, secure_url)

        Returns a flattened DataFrame with the extracted information.
        """

        if self.data is None:
            raise ValueError("The DataFrame has not been loaded yet. Use load_data_set() first.")

        def safe_parse(cell):
            # Ignore empty or invalid cells
            if not isinstance(cell, str) or cell.strip() == "[]":
                return {}

            try:
                parsed_list = ast.literal_eval(cell)
            except:
                return {}

            final_dict = {}

            for i, item in enumerate(parsed_list):
                # Case 1: attributes (simple list of dicts)
                if "name" in item and "value_name" in item:
                    final_dict[item["name"]] = item["value_name"]

                # Case 2: variations (dict with attribute_combinations)
                elif "attribute_combinations" in item:
                    for combo in item.get("attribute_combinations", []):
                        name = combo.get("name")
                        value = combo.get("value_name")
                        if name:
                            final_dict[name] = value
                    if "price" in item:
                        final_dict[f"price_{i+1}"] = item["price"]
                    if "available_quantity" in item:
                        final_dict[f"available_quantity_{i+1}"] = item["available_quantity"]

                # Case 3: generic list of dicts (e.g. pictures)
                else:
                    for key, value in item.items():
                        final_dict[f"{key}_{i+1}"] = value

            return final_dict

        # Apply parsing
        df_parsed = self.data[column_name].apply(safe_parse)

        # Flatten to columns
        parsed_df = pd.json_normalize(df_parsed)

        return parsed_df


    def marking_outliers(self, column_name: str):
        """function that calculates the zcore and defines outliers that correspond to the 99.865% percentile"""
        price = self.data[column_name]
        z_score = (price - price.mean()) / price.std()
        outlier_rule = z_score.apply(lambda x: 'Outlier: High price' if x > 3 else ('Outlier: Low price' if x < -3 else 'Normal'))
        self.data[f'outlier_rule_for_{column_name}'] = outlier_rule

        return print(f'Calculation completed for {column_name}. Column with the outlier rule added to the original dataset.')


    def identifying_inconsistent(self):
        """Function that marks the lines in the database for which the id is null"""
        inconsistence_rule = self.data['id'].apply(lambda x: 'Inconsistent' if pd.isna(x) else 'Consistent')

        self.data[f'inconsistence_rule'] = inconsistence_rule

        return print('The inconsistency rule was applied and a new column was added to the data')


    def date_converters(self):
        """This function returns the creation date of each record and colon in the format yyyyMMdd"""
        date_conveted = pd.to_datetime(data['date_created'], errors="coerce", format="mixed")

        self.data['date_created_converted'] = date_conveted.apply(lambda x: x.year*10000 + x.month*100 + x.day)

        return print('The date was converted and added to the original dataset')
