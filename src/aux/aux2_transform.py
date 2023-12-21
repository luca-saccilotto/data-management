import pandas as pd
import re

class DataProcessor:
    
    def standardize_numbers(self, df):
        country_codes = {"GB": "+44", "DE": "+49", "US": "+1"}
        for index, row in df.iterrows():
            phone_number = row["phone_number"]
            country_code = row["country_code"]
            # Remove any non-digit characters from the phone number
            phone_number = re.sub(r"[^\d+]", "", phone_number)
            # Add the prefix if it is not already present
            if not phone_number.startswith(country_codes[country_code]):
                phone_number = country_codes[country_code] + phone_number
            # Update the phone number
            df.at[index, "phone_number"] = phone_number
        return df

    def clean_users(self, df):
        df = df.dropna()
        df = df.drop_duplicates()
        # Filter and standardize country codes
        df = df[df["country"].str.contains("United Kingdom|Germany|United States")]
        df["country_code"] = df["country_code"].replace("GGB", "GB")
        df = df[df["country_code"].str.contains("GB|DE|US")]
        # Standardize phone numbers and dates
        df = self.standardize_numbers(df)
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], infer_datetime_format = True, errors = "coerce")
        df["join_date"] = pd.to_datetime(df["join_date"], infer_datetime_format = True, errors = "coerce")
        return df
    
    def clean_cards(self, df):
        df = df.dropna()
        df = df.drop_duplicates()
        df = df[df["card_provider"].str.contains("VISA 16 digit|JCB 16 digit|VISA 13 digit|JCB 15 digit|VISA 19 digit|Diners Club / Carte Blanche|American Express|Maestro|Discover|Mastercard")]
        df["date_payment_confirmed"] = pd.to_datetime(df["date_payment_confirmed"], infer_datetime_format = True, errors = "coerce")
        df = df.reset_index(drop = True)
        return df
    
    def clean_stores(self, df):
        df = df.drop("lat", axis = 1)
        df = df.dropna()
        df = df.drop_duplicates()
        df = df[~ df.locality.str.contains(r"\d")]
        df = df[pd.to_numeric(df["latitude"], errors = "coerce").notnull()]
        df = df[pd.to_numeric(df["staff_numbers"], errors = "coerce").notnull()]
        df["continent"] = df["continent"].replace({"eeEurope": "Europe", "eeAmerica": "America"})
        df["opening_date"] = pd.to_datetime(df["opening_date"], infer_datetime_format = True, errors = "coerce")
        df = df.reset_index(drop = True)
        return df
    
    def convert_weights(self, df):
        # Create an empty column for the weight unit
        df["unit"] = pd.NA
        # Loop through the rows of the dataset
        for index, row in df.iterrows():
            weight = str(row["weight"])
            # Check the measure of weight
            if "kg" in weight:
                unit = "kg"
            elif "g" in weight:
                unit = "g"
            elif "ml" in weight:
                unit = "ml"
            elif "oz" in weight:
                unit = "oz"
            else:
                continue
            # Save the unit and weight to their respective columns
            df.at[index, "unit"] = unit
            df.at[index, "weight"] = weight.replace(unit, "")
            # Take a value that may contain a quantity multiplier and evaluate it
            if "x" in weight:
                weight = eval(weight.replace("x", "*").replace(" ", "").replace("g", ""))
            else:
                continue
        df.at[1779, "weight"] = 77
        df = df[pd.to_numeric(df["weight"], errors = "coerce").notnull()]
        # Standardize weight type to numeric and units measure
        df["weight"] = df["weight"].astype(float)
        df.loc[df["unit"] == "g", "weight"] /= 1000
        df.loc[df["unit"] == "ml", "weight"] /= 1000
        df.loc[df["unit"] == "oz", "weight"] /= 35.274
        return df
    
    def clean_products(self, df):
        df = df.dropna()
        df = df.drop_duplicates()
        df = df.drop(columns = ["unit"])
        df = df.drop(df.columns[0], axis = 1)
        df = df.reset_index().rename(columns = {"index": "id"})
        df["removed"] = df["removed"].replace("Still_avaliable", "Available")
        df["date_added"] = pd.to_datetime(df["date_added"], infer_datetime_format = True, errors = "coerce")
        return df
    
    def clean_orders(self, df):
        column_names = ["first_name", "last_name", "1", "level_0", "index"]
        df = df.drop(columns = column_names)
        return df
    
    def clean_events(self, df):
        df = df.dropna()
        df = df.drop_duplicates()
        df = df[df["time_period"].str.contains("Midday|Late_Hours|Evening|Morning")]
        return df
