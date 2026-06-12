
import pandas as pd

def transform_cities(data):
    cities_df = pd.DataFrame(data)
    cities_df = cities_df.drop(columns="lowername")
    cities_df = cities_df.rename(columns={"name": "cities"})
    return cities_df