from extract import fetch_state_gas_prices
from load import load_cities
from transform import transform_cities

def main():
    state = "WA"
    data = fetch_state_gas_prices(state)
    cities_df = transform_cities(data)
    load_cities(cities_df)

    print('ETL process completed successfully.')


if __name__ == "__main__":
    main()  