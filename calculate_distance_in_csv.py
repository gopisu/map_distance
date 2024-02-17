import pandas as pd
import requests
import time
import os

API_KEY = os.environ['apikey_googl']
API_URL = 'https://maps.googleapis.com/maps/api/distancematrix/json?origins={}&destinations={}&key={}'
RETRY_LIMIT = 3


def get_distance(origin, destination):
    for i in range(RETRY_LIMIT):
        try:
            response = requests.get(API_URL.format(origin, destination, API_KEY))
            data = response.json()
            print(data)

            if data['status'] == 'OK':
                row = data['rows'][0]
                element = row['elements'][0]
                if element['status'] == 'OK':
                    return element['distance']['value'], element['distance']['text']
                else:
                    return 'Error', element['status']
            else:
                return 'Error', data['status']

        except requests.exceptions.RequestException as e:
            if i < RETRY_LIMIT - 1:  # i is zero indexed
                time.sleep(1)  # wait a bit before retrying
                continue
            else:
                return 'Error', str(e)

    return 'Error', 'Max retries exceeded'


def process_excel(input_filepath, output_directory):
    """Reads an Excel file and writes a new one with the distance and distance text added to each row."""

    df = pd.read_excel(input_filepath)

    try:
        for index, row in df.iterrows():
            distance, distance_text = get_distance(row['Loading_city'] + f" ,{row['Loading_address']}",
                                                   row['Unloading_city'] + f" ,{row['Unloading_address']}")
            df.loc[index, 'distance'] = distance
            df.loc[index, 'distance text'] = distance_text

    except KeyboardInterrupt:
        print("Interrupted by user, saving current progress...")

    finally:
        # Get a list of all files in the output directory
        files = os.listdir(output_directory)

        # Find the highest numbered file
        highest_num = 0
        for file in files:
            if file.endswith('.xlsx'):
                try:
                    num = int(file.split('.')[0])
                    if num > highest_num:
                        highest_num = num
                except ValueError:
                    continue

        # Increment the highest number for the new file
        output_filepath = os.path.join(output_directory, f'{highest_num + 1}.xlsx')

        df.to_excel(output_filepath, index=False)
        print(f"Data saved to {output_filepath}")


def process_cities(input_filepath, output_directory):
    """Reads an Excel file, modifies the 'Loading_city' and 'Loading_address' columns
    if 'Loading_country' is 'PL', and writes a new Excel file."""

    df = pd.read_excel(input_filepath)

    # Create new columns for the original values

    try:
        n = 1
        for index, row in df.iterrows():
            print(n)
            if row['Loading_country'] == 'PL':
                df.loc[index, 'Unloading_city'] = 'Warsaw'
                df.loc[index, 'Unloading_address'] = 'Annopol 4'

            elif row['Unloading_country'] == 'PL':
                df.loc[index, 'Loading_city'] = 'Warsaw'
                df.loc[index, 'Loading_address'] = 'Annopol 4'
            n = n + 1

    except KeyboardInterrupt:
        print("Interrupted by user, saving current progress...")

    finally:
        # Get a list of all files in the output directory
        files = os.listdir(output_directory)

        # Find the highest numbered file
        highest_num = 0
        for file in files:
            if file.endswith('.xlsx'):
                try:
                    num = int(file.split('.')[0])
                    if num > highest_num:
                        highest_num = num
                except ValueError:
                    continue

        # Increment the highest number for the new file
        output_filepath = os.path.join(output_directory, f'{highest_num + 1}.xlsx')

        df.to_excel(output_filepath, index=False)
        print(f"Data saved to {output_filepath}")


if __name__ == "__main__":
    process_cities(
        'csv/input/input.xlsx',
        'csv/output/cities'
    )
    process_excel('csv/input/input.xlsx', 'csv/output')
