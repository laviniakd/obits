## want to sample jsons from data/obit_jsons and create a sample csv with text, id, and several empty label columns:
# sentence that describes cause of death, birth date, death date, age, birth location, and occupation

import os
import pandas as pd
import json
from datetime import datetime
from tqdm import tqdm

def create_annotation_sample(data_dir, sample_size=100):
    df = sample_json_data(data_dir, sample_size)

    # Define the columns for the CSV
    columns = ['text', 'id', 'cause_of_death_sentence',
               'birth_date', 'death_date', 'age', 'birth_location', 'occupation']
    for col in columns:
        if col not in df.columns:
            df[col] = ""

    # Save the DataFrame to a CSV file
    df.to_csv('annotation_sample.csv', index=False)

def sample_json_data(data_dir, sample_size):
    json_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    sampled_data = []

    for json_file in tqdm(json_files[:sample_size]):
        with open(os.path.join(data_dir, json_file), 'r') as f:
            data = json.load(f)
            # Process the JSON data as needed
            sampled_data.append(data)

    # Create a DataFrame from the sampled data
    df = pd.DataFrame(sampled_data)
    return df

if __name__ == "__main__":
    data_dir = "/home/laviniad/projects/obits/data/obit_jsons"
    just_get_obit_text = True

    if just_get_obit_text:
        df = sample_json_data(data_dir, sample_size=25)
        df.rename(columns={'text': 'OBITUARY_TEXT'}, inplace=True)
        df = df[['OBITUARY_TEXT', 'id']]
        df.to_csv('obit_text_sample.csv', index=False)
    else:
        create_annotation_sample(data_dir, sample_size=500)
