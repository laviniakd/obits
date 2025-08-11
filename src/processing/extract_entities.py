# for each of the obituaries, use NLTK to extract entities like names, dates, locations, etc.
# save the results to a json file
import os
import re
import json
import nltk
import pandas as pd
from tqdm import tqdm
import sys
import logging

from src.data.load_data import load_jsons_to_dataframe

from nltk import word_tokenize, pos_tag, ne_chunk
import nltk
nltk.download('averaged_perceptron_tagger_eng')
nltk.download('maxent_ne_chunker_tab')


DATA_DIR = "/home/laviniad/projects/obits/data/"


def infer_entities(text):
    entities = []
    for sentence in tqdm(text):
        tokens = word_tokenize(sentence)
        tags = pos_tag(tokens)
        chunks = ne_chunk(tags)
        entities.append(chunks)
    return entities


def main():
    df = load_jsons_to_dataframe()
    files_in_dir = os.listdir(DATA_DIR)
    # drop rows of df where id is in files_in_dir
    df = df[~df['id'].isin([f.replace('.json', '') for f in files_in_dir])]
    print(f"Processing {len(df)} obituaries")

    text = df['text'].tolist()
    ids = df['id'].tolist()
    entities = infer_entities(text)
    
    print("Saving entities...")
    for (id, entities) in zip(ids, entities):
        entity_list = []
        for chunk in entities:
            if hasattr(chunk, 'label'):
                entity_list.append((chunk.label(), ' '.join(c[0] for c in chunk)))
        entities_dict = {
            "id": id,
            "entities": entity_list
        }
        with open(os.path.join(DATA_DIR, f"{id}.json"), 'w') as f:
            json.dump(entities_dict, f)
    

if __name__ == "__main__":
    main()
