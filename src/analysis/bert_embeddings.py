"""
Embed obituary texts using a sentence-transformers model.

Produces a numpy array of embeddings and a companion CSV mapping
row indices to obituary IDs so results can be joined back to the
original dataset.

Usage:
    python src/analysis/bert_embeddings.py
    python src/analysis/bert_embeddings.py --model sentence-transformers/all-mpnet-base-v2 --sample 1000
    python src/analysis/bert_embeddings.py --output output/embeddings.npy --batch-size 32
"""

import argparse
import os
import logging

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Embed obituary texts with a BERT-like model')
    parser.add_argument('--data', default='obit_data.csv', help='Path to CSV data file')
    parser.add_argument('--text-col', default='text', help='Column containing obituary text')
    parser.add_argument('--model', default='sentence-transformers/all-MiniLM-L6-v2',
                        help='sentence-transformers model name or path')
    parser.add_argument('--output', default='output/embeddings.npy',
                        help='Path for output .npy embeddings file')
    parser.add_argument('--ids-output', default='output/embedding_ids.csv',
                        help='Path for companion CSV mapping embedding rows to obituary IDs')
    parser.add_argument('--batch-size', type=int, default=64, help='Encoding batch size')
    parser.add_argument('--sample', type=int, default=None,
                        help='Randomly subsample N rows before embedding')
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(args.ids_output) or '.', exist_ok=True)

    logger.info(f"Loading data from {args.data}")
    df = pd.read_csv(args.data, low_memory=False)
    df = df.dropna(subset=[args.text_col])
    df = df[df[args.text_col].str.strip().astype(bool)].reset_index(drop=True)
    logger.info(f"Loaded {len(df)} documents with non-empty text")

    if args.sample:
        df = df.sample(n=min(args.sample, len(df)), random_state=42).reset_index(drop=True)
        logger.info(f"Sampled {len(df)} rows")

    texts = df[args.text_col].tolist()

    logger.info(f"Loading model: {args.model}")
    model = SentenceTransformer(args.model)

    logger.info(f"Encoding {len(texts)} texts (batch_size={args.batch_size})...")
    embeddings = model.encode(
        texts,
        batch_size=args.batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
    )

    np.save(args.output, embeddings)
    logger.info(f"Embeddings saved to {args.output} — shape: {embeddings.shape}")

    id_col = df['id'] if 'id' in df.columns else df.index.to_series().rename('id')
    ids_df = pd.DataFrame({'embedding_index': range(len(df)), 'id': id_col.values})
    ids_df.to_csv(args.ids_output, index=False)
    logger.info(f"ID mapping saved to {args.ids_output}")

    print(f"\nDone.")
    print(f"  Embeddings : {args.output}  shape={embeddings.shape}")
    print(f"  ID mapping : {args.ids_output}")


if __name__ == '__main__':
    main()
