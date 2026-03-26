"""
N-gram language model analysis for obituaries.

Fits a Kneser-Ney interpolated n-gram LM on all data and one per class value
of a chosen categorical column. Reports:
  - Top-K n-grams per class (by frequency)
  - Per-document perplexity under the overall and per-class models
  - Log-odds of n-grams between each class and the rest

Usage:
    python src/analysis/ngram_lm.py --column inferred_gender
    python src/analysis/ngram_lm.py --column funeral_location_state --n 3 --sample 5000
"""

import argparse
import os
import sys
import logging

import pandas as pd
import numpy as np
import nltk
from nltk.lm import KneserNeyInterpolated
from nltk.lm.preprocessing import padded_everygram_pipeline
from sklearn.feature_extraction.text import CountVectorizer

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from processing.log_odds import find_distinctive_words

nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def tokenize(text):
    return nltk.word_tokenize(text.lower())


def train_ngram_lm(tokenized_docs, n):
    train_data, padded_vocab = padded_everygram_pipeline(n, tokenized_docs)
    model = KneserNeyInterpolated(n)
    model.fit(train_data, padded_vocab)
    return model


def compute_perplexity(model, tokenized_doc, n):
    test_data, _ = padded_everygram_pipeline(n, [tokenized_doc])
    ngrams = list(list(test_data)[0])
    try:
        return model.perplexity(ngrams)
    except Exception:
        return float('inf')


def top_k_ngrams(texts, n, k):
    vectorizer = CountVectorizer(ngram_range=(n, n), max_features=None)
    X = vectorizer.fit_transform(texts)
    counts = np.asarray(X.sum(axis=0)).flatten()
    features = vectorizer.get_feature_names_out()
    top_idx = np.argsort(counts)[-k:][::-1]
    return [(features[i], int(counts[i])) for i in top_idx]


def main():
    parser = argparse.ArgumentParser(description='N-gram LM analysis per categorical column')
    parser.add_argument('--data', default='obit_data.csv', help='Path to CSV data file')
    parser.add_argument('--column', required=True, help='Categorical column to split by')
    parser.add_argument('--text-col', default='text', help='Column containing obituary text')
    parser.add_argument('--n', type=int, default=2, help='N-gram order')
    parser.add_argument('--top-k', type=int, default=20, help='Top K n-grams per class')
    parser.add_argument('--output-dir', default='output/ngram_lm', help='Output directory')
    parser.add_argument('--sample', type=int, default=None, help='Subsample N rows for speed')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    logger.info(f"Loading data from {args.data}")
    df = pd.read_csv(args.data, low_memory=False)
    df = df.dropna(subset=[args.text_col, args.column])
    df = df[df[args.text_col].str.strip().astype(bool)]

    if args.sample:
        df = df.sample(n=min(args.sample, len(df)), random_state=42)
        logger.info(f"Sampled {len(df)} rows")

    logger.info(f"Working with {len(df)} documents, column='{args.column}', n={args.n}")

    classes = sorted(df[args.column].unique())
    logger.info(f"Classes ({len(classes)}): {classes}")

    # --- Tokenize all documents ---
    logger.info("Tokenizing documents...")
    all_texts = df[args.text_col].tolist()
    all_tokens = [tokenize(t) for t in all_texts]

    # --- Overall model ---
    logger.info("Training overall n-gram LM...")
    overall_model = train_ngram_lm(list(all_tokens), args.n)

    # --- Per-class models and top-K n-grams ---
    class_models = {}
    class_indices = {}
    for cls in classes:
        mask = df[args.column] == cls
        class_indices[cls] = df.index[mask].tolist()
        cls_tokens = [all_tokens[i] for i in range(len(df)) if df.iloc[i][args.column] == cls]
        cls_texts = df.loc[mask, args.text_col].tolist()

        logger.info(f"Training LM for class '{cls}' ({len(cls_tokens)} docs)...")
        class_models[cls] = train_ngram_lm(list(cls_tokens), args.n)

        top_ngrams = top_k_ngrams(cls_texts, args.n, args.top_k)
        print(f"\nTop {args.top_k} {args.n}-grams for class '{cls}':")
        for ngram, count in top_ngrams:
            print(f"  {ngram!r:40s}  {count}")

        top_df = pd.DataFrame(top_ngrams, columns=['ngram', 'count'])
        top_df.to_csv(os.path.join(args.output_dir, f'top_ngrams_{cls}.csv'), index=False)

    # --- Perplexity ---
    logger.info("Computing perplexity scores...")
    perplexity_rows = []
    for i, (idx, row) in enumerate(df.iterrows()):
        tokens = all_tokens[i]
        cls = row[args.column]
        overall_ppl = compute_perplexity(overall_model, tokens, args.n)
        class_ppl = compute_perplexity(class_models[cls], tokens, args.n)
        perplexity_rows.append({
            'id': row.get('id', idx),
            args.column: cls,
            'overall_perplexity': overall_ppl,
            f'{cls}_perplexity': class_ppl,
        })
        if (i + 1) % 500 == 0:
            logger.info(f"  Perplexity: {i+1}/{len(df)} done")

    ppl_df = pd.DataFrame(perplexity_rows)
    ppl_path = os.path.join(args.output_dir, 'perplexity.csv')
    ppl_df.to_csv(ppl_path, index=False)
    logger.info(f"Perplexity saved to {ppl_path}")

    print("\nPerplexity summary (mean per class):")
    print(ppl_df.groupby(args.column)[['overall_perplexity']].mean().to_string())

    # --- Log-odds (one-vs-rest per class) ---
    logger.info("Computing log-odds (one-vs-rest)...")
    for cls in classes:
        mask = df[args.column] == cls
        corpus_cls = df.loc[mask, args.text_col].tolist()
        corpus_rest = df.loc[~mask, args.text_col].tolist()

        if len(corpus_cls) == 0 or len(corpus_rest) == 0:
            continue

        result = find_distinctive_words(
            corpus_cls, corpus_rest,
            top_n=args.top_k,
            filter_stopwords_etc=True
        )

        lo_df = pd.DataFrame({
            f'top_{cls}': result['corpus_a'],
            f'top_{cls}_log_odds': result['corpus_a_values'],
            'top_rest': result['corpus_b'],
            'top_rest_log_odds': result['corpus_b_values'],
        })
        lo_path = os.path.join(args.output_dir, f'log_odds_{cls}.csv')
        lo_df.to_csv(lo_path, index=False)

        print(f"\nLog-odds top words for '{cls}' vs rest:")
        print(f"  Distinctive for '{cls}': {list(result['corpus_a'])}")
        print(f"  Distinctive for rest:    {list(result['corpus_b'])}")

    logger.info(f"All outputs written to {args.output_dir}/")


if __name__ == '__main__':
    main()
