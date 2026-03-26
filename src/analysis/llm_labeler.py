"""
Label obituary variables using a small local HuggingFace instruction-following model.

Extracts four structured fields from each obituary:
  - birth_year
  - death_year
  - cause_of_death
  - occupation

Outputs a CSV with the original data columns plus the four new label columns.
Supports --resume to skip already-processed rows, and --sample for quick tests.

Usage:
    python src/analysis/llm_labeler.py --sample 10
    python src/analysis/llm_labeler.py --model Qwen/Qwen2.5-1.5B-Instruct --resume
    python src/analysis/llm_labeler.py --data obit_data.csv --output output/llm_labels.csv
"""

import argparse
import json
import logging
import os
import re
import sys

import pandas as pd
from tqdm import tqdm
from transformers import pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

LABEL_FIELDS = ['birth_year', 'death_year', 'cause_of_death', 'occupation']

PROMPT_TEMPLATE = """You are analyzing an obituary to extract specific information.

<obituary>
{text}
</obituary>

Extract the following fields:
1. birth_year: The 4-digit year the person was born. Use null if not stated.
2. death_year: The 4-digit year the person died. Use null if not stated.
3. cause_of_death: The cause of death as described in the obituary itself---quote directly and limit to a short phrase or a word. Use null if not stated.
4. occupation: The person's primary occupation or profession. Use null if not stated.

Respond ONLY with a JSON object inside <output> tags. Example:
<output>
{{"birth_year": 1942, "death_year": 2021, "cause_of_death": "cancer", "occupation": "teacher"}}
</output>

Do not include any explanation outside the <output> tags."""

EMPTY_LABELS = {f: None for f in LABEL_FIELDS}


def parse_response(response_text):
    match = re.search(r'<output>\s*(\{.*?\})\s*</output>', response_text, re.DOTALL)
    if not match:
        return dict(EMPTY_LABELS)
    try:
        parsed = json.loads(match.group(1))
        return {f: parsed.get(f) for f in LABEL_FIELDS}
    except json.JSONDecodeError:
        return dict(EMPTY_LABELS)


def truncate_text(text, max_chars=2000):
    """Truncate long obituaries to keep inference fast on small models."""
    return text[:max_chars] if len(text) > max_chars else text


def main():
    parser = argparse.ArgumentParser(description='Label obituary variables with a local LLM')
    parser.add_argument('--data', default='obit_data.csv', help='Path to input CSV')
    parser.add_argument('--output', default='output/llm_labels.csv', help='Path to output CSV')
    parser.add_argument('--model', default='Qwen/Qwen2.5-1.5B-Instruct',
                        help='HuggingFace model name or local path')
    parser.add_argument('--sample', type=int, default=None,
                        help='Process only N rows (random sample)')
    parser.add_argument('--resume', action='store_true',
                        help='Skip rows whose id already appears in --output')
    parser.add_argument('--text-col', default='text', help='Column containing obituary text')
    parser.add_argument('--max-new-tokens', type=int, default=200,
                        help='Max tokens to generate per response')
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)

    logger.info(f"Loading data from {args.data}")
    df = pd.read_csv(args.data, low_memory=False)
    df = df.dropna(subset=[args.text_col])
    df = df[df[args.text_col].str.strip().astype(bool)].reset_index(drop=True)
    logger.info(f"Loaded {len(df)} rows with non-empty text")

    if args.resume and os.path.exists(args.output):
        existing = pd.read_csv(args.output, low_memory=False)
        done_ids = set(existing['id'].astype(str))
        df = df[~df['id'].astype(str).isin(done_ids)].reset_index(drop=True)
        logger.info(f"Resuming: {len(done_ids)} already done, {len(df)} remaining")

    if args.sample:
        df = df.sample(n=min(args.sample, len(df)), random_state=42).reset_index(drop=True)
        logger.info(f"Sampled {len(df)} rows")

    logger.info(f"Loading model: {args.model}")
    pipe = pipeline(
        'text-generation',
        model=args.model,
        device_map='auto',
        trust_remote_code=True,
    )
    # Disable sampling for deterministic extraction
    gen_kwargs = dict(
        max_new_tokens=args.max_new_tokens,
        do_sample=False,
        temperature=None,
        top_p=None,
    )

    results = []
    write_mode = 'a' if (args.resume and os.path.exists(args.output)) else 'w'
    write_header = not (args.resume and os.path.exists(args.output))
    BATCH_WRITE = 100

    for i, (_, row) in enumerate(tqdm(df.iterrows(), total=len(df), desc='Labeling')):
        text = truncate_text(str(row[args.text_col]))
        prompt = PROMPT_TEMPLATE.format(text=text)

        try:
            out = pipe(prompt, **gen_kwargs)
            generated = out[0]['generated_text']
            # Strip the input prompt from the output (some models include it)
            if generated.startswith(prompt):
                generated = generated[len(prompt):]
            labels = parse_response(generated)
        except Exception as e:
            logger.warning(f"Row {i} failed: {e}")
            labels = dict(EMPTY_LABELS)

        record = row.to_dict()
        record.update(labels)
        results.append(record)

        if len(results) >= BATCH_WRITE:
            chunk = pd.DataFrame(results)
            chunk.to_csv(args.output, mode=write_mode, header=write_header, index=False)
            write_mode = 'a'
            write_header = False
            results = []
            logger.info(f"Wrote batch at row {i+1}")

    if results:
        chunk = pd.DataFrame(results)
        chunk.to_csv(args.output, mode=write_mode, header=write_header, index=False)

    logger.info(f"Done. Output saved to {args.output}")

    # Print a quick preview of extracted labels
    final = pd.read_csv(args.output, low_memory=False)
    print(f"\nLabeled {len(final)} rows. Sample output:")
    print(final[['id'] + LABEL_FIELDS].head(5).to_string(index=False))


if __name__ == '__main__':
    main()
