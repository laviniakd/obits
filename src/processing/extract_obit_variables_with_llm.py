import anthropic
import os
import transformers
import pandas as pd
from tqdm import tqdm
import json
import re
from functools import partial
import random
import sys

# use sys to import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.data.load_data import load_jsons_to_dataframe
import multiprocessing
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_using_anthropic(client, obituary_list, MODEL, PROMPT, max_tokens=2000, temperature=1):
    responses = []
    for obit in obituary_list:
        message = client.messages.create(
            model=MODEL,
            max_tokens=max_tokens,
            temperature=temperature,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": PROMPT.replace("{{OBITUARY_TEXT}}", obit)
                        }
                    ]
                }
            ]
        )
        response = message.content[0].text
        print("Raw response:", response[:500])  # print first 500 chars of response
        responses.append(response)

    return responses


def postprocess_anthropic_responses(response_list):
    # attempt to parse as json
    parsed_responses = []
    for response in response_list:
        try:
            json_str = re.search(r'<output>\s*({.*?})\s*</output>', response, re.DOTALL).group(1)
            parsed_response = json.loads(json_str)
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            parsed_response = {
                "cause_of_death": None,
                "birth_date": None,
                "death_date": None,
                "birth_location": None,
                "age_at_death": None,
                "occupation": None,
                "donation_instructions": None
            }
        parsed_responses.append(parsed_response)
    return parsed_responses


def process_batch(batch, text_to_id, MODEL, PROMPT, outdir):
    client = anthropic.Anthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY"),
    )
    result = process_using_anthropic(client, batch, MODEL=MODEL, PROMPT=PROMPT)
    # save intermediate results
    os.makedirs(outdir, exist_ok=True)
    for i, (obit_text, response) in enumerate(zip(batch, result)):
        outpath = os.path.join(outdir, f"{text_to_id[obit_text]}.json")
        with open(outpath, 'w') as f:
            json.dump({"obituary_text": obit_text, "response": response}, f)

    logger.info(f"Processed batch of size {len(batch)}")
    return result


def main():
    DEBUG = False
    logging_dir = "/home/laviniad/projects/obits/logging/obit_extraction_claude"
    data_dir = "/home/laviniad/projects/obits/data/obit_jsons"
    out_dir = "/home/laviniad/projects/obits/data/claude_responses"
    obit_data = load_jsons_to_dataframe(data_dir)
    print("Initial obituaries loaded:", len(obit_data))

    # remove obituaries represented in out_dir
    existing_ids = {int(f.split('.')[0]) for f in os.listdir(out_dir) if f.endswith('.json')}
    obit_data = obit_data[~obit_data['id'].isin(existing_ids)]


    print("Obituaries loaded after filtering out ones already processed:", len(obit_data))
    print("head:", obit_data.head(10))

    obit_texts = [obit['text'] for idx, obit in obit_data.iterrows() if obit['text']]
    ids = [obit['id'] for idx, obit in obit_data.iterrows() if obit['text']]
    text_to_id = {obit['text']: obit['id'] for idx, obit in obit_data.iterrows() if obit['text']}
    random_obit = random.choice(obit_texts)

    print("Example obituary text:" \
    "\n" + "-"*80 + "\n" + random_obit[:1000] + "\n" + "-"*80)

    MODEL = "claude-3-5-haiku-20241022"
    PROMPT = "You will be analyzing an obituary text to extract specific information. The obituary text is provided below:\n\n<obituary>\n{{OBITUARY_TEXT}}\n</obituary>\n\nYour task is to extract the following information from the obituary:\n1. Cause of death (the sentence describing it)\n2. Birth date\n3. Death date\n4. Birth location\n5. Age at death\n6. Occupation\n7. Donation instructions\n\nFor each piece of information, carefully search the obituary text. If the information is explicitly stated, extract it. If it is not mentioned or unclear, use 'None' as the value.\n\nPay special attention to the cause of death. Look for a sentence that directly states or strongly implies how the person died. This might include phrases like \"passed away after a battle with...\", \"died suddenly of...\", or \"succumbed to...\". If no clear cause is given, use 'None'.\n\nAfter analyzing the text, provide your output in JSON format. Use the following structure:\n\n<output>\n{\n  \"cause_of_death\": \"Extracted sentence or 'None'\",\n  \"birth_date\": \"Extracted date or 'None'\",\n  \"death_date\": \"Extracted date or 'None'\",\n  \"birth_location\": \"Extracted location or 'None'\",\n  \"age_at_death\": \"Extracted age or 'None'\",\n  \"occupation\": \"Extracted occupation or 'None'\",\n  \"donation_instructions\": \"Extracted instructions or 'None'\"\n}\n</output>\n\nEnsure that your JSON is properly formatted and that each field contains either the extracted information or 'None' if the information is not provided in the obituary. Do not infer or guess any information that is not explicitly stated in the text. Do not explain your output after providing it."

    logger.info(f"Using model: {MODEL}")
    logger.info(f"Number of obituaries to process: {len(obit_texts)}")

    if DEBUG:
        obit_texts = obit_texts[:100]

    batch_size = 200
    batches = [obit_texts[i:i+batch_size] for i in range(0, len(obit_texts), batch_size)]

    process_batch_partial = partial(process_batch, text_to_id=text_to_id, MODEL=MODEL, PROMPT=PROMPT, outdir=out_dir)

    NUM_PROCESSES = 4
    with multiprocessing.Pool(processes=min(len(batches), NUM_PROCESSES)) as pool:
        all_responses_nested = list(tqdm(pool.imap(process_batch_partial, batches), total=len(batches)))

    all_responses = [resp for batch in all_responses_nested for resp in batch]

    logger.info(f"Beginning JSON postprocessing of {len(obit_texts)} obituaries in {len(batches)} batches...")
    parsed_data = postprocess_anthropic_responses(all_responses)

    # Combine with original data
    for obit, parsed in zip(obit_data, parsed_data):
        obit.update(parsed)

    # Save to CSV
    df = pd.DataFrame(obit_data)
    df.to_csv('obituaries_with_extracted_variables.csv', index=False)


if __name__ == "__main__":
    main()
