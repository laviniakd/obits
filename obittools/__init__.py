import os
import stat
from obittools.extract_data import parse_page_metadata_from_schemas_in_html

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))


def initialize_collection(collection_name: str):
    if not os.path.exists(os.path.join(ROOT_DIR, "collections")):
        os.makedirs(os.path.join(ROOT_DIR, "collections"))
        try:
            os.chmod(os.path.join(ROOT_DIR, "collections"),
                     stat.S_IRWXU | stat.S_IRGRP | stat.S_IRWXO)
        except Exception as e:
            print(e)
    if not os.path.exists(os.path.join(ROOT_DIR, "collections", collection_name)):
        os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name))
        os.chmod(os.path.join(ROOT_DIR, "collections", collection_name),
                 stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, "metadata"))
        os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, "metadata"),
                 stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, "queries"))
        os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, "queries"),
                 stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    return os.path.join(ROOT_DIR, "collections", collection_name)
