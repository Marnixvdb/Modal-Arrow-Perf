import modal
import requests
import pyarrow
from io import BytesIO
from datetime import datetime

image = modal.Image.debian_slim().pip_install(['pyarrow', 'requests'])

# replace with generated for the arrow_sender app
sender_url = 'https://bundlesandbatches-arrow-sender-fastapi-app.modal.run'

stub = modal.Stub(
    image=image,
    name='arrow_receiver'
)
volume = modal.SharedVolume().persist("model-cache-vol")
CACHE_DIR = "/cache"


@stub.function()
def get_data_over_network():
    start = datetime.now()
    # get binary data
    data = requests.get(
        f'{sender_url}/network')

    # open response as input stream and return the processing time and metadata of the table
    with pyarrow.ipc.open_stream(BytesIO(data.content)) as reader:
        batches = [b for b in reader]
        table = pyarrow.Table.from_batches(batches)
        processing_time = datetime.now() - start

        result = f"Table with {table.num_columns} columns, {table.num_rows} rows, totaling {table.nbytes} bytes was processed in {processing_time}"
        return result


@stub.function(shared_volumes={CACHE_DIR: volume})
def get_data_from_shared_volume():
    import pyarrow.feather
    import os

    start = datetime.now()
    # get id of file
    id = requests.get(
        f'{sender_url}/shared_volume').json()['file_id']

    # construct path, read file and return the processing time and metadata of the table
    input_path = f"{CACHE_DIR}/{id}.feather"
    table = pyarrow.feather.read_table(input_path)
    processing_time = datetime.now() - start

    result = f"Table with {table.num_columns} columns, {table.num_rows} rows, totaling {table.nbytes} bytes was processed in {processing_time}"

    os.remove(input_path)

    return result


@stub.function
def get_data_as_function_result():
    start = datetime.now()
    as_function_result = modal.lookup('arrow_sender', 'as_function_result')
    table = as_function_result()
    processing_time = datetime.now() - start

    result = f"Table with {table.num_columns} columns, {table.num_rows} rows, totaling {table.nbytes} bytes was processed in {processing_time}"

    return result
