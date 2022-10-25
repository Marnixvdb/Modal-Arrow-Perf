import modal
import pyarrow
import pyarrow.parquet
from fastapi import FastAPI
from fastapi.responses import Response
from io import BytesIO
import requests
from uuid import uuid4
from datetime import datetime
from pathlib import Path


image = modal.Image.debian_slim().pip_install(['pyarrow', 'requests'])

volume = modal.SharedVolume().persist("model-cache-vol")
CACHE_DIR = "/cache"

stub = modal.Stub(
    image=image,
    name='arrow_sender',
    mounts=[
        modal.Mount(remote_dir="/root/data", local_dir="./data")
    ]
)

web_app = FastAPI()


@web_app.get("/network")
def send_data():
    table = pyarrow.parquet.read_table(
        '/root/data/yellow_tripdata_2020-06.parquet')

    # create binary outputstream
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    buf = sink.getvalue()

    return Response(bytes(buf))


@web_app.get("/shared_volume")
def send_id():
    import pyarrow.feather
    import pyarrow.parquet

    # read table from mount
    table = pyarrow.parquet.read_table(
        '/root/data/yellow_tripdata_2020-06.parquet')

    # assign random id, store table as feather
    id = uuid4()
    output_path = f"{CACHE_DIR}/{str(id)}.feather"
    pyarrow.feather.write_feather(table, output_path)

    # return id to allow receiver to load file
    return {"file_id": str(id)}


@stub.function
def as_function_result():
    import pyarrow.parquet

    # read table from mount
    table = pyarrow.parquet.read_table(
        '/root/data/yellow_tripdata_2020-06.parquet')

    return table


@stub.asgi(image=image, shared_volumes={CACHE_DIR: volume}, mounts=[
    modal.Mount(remote_dir="/root/data", local_dir="./data")
])
def fastapi_app():
    return web_app


if __name__ == "__main__":
    stub.serve()
