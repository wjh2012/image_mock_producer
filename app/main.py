import io
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, BackgroundTasks
from uuid_extensions import uuid7str

from app.aio_boto import AioBoto
from app.aio_publisher import AioPublisher
from app.config import get_settings
from app.a4_text_image_maker import (
    get_single_a4_p,
    get_compressed_a4_mp,
    get_compressed_a4,
)
from app.publish_message import MessagePayload

config = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global rabbit_publisher, minio
    rabbit_publisher = AioPublisher()
    minio = AioBoto()

    await rabbit_publisher.connect()
    await minio.connect()

    yield
    await rabbit_publisher.close()
    await minio.close()


app = FastAPI(lifespan=lifespan)


async def process_image_upload():
    image_bytes = await get_single_a4_p()
    image_file = io.BytesIO(image_bytes)
    image_file.seek(0)

    prefix = config.original_image_object_key_prefix
    gid = uuid7str()
    short_uuid = gid[-8:]

    now = datetime.now()
    date_path = now.strftime("%Y/%m/%d")
    time_part = now.strftime("%H%M%S")

    gen_name = f"{date_path}/{prefix}/{time_part}_{short_uuid}.jpeg"

    bucket_name = f"{config.minio_bucket}-{config.run_mode}"

    await minio.upload_image_with_resource(
        file=image_file, bucket_name=bucket_name, key=gen_name
    )

    trace_id = uuid7str()
    body = MessagePayload(gid, bucket_name, gen_name)

    await rabbit_publisher.send_message(trace_id=trace_id, body=body)


async def process_compressed_image_upload(count=10):
    compressed_image_bytes = await get_compressed_a4(count)
    compressed_image = io.BytesIO(compressed_image_bytes)

    prefix = config.original_image_object_key_prefix
    gid = uuid7str()
    short_uuid = gid[-8:]

    now = datetime.now()
    date_path = now.strftime("%Y/%m/%d")
    time_part = now.strftime("%H%M%S")

    gen_name = f"{date_path}/{prefix}/{time_part}_{short_uuid}.zip"

    bucket_name = f"a4-ocr-{config.run_mode}"

    await minio.upload_image_with_resource(
        file=compressed_image, bucket_name=bucket_name, key=gen_name
    )

    trace_id = uuid7str()
    body = MessagePayload(gid, bucket_name, gen_name)

    await rabbit_publisher.send_message(trace_id=trace_id, body=body)


async def process_compressed_image_upload_mp(count=10):
    compressed_image_bytes = await get_compressed_a4_mp(count=count)
    compressed_image = io.BytesIO(compressed_image_bytes)

    prefix = config.original_image_object_key_prefix
    gid = uuid7str()
    short_uuid = gid[-8:]

    now = datetime.now()
    date_path = now.strftime("%Y/%m/%d")
    time_part = now.strftime("%H%M%S")

    gen_name = f"{date_path}/{prefix}/{time_part}_{short_uuid}.zip"

    bucket_name = f"a4-ocr-{config.run_mode}"

    await minio.upload_image_with_resource(
        file=compressed_image, bucket_name=bucket_name, key=gen_name
    )

    trace_id = uuid7str()
    body = MessagePayload(gid, bucket_name, gen_name)

    await rabbit_publisher.send_message(trace_id=trace_id, body=body)


@app.get("/async-produce-single-image", status_code=202)
async def produce_single_image(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_image_upload)
    return {"message": "produce single image and upload success"}


@app.get("/async-produce-compressed-image_mp", status_code=202)
async def async_produce_single_image(
    background_tasks: BackgroundTasks, count: int = 10
):
    background_tasks.add_task(process_compressed_image_upload_mp, count)
    return {"message": "produce compressed image and upload"}


@app.get("/async-produce-compressed-image", status_code=202)
async def async_produce_single_image(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_compressed_image_upload)
    return {"message": "produce compressed image and upload"}
