import io
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, BackgroundTasks

from app.aio_boto import AioBoto
from app.aio_publisher import AioPublisher
from app.config import get_settings
from app.a4_text_image_maker import (
    get_single_a4_p,
    get_compressed_a4_mp,
    get_compressed_a4,
)

config = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global rabbit_publisher, minio
    rabbit_publisher = AioPublisher(
        f"amqp://{config.rabbitmq_username}:{config.rabbitmq_password}@{config.rabbitmq_host}:{config.rabbitmq_port}"
    )
    minio = AioBoto(f"http://{config.minio_host}:{config.minio_port}")

    await rabbit_publisher.connect()
    await minio.connect()

    yield
    await rabbit_publisher.close()
    await minio.close()


app = FastAPI(lifespan=lifespan)


async def process_image_upload():
    image_bytes = await get_single_a4_p()
    image_file = io.BytesIO(image_bytes)

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.jpeg"

    bucket_name = f"a4-ocr-{config.run_mode}"

    await minio.uploadFile(file=image_file, bucket_name=bucket_name, key=gen_name)

    message = {"file_name": gen_name, "bucket": bucket_name, "timestamp": timestamp}
    await rabbit_publisher.send_message("image_validation", json.dumps(message))


async def process_compressed_image_upload(count=10):
    compressed_image_bytes = await get_compressed_a4(count)
    compressed_image = io.BytesIO(compressed_image_bytes)

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.zip"

    bucket_name = f"a4-ocr-{config.run_mode}"

    await minio.uploadFile(file=compressed_image, bucket_name=bucket_name, key=gen_name)

    message = {"file_name": gen_name, "bucket": bucket_name, "timestamp": timestamp}
    await rabbit_publisher.send_message("image_validation", json.dumps(message))


async def process_compressed_image_upload_mp(count=10):
    compressed_image_bytes = await get_compressed_a4_mp(count=count)
    compressed_image = io.BytesIO(compressed_image_bytes)

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.zip"

    bucket_name = f"a4-ocr-{config.run_mode}"
    await minio.uploadFile(file=compressed_image, bucket_name=bucket_name, key=gen_name)

    message = {"file_name": gen_name, "bucket": bucket_name, "timestamp": timestamp}
    await rabbit_publisher.send_message("image_validation", json.dumps(message))


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
