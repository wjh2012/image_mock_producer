import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, BackgroundTasks

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
    global rabbit_publisher
    rabbit_publisher = AioPublisher("amqp://admin:admin@192.168.45.131/")
    await rabbit_publisher.connect()

    yield
    await rabbit_publisher.close()


app = FastAPI(lifespan=lifespan)


async def process_image_upload():
    image = await get_single_a4_p()

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.jpeg"

    bucket_name = f"a4-ocr-{config.run_mode}"

    message = {"file_name": gen_name, "bucket": bucket_name, "timestamp": timestamp}
    await rabbit_publisher.send_message("image_validation", json.dumps(message))


async def process_compressed_image_upload():
    compressed_image = await get_compressed_a4()

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.zip"

    bucket_name = f"a4-ocr-{config.run_mode}"

    message = {"file_name": gen_name, "bucket": bucket_name, "timestamp": timestamp}
    await rabbit_publisher.send_message("image_validation", json.dumps(message))


async def process_compressed_image_upload_mp():
    compressed_image = await get_compressed_a4_mp()

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.zip"

    bucket_name = f"a4-ocr-{config.run_mode}"

    message = {"file_name": gen_name, "bucket": bucket_name, "timestamp": timestamp}
    await rabbit_publisher.send_message("image_validation", json.dumps(message))


@app.get("/async-produce-single-image", status_code=202)
async def produce_single_image(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_image_upload)
    return {"message": "produce single image and upload success"}


@app.get("/async-produce-compressed-image_mp", status_code=202)
async def async_produce_single_image(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_compressed_image_upload_mp)
    return {"message": "produce compressed image and upload"}


@app.get("/async-produce-compressed-image", status_code=202)
async def async_produce_single_image(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_compressed_image_upload)
    return {"message": "produce compressed image and upload"}
