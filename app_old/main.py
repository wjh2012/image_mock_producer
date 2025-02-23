import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, BackgroundTasks

from app_old.a4_text_image_maker import compressed_a4, get_single_a4
from app_old.config import get_settings
from app_old.minio_client import get_minio_client, upload_file, ContentType
from app_old.rabbitmq_publisher import get_rabbitmq_publisher

config = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global rabbitmq_publisher, minio_client

    rabbitmq_publisher = get_rabbitmq_publisher()
    print("✅ RabbitMQ 연결이 설정되었습니다.")
    minio_client = get_minio_client()
    print("✅ MinIO 클라이언트가 설정되었습니다.")

    yield

    rabbitmq_publisher.close()
    print("🛑 RabbitMQ 연결이 종료되었습니다.")
    print("🛑 MinIO 연결이 종료되었습니다.")


app = FastAPI(lifespan=lifespan)


async def process_image_upload():
    image = await get_single_a4()

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.jpeg"

    bucket_name = f"a4-ocr-{config.run_mode}"

    upload_file(
        minio_client=minio_client,
        bucket_name=bucket_name,
        object_name=gen_name,
        file_data=image,
        content_type=ContentType.IMAGE_JPEG,
    )

    message = {"file_name": gen_name, "bucket": bucket_name, "timestamp": timestamp}
    rabbitmq_publisher.publish_message(json.dumps(message))


async def process_compressed_image_upload():
    compressed_image = await compressed_a4()

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.zip"

    bucket_name = f"a4-ocr-{config.run_mode}"

    upload_file(
        minio_client=minio_client,
        bucket_name=bucket_name,
        object_name=gen_name,
        file_data=compressed_image,
        content_type=ContentType.APPLICATION_ZIP,
    )
    message = {"file_name": gen_name, "bucket": bucket_name, "timestamp": timestamp}
    rabbitmq_publisher.publish_message(json.dumps(message))


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/produce-single-image", status_code=201)
async def produce_single_image():
    await process_image_upload()
    return {"message": "produce single image and upload success"}


@app.get("/produce-compressed-image", status_code=201)
async def produce_compressed_image():
    await process_compressed_image_upload()
    return {"message": "produce compressed image and upload success"}


@app.get("/async-produce-single-image", status_code=202)
async def async_produce_single_image(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_image_upload)
    return {"message": "produce compressed image and upload"}


@app.get("/async-produce-compressed-image", status_code=202)
async def async_produce_single_image(background_tasks: BackgroundTasks):
    background_tasks.add_task(process_compressed_image_upload)
    return {"message": "produce compressed image and upload"}
