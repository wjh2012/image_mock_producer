import uuid
from datetime import datetime

import boto3
from fastapi import FastAPI, Depends

from app.a4_text_image_maker import compressed_a4, get_single_a4
from app.config import get_settings
from app.minio_client import get_minio_client, upload_file, ContentType
from app.rabbitmq_publisher import RabbitMQPublisher, get_rabbitmq_publisher

app = FastAPI()
config = get_settings()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/produce-single-image")
async def produce_single_image(
    minio_client: boto3.client = Depends(get_minio_client),
    rabbitmq_publisher: RabbitMQPublisher = Depends(get_rabbitmq_publisher),
):
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
    rabbitmq_publisher.publish_message(gen_name)

    return {"message": "produce a4 success"}


@app.get("/produce-compressed-image")
async def produce_compressed_image(
    minio_client: boto3.client = Depends(get_minio_client),
    rabbitmq_publisher: RabbitMQPublisher = Depends(get_rabbitmq_publisher),
):
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
    rabbitmq_publisher.publish_message(gen_name)

    return {"message": "Hello World"}
