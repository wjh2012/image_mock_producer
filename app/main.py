import uuid
from datetime import datetime

from fastapi import FastAPI, Depends

from app.a4_text_image_maker import compressed_a4, get_single_a4
from app.minio_client import get_minio_client, upload_file, ContentType
from app.rabbitmq_publisher import RabbitMQPublisher, get_rabbitmq_publisher

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/produce-single-image")
async def produce_single_image(
    minio_client=Depends(get_minio_client),
):
    image = await get_single_a4()

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.jpeg"

    upload_file(
        minio_client=minio_client,
        bucket_name="upload-test",
        object_name=gen_name,
        file_data=image,
        content_type=ContentType.IMAGE_JPEG,
    )
    return {"message": "produce a4 success"}


@app.get("/produce-compressed-image")
async def produce_compressed_image(
    minio_client=Depends(get_minio_client),
):
    compressed_image = await compressed_a4()

    prefix = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    gen_name = f"{prefix}/{timestamp}_{short_uuid}.zip"

    upload_file(
        minio_client=minio_client,
        bucket_name="upload-test",
        object_name=gen_name,
        file_data=compressed_image,
        content_type=ContentType.APPLICATION_ZIP,
    )

    return {"message": "Hello World"}
