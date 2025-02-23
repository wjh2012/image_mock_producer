from enum import Enum

import boto3
import botocore

from app_old.config import get_settings
from app_old.custom_logger import logger

config = get_settings()


class ContentType(Enum):
    IMAGE_JPEG = "image/jpeg"
    IMAGE_PNG = "image/png"
    IMAGE_GIF = "image/gif"
    IMAGE_WEBP = "image/webp"
    APPLICATION_ZIP = "application/zip"
    APPLICATION_PDF = "application/pdf"
    TEXT_PLAIN = "text/plain"
    TEXT_HTML = "text/html"
    APPLICATION_JSON = "application/json"
    APPLICATION_XML = "application/xml"
    APPLICATION_OCTET_STREAM = "application/octet-stream"


def get_minio_client() -> boto3.client:
    minio_url = f"http://{config.minio_host}:{config.minio_port}"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=minio_url,
            aws_access_key_id=config.minio_username,
            aws_secret_access_key=config.minio_password,
            aws_session_token=None,
            config=boto3.session.Config(signature_version="s3v4"),
        )
        return client
    except Exception as e:
        raise Exception(f"❌ MinIO 연결 실패: {e}")


def get_bucket_list(minio_client: boto3.client):
    try:
        response = minio_client.list_buckets()
        buckets = response.get("Buckets", [])
        return buckets
    except Exception as e:
        raise Exception(f"❌ MinIO 버킷 조회 실패: {e}")


def upload_file(
    minio_client: boto3.client,
    bucket_name,
    object_name,
    file_data,
    content_type: ContentType,
):
    try:
        ensure_bucket(minio_client, bucket_name)
        response = minio_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=file_data,
            ContentType=content_type.value,
        )

        http_status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", None)

        if http_status == 200:
            logger.info(
                f"✅ MinIO 파일 업로드 성공: {object_name} (Bucket: {bucket_name})"
            )
            return response
        else:
            error_msg = f"❌ MinIO 파일 업로드 실패 (Status: {http_status})"
            logger.error(error_msg)
            raise Exception(error_msg)

    except Exception as e:
        error_msg = f"❌ MinIO 파일 업로드 실패: {e}"
        logger.error(error_msg)
        raise Exception(error_msg)


def ensure_bucket(minio_client: boto3.client, bucket_name):
    try:
        minio_client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("400", "404", "NoSuchBucket"):
            try:
                minio_client.create_bucket(Bucket=bucket_name)
            except Exception as create_error:
                raise Exception(f"❌ 버킷 생성 실패: {create_error}")
        else:
            raise Exception(f"❌ 버킷 확인 실패: {e}")
