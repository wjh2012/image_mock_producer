import aioboto3
from app.config import get_settings
from app.custom_logger import time_logger

config = get_settings()


class AioBoto:
    def __init__(self, minio_url: str):
        self.minio_url = minio_url
        self._session = None
        self.s3_resource_cm = None
        self.s3_resource = None
        self.s3_client_cm = None
        self.s3_client = None

    async def connect(self):
        self._session = aioboto3.Session()
        self.s3_resource_cm = self._session.resource(
            "s3",
            endpoint_url=self.minio_url,
            aws_access_key_id=config.minio_username,
            aws_secret_access_key=config.minio_password,
        )
        self.s3_resource = await self.s3_resource_cm.__aenter__()

        self.s3_client_cm = self._session.client(
            "s3",
            endpoint_url=self.minio_url,
            aws_access_key_id=config.minio_username,
            aws_secret_access_key=config.minio_password,
        )
        self.s3_client = await self.s3_client_cm.__aenter__()

        print("✅ Minio 연결 성공")

    @time_logger
    async def upload_image_with_resource(self, file, bucket_name: str, key: str):
        bucket = await self.s3_resource.Bucket(bucket_name)
        await bucket.upload_fileobj(file, key)
        print(f"✅ MinIO resource 파일 업로드 성공: {key} (Bucket: {bucket_name})")

    @time_logger
    async def upload_image_with_client(self, file, bucket_name: str, key: str):
        await self.s3_client.upload_fileobj(file, Bucket=bucket_name, Key=key)
        print(f"✅ MinIO client 파일 업로드 성공: {key} (Bucket: {bucket_name})")

    async def close(self):
        if self.s3_resource_cm:
            await self.s3_resource_cm.__aexit__(None, None, None)
        if self.s3_client_cm:
            await self.s3_client_cm.__aexit__(None, None, None)
        print("❌ Minio 연결 종료")
