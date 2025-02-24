import os
import threading

import aioboto3
from app.config import get_settings

config = get_settings()


class AioBoto:
    def __init__(self, minio_url: str):
        self._session = None
        self.s3_resource_cm = None
        self.s3_resource = None
        self.minio_url = minio_url

    async def connect(self):
        self._session = aioboto3.Session()
        self.s3_resource_cm = self._session.resource(
            "s3",
            endpoint_url=self.minio_url,
            aws_access_key_id=config.minio_username,
            aws_secret_access_key=config.minio_password,
        )
        self.s3_resource = await self.s3_resource_cm.__aenter__()
        print("✅ Minio 연결 성공")

    async def uploadFile(self, file, bucket_name: str, key: str):
        bucket = await self.s3_resource.Bucket(bucket_name)
        await bucket.upload_fileobj(file, key)
        print(
            f"uploadFile 실행 중: PID {os.getpid()}, 스레드 {threading.current_thread().name}"
        )
        print(f"✅ MinIO 파일 업로드 성공: {key} (Bucket: {bucket_name})")

    async def close(self):
        if self.s3_resource_cm:
            await self.s3_resource_cm.__aexit__(None, None, None)
            print("❌ Minio 연결 종료")
