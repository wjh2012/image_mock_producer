from functools import lru_cache

from fastapi import FastAPI

from app import config
from app.a4_text_image_maker import compressed_a4, get_single_a4


app = FastAPI()


@lru_cache
def get_settings():
    return config.Settings()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/produce-single-image")
async def produce_single_image():
    image = await get_single_a4()
    return {"message": "produce a4 success"}


@app.get("/produce-compressed-image")
async def produce_compressed_image():
    compressed_image = compressed_a4()
    return {"message": "Hello World"}
