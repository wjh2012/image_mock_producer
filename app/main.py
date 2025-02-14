from fastapi import FastAPI

from app.a4_text_image_maker import compressed_a4

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/produce-single-image")
async def produce_single_image():
    return {"message": "Hello World"}


@app.get("/produce-compressed-image")
async def produce_compressed_image():
    compressed_image = compressed_a4()
    return {"message": "Hello World"}
