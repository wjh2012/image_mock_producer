import asyncio
import io
import random
import time
import uuid
import zipfile
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

sentences = [
    "Hello! Have a great day.",
    "Python is an interesting programming language.",
    "Unleash your creativity and create something amazing.",
    "Data science and artificial intelligence are the core of modern technology.",
    "Have a happy day today!",
]


def get_default_font(font_size=100):
    possible_fonts = [
        "arial.ttf",  # Windows
        "DejaVuSans.ttf",  # Linux
        "/System/Library/Fonts/Supplemental/Arial.ttf",  # macOS
    ]
    for font_path in possible_fonts:
        try:
            font = ImageFont.truetype(font_path, font_size)
            return font
        except IOError:
            continue
    return ImageFont.load_default()


def get_single_a4_sync():
    a4_width, a4_height = 2480, 3508
    img = Image.new("RGB", (a4_width, a4_height), "white")
    draw = ImageDraw.Draw(img)

    if random.random() < 1 / 4:
        return img

    font_size = 55
    font = get_default_font(font_size)
    text = random.choice(sentences)

    while font_size > 50:
        text_bbox = draw.textbbox((0, 0), text, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]

        if text_width < (2480 - 100) and text_height < (3508 - 100):
            break

        font_size -= 10
        font = get_default_font(font_size)

    if text_width > 2480 - 100 or text_height > 3508 - 100:
        x = (2480 - text_width) // 2
        y = (3508 - text_height) // 2
    else:
        x = random.randint(50, max(50, 2480 - text_width - 50))
        y = random.randint(50, max(50, 3508 - text_height - 50))

    draw.text((x, y), text, fill="black", font=font)
    return img


def get_single_a4_bytes():
    image = get_single_a4_sync()
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format="JPEG")
    return img_byte_arr.getvalue()


async def get_single_a4_p():
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=1) as executor:
        result = await loop.run_in_executor(executor, get_single_a4_bytes)
    return result


def zip_images_mp(img_bytes_list):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for i, img_bytes in enumerate(img_bytes_list):
            if not isinstance(img_bytes, bytes):
                raise Exception(f"이미지 데이터 형식 오류 (인덱스 {i})")
            short_uuid = uuid.uuid4().hex[:8]
            gen_name = f"{timestamp}_{short_uuid}.jpeg"
            zipf.writestr(gen_name, img_bytes)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


async def get_compressed_a4_mp(count=40):
    start_time = time.perf_counter()
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=4) as executor:
        tasks = [
            loop.run_in_executor(executor, get_single_a4_bytes) for _ in range(count)
        ]
        images_bytes = await asyncio.gather(*tasks)
        zip_bytes = await loop.run_in_executor(executor, zip_images_mp, images_bytes)
    end_time = time.perf_counter()
    print(f"전체 처리 시간: {end_time - start_time:.2f} 초")
    return zip_bytes


##################################################################################################


async def get_single_a4():
    return await asyncio.to_thread(get_single_a4_bytes)


def zip_images(img_bytes_list):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for i, img_bytes in enumerate(img_bytes_list):
            if not isinstance(img_bytes, bytes):
                raise Exception(f"이미지 데이터 형식 오류 (인덱스 {i})")
            short_uuid = uuid.uuid4().hex[:8]
            gen_name = f"{timestamp}_{short_uuid}.jpeg"
            zipf.writestr(gen_name, img_bytes)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


async def get_compressed_a4(count=40):
    start_time = time.perf_counter()
    tasks = [get_single_a4_p() for _ in range(count)]
    images = await asyncio.gather(*tasks)
    zip_buffer = await asyncio.to_thread(zip_images, images)
    end_time = time.perf_counter()
    print(f"전체 처리 시간: {end_time - start_time:.2f} 초")
    return zip_buffer
