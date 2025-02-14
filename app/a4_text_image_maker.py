import asyncio
import random
import zipfile
from io import BytesIO
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


async def get_single_a4():
    return await asyncio.to_thread(get_single_a4_sync)


async def compressed_a4(count=5):
    tasks = [get_single_a4() for _ in range(count)]
    images = await asyncio.gather(*tasks)

    def zip_images(img_list):
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for i, img in enumerate(img_list):
                img_buffer = BytesIO()
                img.save(img_buffer, format="PNG")
                img_buffer.seek(0)
                zipf.writestr(f"a4_image_{i+1}.png", img_buffer.read())
        zip_buffer.seek(0)
        return zip_buffer

    zip_buffer = await asyncio.to_thread(zip_images, images)
    return zip_buffer


def save_a4_example():
    output_path = "random_text_a4.png"
    random_image = get_single_a4_sync()
    if random_image is not None:
        random_image.save(output_path)


async def save_compressed_a4_example(output_path="random_text_a4.zip", count=5):
    zip_buffer = await compressed_a4(count)

    with open(output_path, "wb") as f:
        f.write(zip_buffer.getvalue())

    print(f"✅ ZIP 파일 저장 완료: {output_path}")


# 실행
if __name__ == "__main__":
    asyncio.run(save_compressed_a4_example("a4_images.zip", count=5))
