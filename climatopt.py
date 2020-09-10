from json_dump import open_df
from asyncio import run, gather
from aiohttp import ClientSession
from csv import writer
from json import load
from lxml.html import fromstring, tostring
from html2text import html2text
from tqdm import tqdm
from random import randrange

NUMS_THREADS = 10
DUMP_FILE = 'climatopt.json'
RESULT_FILE = 'climatopt.csv'
first = True


async def get_page(url, **kwargs):
    async with ClientSession() as sess:
        async with sess.get(url, **kwargs) as req:
            return await req.text()


async def get_all_catalog_urls():
    url = 'https://climatopt.ru/catalog/'

    content_page = await get_page(url)

    document = fromstring(content_page)

    urls = document.xpath('//td[@class="section_info"]/ul/li[@class="sect"]/a[@class="dark_link"]/@href')

    for i in range(len(urls)):
        urls[i] = 'https://climatopt.ru' + urls[i]

    return urls


async def get_catalog_length(catalog_url):
    content_page = await get_page(catalog_url)

    document = fromstring(content_page)

    nums = document.xpath('//div[@class="module-pagination"]/div/a[last()]/text()')
    if not nums:
        return [catalog_url]
    else:
        return [catalog_url + f'?PAGEN_1={i}' for i in range(1, int(nums[-1]) + 1)]


async def get_product_urls_from_page(page_url):
    content_page = await get_page(page_url)

    document = fromstring(content_page)

    urls = document.xpath('//div[@class="item-title"]/a/@href')

    for i in range(len(urls)):
        urls[i] = 'https://climatopt.ru' + urls[i]

    return urls


async def get_all_product_urls():
    catalog_urls = await get_all_catalog_urls()
    all_catalog_page_urls = []
    all_products_urls = []

    for i in range(0, len(catalog_urls), NUMS_THREADS):
        urls = catalog_urls[i: i + NUMS_THREADS] if i + NUMS_THREADS < len(catalog_urls) else catalog_urls[i:]
        answers = await gather(*[get_catalog_length(url) for url in urls])

        for answer in answers:
            all_catalog_page_urls += answer

    for i in range(0, len(all_catalog_page_urls), NUMS_THREADS):
        urls = all_catalog_page_urls[i: i + NUMS_THREADS] if i + NUMS_THREADS < len(all_catalog_page_urls) else all_catalog_page_urls[i:]
        answers = await gather(*[get_product_urls_from_page(url) for url in urls])

        for answer in answers:
            all_products_urls += answer

    return all_products_urls


async def get_product_data(url):
    global first

    data = {'url': url}

    try:
        content_page = await get_page(url)
    except:
        return False

    document = fromstring(content_page)

    image = document.xpath('//div[@class="slides"]/ul/li/a/img/@src')
    if image:
        for i in range(10):
            if i < len(image):
                data[f'Фото товара {i + 1}'] = image[i]
            else:
                data[f'Фото товара {i + 1}'] = ''
    else:
        for i in range(10):
            data[f'Фото товара {i + 1}'] = ''

    name = document.xpath('//h1[@id="pagetitle"]/text()')
    if name:
        data['Название'] = name[0]
    else:
        data['Название'] =''

    price = document.xpath('//div[@class="info_item"]/div/div/div/div/div[@class="price"]/@data-value')
    if price:
        data['Цена'] = price[0]
    if first:
        first = False
        print(price)

    article = document.xpath('//div[@class="article iblock"]/span[@class="value"]/text()')
    if article:
        data['Артикул'] = article[0]
    else:
        data['Артикул'] = 'cl-{:06}'.format(randrange(1000000))

    category = document.xpath('//div[@id="navigation"]/div/div[3]/a/span/text()')
    if category:
        data['Категория'] = category[0]  #TODO:
    else:
        data['Категория'] = ''

    files = document.xpath('//div[@class="files_block"]//a/@href')
    for i, file in enumerate(files):
        data[f'Файл {i}'] = 'https://climatopt.ru' + file

    description = document.xpath('//div[@class="detail_text"]')
    if description:
        data['Описание'] = html2text('\n'.join(description[0].xpath('../div[@class="detail_text"]//text()'))).\
            replace('\t', '').replace('\r', '')
        data['Описание html'] = tostring(description[0]).decode()

    characteristics = document.xpath('//tr[@itemprop="additionalProperty"]')
    for characteristic in characteristics:
        characteristic_name = characteristic.xpath('td[@class="char_name"]/div/span/text()')
        characteristic_value = characteristic.xpath('td[@class="char_value"]/span/text()')
        if characteristic_name and characteristic_value:
            data[characteristic_name[0].replace('\n', '').replace('\t', '')] = characteristic_value[0].replace('\n', '').replace('\t', '')

    return data


async def main():
    product_urls = await get_all_product_urls()

    dump_file = open_df(DUMP_FILE)
    for i in tqdm(range(0, len(product_urls), NUMS_THREADS)):
        urls = product_urls[i: i + NUMS_THREADS] if i + NUMS_THREADS < len(product_urls) else product_urls[i:]
        answers = await gather(*[get_product_data(url) for url in urls])

        for answer in answers:
            if answer:
                dump_file.write(answer)
    dump_file.close()

    with open(DUMP_FILE, 'r') as file:
        write_to_csv(load(file))


def write_to_csv(data_products):
    default_characteristics = {}

    all_characteristics_name = []
    for product in data_products:  # Получение списка ВСЕХ возможных характеристик
        for name in product.keys():
            if name not in all_characteristics_name:
                all_characteristics_name.append(name)
                default_characteristics[name] = ''

    for i in range(len(data_products)):  # Добавление ВСЕХ характеристик к каждому продукту
        dh = default_characteristics.copy()
        dh.update(data_products[i])
        data_products[i] = dh

    with open(RESULT_FILE, 'w') as file:  # Запись в csv файл
        csv_writer = writer(file, delimiter=';')

        data = []
        for value in data_products[0].keys():
            data.append(value.replace('\n', '').replace('\r', ''))

        csv_writer.writerow(data)

        for product in data_products:
            csv_writer.writerow(product.values())


if __name__ == '__main__':
    run(main())
    #main()
