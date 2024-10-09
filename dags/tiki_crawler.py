import pandas as pd
import requests
from sqlalchemy import create_engine

id_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8,en-GB;q=0.7,pt-BR;q=0.6,pt;q=0.5',
    'Referer': 'https://tiki.vn/dien-thoai-may-tinh-bang/c1789',
    'x-guest-token': '79RCLgrUOvdcxWG082NlFpH3ftzeVw1Z',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

id_params = {
    'limit': '40',
    'include': 'advertisement',
    'aggregations': '2',
    'version': 'home-persionalized',
    'trackity_id': '21b11ec5-cb0e-29d9-9943-af73cebd79a4',
    'category': '1789',
    'page': '1',
    'urlKey': 'dien-thoai-may-tinh-bang'
}

data_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8,en-GB;q=0.7,pt-BR;q=0.6,pt;q=0.5',
    'referer': 'https://tiki.vn/dien-thoai-samsung-galaxy-a05s-4gb-128gb-da-kich-hoat-bao-hanh-dien-tu-hang-chinh-hang-p273258825.html?itm_campaign=CTP_YPD_TKA_PLA_UNK_ALL_UNK_UNK_UNK_UNK_X.286284_Y.1868604_Z.3940284_CN.Samsung-l-S24-Series&itm_medium=CPC&itm_source=tiki-ads&spid=273259161',
    'x-guest-token': '79RCLgrUOvdcxWG082NlFpH3ftzeVw1Z',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

data_params = {
    'platform': 'web',
    'spid': '273259161',
    'version': '3',
}


def get_product_id():
    product_id = []
    for i in range(1, 5):
        id_params['page'] = str(i)
        response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings', headers=id_headers, params=id_params)
        if response.status_code == 200:
            print('request success')
            for record in response.json()['data']:
                product_id.append({'id': record.get('id')})
    df = pd.DataFrame(product_id)
    df.to_csv('productId.csv', index=False)



def parser_product(json):
    d = dict()
    d['id'] = json['id']
    d['sku'] = json['sku']
    d['name'] = json['name']
    d['short_description'] = json['short_description']
    d['price'] = json['price']
    d['original_price'] = json['original_price']
    d['discount'] = json['discount']
    d['discount_rate'] = json['discount_rate']
    d['rating_average'] = json['rating_average']
    d['reviews_count'] = json['review_count']
    d['inventory_status'] = json['inventory_status']
    d['brand_id'] = json['brand']['id']
    d['brand_name'] = json['brand']['name']
    d['category_id'] = json['categories']['id']
    d['category_name'] = json['categories']['name']
    return d


def get_product_data():
    df_id = pd.read_csv('productId.csv')
    p_ids = df_id.id.tolist()
    result = []
    count = 0
    for pid in p_ids:
        response = requests.get('https://tiki.vn/api/v2/products/{}'.format(pid), headers=data_headers, params=data_params)
        if response.status_code == 200:
            print('Crawl data {} success'.format(pid))
            try:
                result.append(parser_product(response.json()))
                count += 1
            except Exception as e:
                print(e)
                continue
        if count == 20:
            break

    df_product = pd.DataFrame(result)

    # save to postgresql
    engine = create_engine('postgresql://postgres:joshuamellody@host.docker.internal:5432/test')
    df_product.to_sql('product_data', engine, index=False, if_exists="replace")