import requests
import json
import os

def load_data(project_id, time_interval):
    url = f'https://bds-api.powerloom.io/tradeVolume/{project_id}/{time_interval}'
    response = requests.get(url, timeout=1000)
    return response.json()


def load_daily_active_pools(page, size, time_interval, metadata=False):
    url = f'https://bds-api.powerloom.io/dailyActivePools?page={page}&size={size}&metadata={metadata}&time_interval={time_interval}'
    response = requests.get(url)
    return response.json()

def gen_and_save_active_pools():

    active_pools = []

    initial_data = load_daily_active_pools(1, 50, 86400, False)
    active_pools.extend(initial_data['active_pools'])
    pages = initial_data['pagination']['total_pages']

    for page in range(2, pages + 1):
        print(f'Loading page {page} of {pages}')
        data = load_daily_active_pools(page, 50, 86400, False)
        active_pools.extend(data['active_pools'])

    print(len(active_pools))

    # save to json
    with open('active_pools.json', 'w') as f:
        json.dump(active_pools, f)


# try to load active pools from json otherwise generate and save
if os.path.exists('active_pools.json'):
    with open('active_pools.json', 'r') as f:
        active_pools = json.load(f)
else:
    active_pools = gen_and_save_active_pools()

init_index = 0

for i in range(init_index, len(active_pools)):
    data = active_pools[i]
    pool_address = data['pool_address']
    print(f'Loading data for {i}, {pool_address}')
    data = load_data(pool_address, 86400)
    print(pool_address, data)