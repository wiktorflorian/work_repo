import credentials as c
import json
import math
import pandas as pd
import re

from requests_oauth2client import *

def get_json_response(api, endpoint):
    # Sends a GET request to the API and returns the parsed JSON response
    response = api.get(endpoint)
    response_data = response.content.decode('utf-8')
    return json.loads(response_data)

def calculate_num_pages(count, per_page):
    # Calculates the number of pages based on the total count and per page limit
    return math.ceil(count / per_page)

# OAuth2 authentication setup
token_endpoint = c.token_endpoint
client_id = c.client_id
client_secret = c.client_secret
api_url = c.api_url
scope = c.scope

oauth2client = OAuth2Client(
    token_endpoint=token_endpoint,
    auth=(client_id, client_secret),
)

auth = OAuth2ClientCredentialsAuth(
    oauth2client, scope=scope, resource=api_url
)

api = ApiClient(
    api_url, auth=auth
)

# Get total client count and per page limit
clients_response = get_json_response(api, "/clients")
clients_data = clients_response['data']
total_count = clients_data['info']['count']
per_page = clients_data['info']['per_page']

# Calculate number of pages
num_pages = calculate_num_pages(total_count, per_page)

# List to store all the clients records
clients = []

# Iterate over each page
for page in range(1, num_pages + 1):
    clients_response = get_json_response(api, f"/clients?page={page}")
    page_data = clients_response['data']

    if page_data is not None:
        for client in page_data['clients']:
            group_name = client['group']['name'] if client['group'] is not None else None

            client_info = [
                client['number'],
                client['email'],
                client['name'],
                client['surname'],
                client['phone'],
                client['company'],
                client['nip'],
                group_name,
                client['additional']['1'],
                client['additional']['2'],
                client['additional']['3']
            ]
            clients.append(client_info)
# typo#
attachments = []
rewards = []
operations = []

# Iterate over each client
for client in clients:
    # Get attachments count
    attachments_response = get_json_response(api, f"/clients/{client[0]}/attachments")
    attachments_data = attachments_response['data']
    attachments_count = attachments_data['info']['count']
    # typo #
    attachments.append([client[0], attachments_count])

    # Get reward
    rewards_response = get_json_response(api, f"/clients/{client[0]}/rewards/orders")
    rewards_data = rewards_response['data']
    rewards_count = rewards_data['info']['count']
    rewards_per_page = rewards_data['info']['per_page']
    rewards_num_pages = calculate_num_pages(rewards_count, rewards_per_page)

    for page in range(1, rewards_num_pages + 1):
        rewards_response = get_json_response(api, f"/clients/{client[0]}/rewards/orders")
        rewards.extend([
            [
                reward_order['client']['number'],
                reward_order['reward']['name'],
                reward_order['quantity'],
                reward_order['points'],
                reward_order['status'],
                reward_order['date_add']
            ]
            for reward_order in rewards_response['data']['rewards_orders']
        ])

    # Get operations
    operations_response = get_json_response(api, f"/clients/{client[0]}/operations")
    operations_data = operations_response['data']
    operations_count = operations_data['info']['count']
    operations_per_page = operations_data['info']['per_page']
    operations_num_pages = calculate_num_pages(operations_count, operations_per_page)

    for page in range(1, operations_num_pages + 1):
        operations_response = get_json_response(api, f"/clients/{client[0]}/operations?page={page}")
        operations.extend([
                [
                    operation['client']['number'],
                    operation['type'],
                    operation['date_add'],
                    operation['tags'],
                    operation['description']
                ]
                for operation in operations_response['data']['operations']
        ])

clients_columns = ['client_id', 'client_email', 'client_name', 'client_surname', 'client_phone', 'company', 'client_nip', 'account_name', 'salesrep', 'products', 'delivery']
clients_df = pd.DataFrame(clients)
clients_df.columns = clients_columns
clients_df['client_id'] = 'BB_' + clients_df['client_id'] 
clients_df['salesrep'] = clients_df['salesrep'].str.split(' - ').str[0]
clients_df.to_excel('output/sellout_clients.xlsx', index=False)

attachments_columns = ['client_id', 'has_attachment']
attachments_df = pd.DataFrame(attachments)
attachments_df.columns = attachments_columns
attachments_df['client_id'] = 'BB_' + attachments_df['client_id']
attachments_df['has_attachment'] = attachments_df['has_attachment'].apply(lambda x: 1 if int(x) > 0 else 0)
attachments_df.to_excel('output/sellout_attachments.xlsx', index=False)

rewards_columns = ['client_id', 'reward_name', 'quantity', 'points', 'status', 'date_add']
rewards_df = pd.DataFrame(rewards)
rewards_df.columns = rewards_columns
rewards_df['client_id'] = 'BB_' + rewards_df['client_id']
rewards_df.to_excel('output/rewards.xlsx', index=False)

operations_columns = ['client_id', 'type', 'date_add', 'tag', 'description']
operations_df = pd.DataFrame(operations)
operations_df.columns = operations_columns
operations_df['client_id'] = 'BB_' + operations_df['client_id']

# Drop rows where tag is empty
operations_df = operations_df[operations_df['tag'] != '']

new_rows = []
pattern = r'(\d+)x(.*?) - (\d{7})'
for _, row in operations_df.iterrows():
    client_id = row['client_id']
    operation = row['type']
    date = row['date_add']
    description = row['description']

    if description is None:
        continue
    
    products = re.findall(pattern, description)
    for product in products:
        quantity = product[0]
        product_name = product[1]
        product_id = product[2]

        new_row = [client_id, operation, date, product_id, product_name, quantity]
        new_rows.append(new_row)

products_df = pd.DataFrame(new_rows, columns=['client_id', 'operation', 'date', 'product_id', 'product_name', 'quantity'])

products_df
products_df.to_excel('output/products.xlsx', index=False)