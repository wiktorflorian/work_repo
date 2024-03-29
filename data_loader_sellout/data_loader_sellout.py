import credentials as c
import json
import math
import pandas as pd
import re
import os

from requests_oauth2client import *

# Function to get JSON response from the API
def get_json_response(api, endpoint):
    # Sends a GET request to the API and returns the parsed JSON response
    response = api.get(endpoint)
    response_data = response.content.decode('utf-8')
    return json.loads(response_data)

# Function to calculate the number of pages
def calculate_num_pages(count, per_page):
    # Calculates the number of pages based on the total count and per page limit
    return math.ceil(count / per_page)

# OAuth2 authentication setup
token_endpoint = c.token_endpoint
client_id = c.client_id
client_secret = c.client_secret
api_url = c.api_url
scope = c.scope
output_path = c.output_path

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

# Iterate over each page to retrive client information
for page in range(1, num_pages + 1):
    clients_response = get_json_response(api, f"/clients?page={page}")
    page_data = clients_response['data']

    if page_data is not None:
        for client in page_data['clients']:
            group_name = client['group']['name'] if client['group'] is not None else None

            # Extract client information and append to the list
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

# List to store attachments, rewards, and operations data
attachments = []
rewards = []
operations = []

# Iterate over each client to retrieve attachments, rewards, and operations data
for client in clients:
    # Get attachments count
    attachments_response = get_json_response(api, f"/clients/{client[0]}/attachments")
    attachments_data = attachments_response['data']
    attachments_count = attachments_data['info']['count']
    attachments.append([client[0], attachments_count])

    # Get rewards
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

# Create a DataFrame for clients data
clients_columns = ['client_id', 'client_email', 'client_name', 'client_surname', 'client_phone', 'company', 'client_nip', 'account_name', 'salesrep', 'products', 'delivery']
clients_df = pd.DataFrame(clients)
clients_df.columns = clients_columns
clients_df['client_id'] = 'BB_' + clients_df['client_id'] 
clients_df['salesrep'] = clients_df['salesrep'].str.split(' - ').str[0]
clients_df.to_excel(os.path.join(output_path, 'sellout_clients.xlsx'), index=False)

# Create a DataFrame for attachments data
attachments_columns = ['client_id', 'has_attachment']
attachments_df = pd.DataFrame(attachments)
attachments_df.columns = attachments_columns
attachments_df['client_id'] = 'BB_' + attachments_df['client_id']
attachments_df['has_attachment'] = attachments_df['has_attachment'].apply(lambda x: 1 if int(x) > 0 else 0)
attachments_df.to_excel(os.path.join(output_path, 'sellout_attachments.xlsx'), index=False)

# Create DataFrame for rewards data
rewards_columns = ['client_id', 'reward_name', 'quantity', 'points', 'status', 'date_add']
rewards_df = pd.DataFrame(rewards)
rewards_df.columns = rewards_columns
rewards_df['client_id'] = 'BB_' + rewards_df['client_id']
rewards_df.to_excel(os.path.join(output_path, 'sellout_rewards.xlsx'), index=False)

# Create a DataFrame for operations data
operations_columns = ['client_id', 'type', 'date_add', 'tag', 'description']
operations_df = pd.DataFrame(operations)
operations_df.columns = operations_columns
operations_df['client_id'] = 'BB_' + operations_df['client_id']

# Keep rows where type is add
operations_df = operations_df[operations_df['type'] == 'add']
# Drop rows where tag is empty
operations_df = operations_df[operations_df['tag'] != '']
# Drop rows which contains 'quiz' in description
operations_df = operations_df[~operations_df['description'].str.contains('quiz', case=False)]
# Drop rows which contains 'ankiety' in description
operations_df = operations_df[~operations_df['description'].str.contains('ankiety', case=False)]
# Drop rows which contains 'zwrot' in description
operations_df = operations_df[~operations_df['description'].str.contains('zwrot', case=False)]
# Drop rows which contains 'Nowy klient' in description
operations_df = operations_df[~operations_df['description'].str.contains('Nowy klient', case=False)]
# Extract product information from the description column
rows = []

# Define regex patterns for two possible formats of product information
pattern1 = r'(\d+)x(.*?) - (\d{7})'
pattern2 = r'(\d+)x(.*?) -  (\d{7})'

# Iterate over each row in the operations DataFrame
for _, row in operations_df.iterrows():
    # Extract relevant fields from the row
    client_id = row['client_id']
    operation = row['type']
    date = row['date_add']
    description = row['description']
    # Split the description into individual product strings using a regex pattern
    products = re.split(r',(?=\d+x)', description)

    # Process each product string
    for product in products:
        # Try to match the product string with the first patter
        match1 = re.match(pattern1, product.strip())

        # Try to match the product string with the second pattern
        match2 = re.match(pattern2, product.strip())

        # If match 1 is successful, extract the product information
        if match1:
            quantity = match1.group(1)
            product_name = match1.group(2)
            product_id = match1.group(3)

            matched_row = [client_id, operation, date, product_id, product_name, quantity]
            rows.append(matched_row)
        
        # If match2 is successful, extract the product information
        elif match2:
            quantity = match2.group(1)
            product_name = match2.group(2)
            product_id = match2.group(3)

            matched_row = [client_id, operation, date, product_id, product_name, quantity]
            rows.append(matched_row)
            
        # If neither match is successful, handle the unmatched product
        else:
            # Split the product string to extract quantity and product name
            product_parts = product.split('x', 1)
            quantity = product_parts[0]
            product_name = product_parts[1].strip().replace(' - -', '')
            product_name = product_name.rsplit(' -', 1)[0] if product_name.endswith(' -') else product_name
            product_id = None
            unmatched_row = [client_id, operation, date, product_id, product_name, quantity]
            rows.append(unmatched_row)


# Create a DataFrame for products data
matched_df = pd.DataFrame(rows, columns=['client_id', 'operation', 'date', 'product_id', 'product_name', 'quantity'])

# Change data types
matched_df['product_id'] = matched_df['product_id'].fillna(0).astype('int32')
matched_df['quantity'] = matched_df['quantity'].fillna(0).astype('int32')

# Define the pattern with escaped characters
pattern = r'\(.*?,.*?\)'

# Use str.contains() to filter rows that match the pattern
matched_rows = matched_df[matched_df['product_name'].str.contains(pattern, case=False, regex=True)]

# Use ~ to filter rows that do not match the pattern
not_matched_rows = matched_df[~matched_df['product_name'].str.contains(pattern, case=False, regex=True)]

# Define the pattern to remove from 'product_name'
patterns_to_remove = [
    'cała gama 1 ',
    'cała gama 2 ',
    'cała gama ',
    'pielęgnacja 1 '
    'pielęgnacja 2 ',
    'Codzienna pielęgnacja 1 ',
    'Codzienna pielęgnacja 2 '
]

# Remove the specified patterns from 'product_name'
for pattern in patterns_to_remove:
    matched_rows.loc[:, 'product_name'] = matched_rows['product_name'].str.replace(pattern, '')

exploded_rows = []

# Iterate through matched rows
for index, row in matched_rows.iterrows():
    product_name = row['product_name']
    values = product_name.split(', ')
    part_before_parentheses = product_name.split('(')[0].strip() 
    for value in values:
        new_row = row.copy()
        new_row['product_name'] = f"{part_before_parentheses} {value}"
        new_row['product_name'] = new_row['product_name'].replace('(', '').replace(')', '')
        exploded_rows.append(new_row)

# Create a new DataFrame from the exploded rows
exploded_df = pd.DataFrame(exploded_rows)

result_df = pd.concat([not_matched_rows, exploded_df], ignore_index=True)

# Save the products data to an Excel file
result_df.to_excel(os.path.join(output_path, 'sellout_products.xlsx'), index=False)