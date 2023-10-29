#!/usr/bin/env python
# coding: utf-8

# ## ArgenProp Scrapping
# 
# **Objetivo:** extraer información de departamentos en alquiler en Capital Federal de la página de [ArgenProp](https://www.argenprop.com/) para su posterior carga a Redshift y análisis.
# 
# * El script sólo busca departamentos ordenados por más recientemente publicados. Como pasos adicionales, puede editarse la URL para extraer casas, PH, etc.
# * También podrán extraerse propiedades en venta. Esto ya se tuvo en cuenta en la estructura al agregar una variable "sell_or_rent_ind" para identificar si se trata de un anuncio de alquiler o de venta

#  ### Imports

# In[1]:


# !pip3 install python-dotenv


# In[2]:


# !pip3 install redshift_connector


# In[3]:


import pandas as pd
import pytz
import redshift_connector
import requests
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import dotenv_values
from sqlalchemy import create_engine, insert, MetaData, Table
from sqlalchemy.orm import sessionmaker


# ### Argenprop Data Extraction

# In[4]:

print("Starting Script")

utc_timezone = pytz.utc
process_dt_utc = datetime.now(utc_timezone)
process_dttm = process_dt_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
process_dt = process_dt_utc.strftime("%Y-%m-%d")

ap_base_url = 'https://www.argenprop.com'

ap_new_url = ap_base_url + '/departamento-alquiler-localidad-capital-federal-orden-masnuevos-pagina-'

def ap_request(url):
  response = requests.get(url)

  if response.status_code == 200:
      # Parse HTML content using lxml
      soup = BeautifulSoup(response.text, 'lxml')
  else:
      print("Error al obtener la página:", response.status_code)

  return soup

listing_urls = []


# In[5]:


# Get the URLs of the most recent listings (First 50 pages. The website does not provide the publication date of the item.)
# The code should be executed every day so it would not be necessary to search all the pages every day, just until we detect a property already scanned the day before

print("Extracting new listings from Argenprop")

for i in range(1,20):

  try:
    page_data = ap_request(ap_new_url + str(i))

    listing_items_temp = page_data.find_all(class_='listing__item')

    for i in listing_items_temp:
      card_link = i.find('a', class_='card')

      # Look for the listing URL inside the "href" atribute
      if card_link and 'href' in card_link.attrs:
        listing_endpoint = card_link['href']

        listing_url = ap_base_url + listing_endpoint

        listing_urls.append(listing_url)
        
  except Exception as e:
    print('Hubo un problema al procesar la página: ' + e)


# In[6]:


len(listing_urls)


# In[7]:


listing_urls[0:10]


# In[8]:


# Create an empty list to store property data and iterate over a list of property listing URLs
listings_data = []

for listing_url in listing_urls:

  try:
    listing_req = ap_request(listing_url)

    # Some of the details of the property will always be there, but others may not be present.
    title_address = listing_req.find(class_='titlebar__address').get_text(strip=True)
    title_desc_short = listing_req.find(class_='titlebar__title').get_text(strip=True)
    description_title = listing_req.find(class_='section-description--title').get_text(strip=True)
    listing_price = listing_req.find(class_='titlebar__price').get_text(strip=True)

    try:
      listing_id = listing_url.split("--")[-1]
    except:
      continue
    try:
      description_content = listing_req.find(class_='section-description--content').get_text(strip=True)
    except:
      description_content = None
    try:
      address_detail = listing_req.find(class_='location-container').find_all('p')[0].get_text(strip = True)
    except:
      address_detail = None
    try:
      address_zone = listing_req.find(class_='location-container').find_all('p')[1].get_text(strip = True)
    except:
      address_zone = None

    # Extract all property features and keep the most relevant ones
    features_raw = listing_req.find_all(class_='property-features')
    features_dict = {}

    for features in features_raw:
      features_list = features.find_all('li')

      for feature in features_list:
        string = feature.get_text(strip = True)

        try:
          key, value = string.split(":")
          features_dict[key.strip()] = value.strip()
        except:
          # If splitting by ":" fails, assume it's a boolean feature and set it to True
          features_dict[string] = True

    listing_data_temp = {
        'listing_id': listing_id,
        'listing_url': listing_url,
        'title_address': title_address,
        'title_desc_short': title_desc_short,
        'description_title': description_title,
        'description_content': description_content,
        'listing_price': listing_price,
        'address_detail': address_detail,
        'address_zone': address_zone,
        'room_qty': features_dict.get('Cant. Ambientes', None),
        'dorms_qty': features_dict.get('Cant. Dormitorios', None),
        'baths_qty': features_dict.get('Cant. Baños', None),
        'parking_qty': features_dict.get('Cant. Cocheras', None),
        'property_conditions': features_dict.get('Estado', None),
        'property_age': features_dict.get('Antiguedad', None),
        'building_conditions': features_dict.get('Estado Edificio', None),
        'sell_or_rent_ind': features_dict.get('Tipo de operación', None),
        'unit_type': features_dict.get('Tipo de Unidad', None),
        'area_built': features_dict.get('Sup. Cubierta', None),
        'area_not_built': features_dict.get('Sup. Descubierta', None),
        'expenses_amt': features_dict.get('Expensas', None),
        'price_amt': features_dict.get('Precio', None),
        'elevator_ind': features_dict.get('Ascensor', False),
        'pets_ind': features_dict.get('Permite Mascotas', False),
        'gym_ind': features_dict.get('Gimnasio', False),
        'rooftop_ind': features_dict.get('Terraza', False),
        'pool_ind': features_dict.get('Pileta', False),
        'grill_ind': features_dict.get('Parrilla', False),
        'solarium_ind':  features_dict.get('Solarium', False),
        'process_dt': process_dt,
        'process_dttm': process_dttm
    }

    listings_data.append(listing_data_temp)

  except Exception as e:
    print(f"Error while processing {listing_url}")
    continue

print(f"Total listings scrapped: {len(listings_data)}")

# In[9]:


# Edit the format of some fields

print("Doing some formatting")

for listing in listings_data:
    
  # Limit description_content to 5000 characters
  listing['description_content'] = listing['description_content'][:5000] if listing['description_content'] is not None else None

  # Convert strings to int
  int_variables = ['room_qty', 'dorms_qty', 'baths_qty', 'parking_qty', 'property_age']
    
  for i in int_variables:
    listing[i] = int(listing[i]) if listing[i] is not None else None

  # Convert areas to int
  for area in ['area_built','area_not_built']:
    area_units_str = area + '_units'

    if listing[area] is None:
      listing[area] = None
      listing[area_units_str] = None

    else:
      try:
        area_temp = re.match(r'([\d,.]+)\s*(\S+)', listing[area])

        if area_temp:
          numeric_value_str, unit_of_measure = area_temp.groups()
          numeric_value = int(float(numeric_value_str.replace('.', '').replace(',', '.')))

          listing[area] = numeric_value
          listing[area_units_str] = unit_of_measure

        else:
          # No data in 'area_built'
          listing[area] = None
          listing[area_units_str] = None
      except:
        print(f'Error getting the area: {area}')
        print(listing)
        print('')
        break

  # Edit Price (if informed)
  if listing['price_amt'] is None or listing['listing_price'] == 'Consultar precio':
    listing['informs_price_ind'] = False
    listing['price_amt'] = None
    listing['price_amt_units'] = None

  else:
    try:
      price_temp = re.match(r'([^\d]+)(\d+(?:[,.]\d+)?)', listing['price_amt'])
    except:
      print('Error getting the price')
      print(listing)
      print('')

    if price_temp:
      price_units, price_amt_str = price_temp.groups()
      price_amt = int(re.sub(r'[,.]', '', price_amt_str))
      price_units = price_units.strip().replace('$','ARS')

      listing['informs_price_ind'] = True
      listing['price_amt'] = price_amt
      listing['price_amt_units'] = price_units

    else:
      listing['informs_price_ind'] = False
      listing['price_amt'] = None
      listing['price_amt_units'] = None

  # Something similar for expenses (if informed)
  if listing['expenses_amt'] is None:
    listing['expenses_ind'] = False
    listing['expenses_amt_units'] = None

  else:
    try:
      expenses_temp = re.match(r'([^\d]+)(\d+(?:[,.]\d+)?)', listing['expenses_amt'])
    except:
      print('Error getting the expenses')
      print(listing)
      print('')

    if expenses_temp:
      expenses_units, expenses_amt_str = expenses_temp.groups()
      expenses_amt = int(re.sub(r'[,.]', '', expenses_amt_str))
      expenses_units = expenses_units.strip().replace('$','ARS')

      listing['expenses_ind'] = True
      listing['expenses_amt'] = expenses_amt
      listing['expenses_amt_units'] = expenses_units

    else:
      listing['expenses_ind'] = False
      listing['expenses_amt'] = None
      listing['expenses_amt_units'] = None


# In[10]:


listings_data[0]


# ### Connect to Redshift Database

# #### Set connection

# In[11]:

print("Loading data to Redshift")
config = dotenv_values(".env")


# In[12]:


host = config['REDSHIFT_HOST']
port = config['REDSHIFT_PORT']
database = config['REDSHIFT_DB']
user = config['REDSHIFT_USER']
password = config['SEDSHIFT_PASSWORD']
schema = config['REDSHIFT_SCHEMA']
table_name = config['REDSHIFT_TABLE']


# In[13]:


def redshift_connection(host, port, db, user, pwd):
  try:
    print('Connecting to Redshift Cluster')
    conn = redshift_connector.connect(
      host = host,
      database = db,
      port = port,
      user = user,
      password = pwd
    )
    
    # Construct the connection URL
    db_url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)
    
    print('Connection Complete')
    return conn, engine

  except:
    print('Failed to connect to Redshift')


# In[14]:


conn, engine = redshift_connection(host, port, database, user, password) 


# Create function to execute SQL Queries

# In[15]:


def exec_sql(conn, query):
  try:
    cursor = conn.cursor()
    conn.autocomit = True
    cursor.execute(query)
    cols = [description[0] for description in cursor.description]
    
    df = pd.DataFrame(cursor.fetchall(), columns = cols)
    return df

  except Exception as e:
    print(f"Failed with error message: {e}")
    raise e
    
  finally:
    cursor.close()


# #### Get Redshift data to look for dupplicates
# 
# As we do not have a temporary identifier to know until when we have downloaded publications, and publications can appear or be deleted from Argenprop at any time, it is necessary to control that we do not add a publication that has been previously scanned. 
# 
# * We may have extracted the publication in a previous process, and it may already be in the Redshift table.
# * It can also happen that the owner has published the same apartment 2 different times. In this case, we are only interested in removing the duplicate if both listings have different prices (it may be that the listing has been updated with a different price).

# In[16]:


query = f"SELECT DISTINCT listing_id FROM {schema}.{table_name}"

db_ids = exec_sql(conn, query)
db_ids['listing_id'] = db_ids['listing_id'].astype(str)


# In[17]:


df = pd.DataFrame(listings_data)


# In[18]:


df.sample(1)


# In[19]:


df.shape


# In[20]:


# Remove duplicates (2 publications with different ID but same department, address and price)
df = df.drop_duplicates(subset=['title_desc_short', 'title_address', 'listing_price'])
df.shape


# In[21]:


# Merge df with db_ids using a left join and indicator=True to remove records already in the database
merged = pd.merge(df, db_ids, on='listing_id', how='left', indicator=True)

df_clean = merged[merged['_merge'] == 'left_only']
df_clean = df_clean.drop(columns=['_merge'])
df_clean.shape


# #### Append new information

# Columns in Redshift table don't have the same order as the ones in the dataframe, so I need to order them

# In[22]:


# Create a metadata object with the schema
metadata = MetaData(schema=schema)

# Reflect the existing table structure
table = Table(table_name, metadata, autoload=True, autoload_with=engine)

# Get the column names and their order from the table
column_order = [col.name for col in table.columns]

# Reorder the Dataframe
df_clean = df_clean[column_order] 


# In[23]:


# Define the chunk size
chunk_size = 20

# Calculate the total number of chunks
total_chunks = len(df_clean) // chunk_size + 1

# Iterate through the chunks

update_start_time = time.time()

for i in range(total_chunks):
  start_idx = i * chunk_size
  end_idx = (i + 1) * chunk_size
  
  # Slice the DataFrame to get the current chunk
  current_subset = df_clean[start_idx:end_idx]
  
  print(f"Uploading iteration {i + 1} out of {total_chunks}")
  
  start_time = time.time()
  
  current_subset.to_sql(table_name, engine, schema=schema, index=False, if_exists='append', chunksize=100)
  
  end_time = time.time()
  elapsed_time = end_time - start_time
    
  print(f"Time taken for iteration {i + 1}: {elapsed_time} seconds")


update_end_time = time.time()
total_elapsed_time = update_end_time - update_start_time

print(f"Processing complete. Total time: {total_elapsed_time:.2f} seconds")


# #### Close Connection

# In[24]:


conn.close()

