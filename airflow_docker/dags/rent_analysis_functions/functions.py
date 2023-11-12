def send_daily_email(MIN_BATHS, MIN_DORMS, MIN_SUP_M2, **kwargs):
	from airflow.operators.email import EmailOperator

    # Recuperar los datos del contexto
	ti = kwargs['ti']
	new_listings_df = ti.xcom_pull(task_ids='get_new_listings', key='new_listings_df')

	new_listings_size = new_listings_df.shape[0]

	# Get search preferences
	filter_values = (new_listings_df['dorms_qty'] >= MIN_DORMS) & (new_listings_df['baths_qty'] >= MIN_BATHS) & (new_listings_df['area_built'] >= MIN_SUP_M2)

	filtered_df = new_listings_df.loc[filter_values]
	filter_df_size = filtered_df.shape[0]

	# List all URLs that match the search preferences
	url_links = "\n".join(f"<p><a href='{url}'>{url}</a></p>" for url in filtered_df['listing_url'])

	body_true = f"""
		<html>
			<head>
				<style>
					body {{
						font-family: Arial, sans-serif;
						margin: 20px;
					}}
					p, li {{
						margin-bottom: 10px;
					}}
					h1, h2, h3, h4, h5, h6 {{
						color: #333;
					}}
				</style>
			</head>
			<body>
				<h2>Proceso completado exitosamente</h2>
				<p>Se han cargado <strong>{new_listings_size}</strong> nuevos registros el día de hoy.</p>
				<p>De ese total, <strong>{filter_df_size}</strong> registros cumplen con los requisitos de su búsqueda.</p>
				<h3>Enlaces a registros que cumplen con los criterios:</h3>
				<ul>
					{url_links}
				</ul>
				<h3>Preferencias de búsqueda:</h3>
				<ul>
					<li>Superficie mínima en m²: {MIN_SUP_M2}</li>
					<li>Cantidad mínima de dormitorios: {MIN_DORMS}</li>
					<li>Cantidad mínima de baños: {MIN_BATHS}</li>
				</ul>
				<p>Recuerde que puede editar las preferencias de búsqueda desde la interfaz gráfica de Airflow.</p>
			</body>
		</html>
	"""

	body_false = f"""
		<html>
			<head>
				<style>
					body {{
						font-family: Arial, sans-serif;
						margin: 20px;
					}}
					p, li {{
						margin-bottom: 10px;
					}}
					h1, h2, h3, h4, h5, h6 {{
						color: #333;
					}}
				</style>
			</head>
			<body>
				<h2>Proceso completado exitosamente</h2>
				<p>Se han cargado <strong>{new_listings_size}</strong> nuevos registros el día de hoy.</p>
				<p>En este día, no se han encontrado nuevos registros que cumplan con las preferencias de búsqueda.</p>
				<p>Si esto continúa en el tiempo, te recomendamos modificar los criterios con nuevos valores. Podrás hacerlo desde la interfaz gráfica de Airflow</p>
				<h3>Preferencias de búsqueda:</h3>
				<ul>
					<li>Superficie mínima en m²: {MIN_SUP_M2}</li>
					<li>Cantidad mínima de dormitorios: {MIN_DORMS}</li>
					<li>Cantidad mínima de baños: {MIN_BATHS}</li>
				</ul>
			</body>
		</html>
	"""

	body = body_true if new_listings_size > 0 else body_false

	email_task = EmailOperator(
		task_id = 'send_email',
		to = 'jbrekesdata@gmail.com',
		subject = 'Nuevas propiedades en Alquiler',
		html_content = body
	)

	email_task.execute(context=kwargs)

def get_new_listings_list(
		REDSHIFT_HOST,
		REDSHIFT_PORT, 
		REDSHIFT_DATABASE, 
		REDSHIFT_USER, 
		REDSHIFT_PASSWORD, 
		REDSHIFT_SCHEMA, 
		REDSHIFT_TABLE, 
		**kwargs
	):
	import os
	import pandas as pd
	import psycopg2
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

	host = REDSHIFT_HOST
	port = REDSHIFT_PORT
	database = REDSHIFT_DATABASE
	user = REDSHIFT_USER
	password = REDSHIFT_PASSWORD
	schema = REDSHIFT_SCHEMA
	table_name = REDSHIFT_TABLE      

	print(f"{host} {port} {database} {user} {password}")

	def redshift_connection(host, port, db, user, pwd):
	  try:
	    print('Connecting to Redshift Cluster')
	    conn = redshift_connector.connect(
	      host = host,
	      database = db,
	      port = int(port),
	      user = user,
	      password = pwd
	    )

	    # Construct the connection URL
	    db_url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

	    # Create an SQLAlchemy engine
	    engine = create_engine(db_url)
	    
	    print('Connection Complete')
	    return conn, engine

	  except Exception as e:
	    print(f'Failed to connect to Redshift: {e}')

	conn, engine = redshift_connection(host, port, database, user, password) 


	# Create function to execute SQL Queries

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

	# Extract last day records
	day_listings_query = f"""
				with last_date as (
					select MAX(process_dt) as process_dt from jbrekesdata_coderhouse.daily_house_rent_listings
				)
				select distinct
					listing_id
					, title_address 
					, title_desc_short 
					, listing_price 
					, expenses_amt 
					, area_built
					, dorms_qty 
					, baths_qty 
					, listing_url
				from {schema}.{table_name} a
				inner join last_date dt
				on a.process_dt = dt.process_dt
			"""

	day_new_listings_df = exec_sql(conn, day_listings_query)

	conn.close()

	day_new_listings = day_new_listings_df.shape[0]

	# Create xCOM to send data in next task
	ti = kwargs['ti']
	ti.xcom_push(key='new_listings_df', value=day_new_listings_df)

def etl_process(REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_SCHEMA, REDSHIFT_TABLE):

	import os
	import pandas as pd
	import psycopg2
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

	    # Limit the length of title_address and title_desc_short to 150 characters
	    max_length = 150
	    title_address = title_address[:max_length]
	    title_desc_short = title_desc_short[:max_length]

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

	print(f"ETL Process complete. Record example: {listings_data[0]}")


	# ### Connect to Redshift Database

	# #### Set connection

	print("Loading data to Redshift")

	host = REDSHIFT_HOST
	port = REDSHIFT_PORT
	database = REDSHIFT_DATABASE
	user = REDSHIFT_USER
	password = REDSHIFT_PASSWORD
	schema = REDSHIFT_SCHEMA
	table_name = REDSHIFT_TABLE      

	print(f"{host} {port} {database} {user} {password}")

	def redshift_connection(host, port, db, user, pwd):
	  try:
	    print('Connecting to Redshift Cluster')
	    conn = redshift_connector.connect(
	      host = host,
	      database = db,
	      port = int(port),
	      user = user,
	      password = pwd
	    )

	    # Construct the connection URL
	    db_url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

	    # Create an SQLAlchemy engine
	    engine = create_engine(db_url)
	    
	    print('Connection Complete')
	    return conn, engine

	  except Exception as e:
	    print(f'Failed to connect to Redshift: {e}')

	conn, engine = redshift_connection(host, port, database, user, password) 


	# Create function to execute SQL Queries

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

	query = f"SELECT DISTINCT listing_id FROM {schema}.{table_name}"

	db_ids = exec_sql(conn, query)
	db_ids['listing_id'] = db_ids['listing_id'].astype(str)

	df = pd.DataFrame(listings_data)

	# Remove duplicates (2 publications with different ID but same department, address and price)
	df = df.drop_duplicates(subset=['title_desc_short', 'title_address', 'listing_price'])
	df.shape

	# Merge df with db_ids using a left join and indicator=True to remove records already in the database
	merged = pd.merge(df, db_ids, on='listing_id', how='left', indicator=True)

	df_clean = merged[merged['_merge'] == 'left_only']
	df_clean = df_clean.drop(columns=['_merge'])
	df_clean.shape


	# #### Append new information

	# Columns in Redshift table don't have the same order as the ones in the dataframe, so I need to order them

	# Create a metadata object with the schema
	metadata = MetaData(schema=schema)

	# Reflect the existing table structure
	table = Table(table_name, metadata, autoload=True, autoload_with=engine)

	# Get the column names and their order from the table
	column_order = [col.name for col in table.columns]

	# Reorder the Dataframe
	df_clean = df_clean[column_order] 


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

	  try:  
	  
	    current_subset.to_sql(table_name, engine, schema=schema, index=False, if_exists='append', chunksize=100)
	  
	  except Exception as e:  
	  	print(f"Failed to upload itaration {i + 1} due to following error: {e}. Continuing with next iteration")

	  end_time = time.time()
	  elapsed_time = end_time - start_time
	  print(f"Time taken for iteration {i + 1}: {elapsed_time} seconds")


	update_end_time = time.time()
	total_elapsed_time = update_end_time - update_start_time

	print(f"Processing complete. Total time: {total_elapsed_time:.2f} seconds")


	# #### Close Connection


	conn.close()