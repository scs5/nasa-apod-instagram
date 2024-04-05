from botocore.exceptions import NoCredentialsError
from io import StringIO
import requests
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import base64
from instagrapi import Client
import tempfile
import os
from PIL import Image
import io


# Fetch API keys/passwords
NASA_API_KEY = Variable.get('NASA_API_KEY')
IG_USERNAME = 'astropyx'
IG_PASSWORD = Variable.get('IG_PASSWORD')

# Schedule settings
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1, 14, 0),
    'retries': 1,
}


dag = DAG(
    'nasa_apod_dag',
    default_args=default_args,
    description='Download APOD data and post to Instagram',
    schedule_interval='0 14 * * *',
)


def get_apod_data(**kwargs):
    """ Retrieves NASA's Astronomy Picture of the Day (APOD) json data. """
    # Fetch json data
    url = f"https://api.nasa.gov/planetary/apod?api_key={NASA_API_KEY}&hd=TRUE"
    response = requests.get(url)
    data = response.json()
    print(data)

    # Push json data to next task
    kwargs['ti'].xcom_push(key='apod_data', value=data)

get_apod_data_task = PythonOperator(
    task_id='get_apod_data',
    python_callable=get_apod_data,
    provide_context=True,
    dag=dag,
)


def transform_apod_data(**kwargs):
    """ Transform json data to APOD image and associated metadata. """
    # Read json data
    apod_data = kwargs['ti'].xcom_pull(task_ids='get_apod_data', key='apod_data')

    # Save and encode image
    img_data = requests.get(apod_data['hdurl']).content
    img_data_base64 = base64.b64encode(img_data).decode('utf-8')
    
    # Save metadata
    metadata_df = pd.DataFrame([apod_data])
    metadata_csv_buffer = StringIO()
    metadata_df.to_csv(metadata_csv_buffer, index=False)
    metadata_csv_buffer.seek(0)
    
    # Push image and metadata to next task
    kwargs['ti'].xcom_push(key='apod_image_data', value=img_data_base64)
    kwargs['ti'].xcom_push(key='apod_metadata', value=metadata_csv_buffer.getvalue())

transform_apod_data_task = PythonOperator(
    task_id='transform_apod_data',
    python_callable=transform_apod_data,
    provide_context=True,
    dag=dag,
)


def post_to_instagram(**kwargs):
    """ Posts APOD to instagram. """
    # Read image and metadata
    img_data_base64 = kwargs['ti'].xcom_pull(task_ids='transform_apod_data', key='apod_image_data')
    img_data = base64.b64decode(img_data_base64)
    metadata_csv_data = kwargs['ti'].xcom_pull(task_ids='transform_apod_data', key='apod_metadata')

    # Save image to a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as temp_img_file:
        temp_img_file.write(img_data)
        temp_img_path = temp_img_file.name

    # Convert image to JPEG format
    image = Image.open(temp_img_path)
    temp_img_path_jpg = temp_img_path + ".jpg"
    image.save(temp_img_path_jpg)

    # Create caption from metadata (title and explanation)
    metadata_df = pd.read_csv(io.StringIO(metadata_csv_data))
    title = metadata_df['title'].iloc[0]
    explanation = metadata_df['explanation'].iloc[0]
    caption = title + '\n\n' + explanation

    # Log in to instagram
    cl = Client()
    cl.login(IG_USERNAME, IG_PASSWORD)

    # Upload image
    cl.photo_upload(temp_img_path_jpg, caption=caption)

    # Clean up temporary files
    os.unlink(temp_img_path)
    os.unlink(temp_img_path_jpg)

post_to_instagram_task = PythonOperator(
    task_id='post_to_instagram',
    python_callable=post_to_instagram,
    provide_context=True,
    dag=dag,
)


get_apod_data_task >> transform_apod_data_task >> post_to_instagram_task