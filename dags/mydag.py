import pendulum
import requests
import pandas as pd
from typing import List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 7, tz="UTC"),
    catchup=False,
    tags=["universities"],
)
def universities_data():
    @task()
    def extract():
        url = 'http://universities.hipolabs.com/search?limit=10'
        request = requests.get(url)
        request.raise_for_status()
        return request.json()

    @task()
    def transform(data):
        df = pd.DataFrame(data).drop(['web_pages', 'domains'], axis=1)
        df.rename(columns={"state-province": "state_province"})
        df.loc[df['name'].str.contains(r'College', case=False), 'type'] = 'College'
        df.loc[df['name'].str.contains(r'University', case=False), 'type'] = 'University'
        df.loc[df['name'].str.contains(r'Institute', case=False), 'type'] = 'Institute'
        return df

    @task()
    def load(data):
        target_fields = ["state_province", "name", "alpha_two_code", "country", "type"]
        PostgresHook(postgres_conn_id='postgres').insert_rows('universities', data.values, target_fields = target_fields)

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)


universities_data()