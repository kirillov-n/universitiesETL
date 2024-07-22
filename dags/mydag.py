import pendulum
import requests
import logging
from typing import List, Dict, Any, Tuple

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    schedule="0 3 * * *", # Запуск каждый день в 3:00
    start_date=pendulum.datetime(2024, 5, 7, tz="Europe/Moscow"),
    catchup=False,
    tags=["universities"],
)
def universities_data() -> None:
    import pandas as pd
    
    @task()
    def check_existing_records() -> List[Tuple[str]]:
        conn = PostgresHook(postgres_conn_id="postgres").get_conn()
        cursor = conn.cursor()
        # Проверяем наличие таблицы universities
        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'universities')")
        table_exists = cursor.fetchone()[0]
        
        # Если таблица не существует, создаем ее
        if not table_exists:
            cursor.execute("""
                CREATE TABLE universities (
                    state_province VARCHAR(255),
                    name VARCHAR(255),
                    alpha_two_code CHAR(2),
                    country VARCHAR(255),
                    type VARCHAR(255)
                )
            """)
            conn.commit()
            logging.info("Таблица universities создана")
        cursor.execute("SELECT DISTINCT name FROM universities")
        existing_records = cursor.fetchall()
        cursor.close()
        conn.close()
        return existing_records
    
    @task()
    def extract() -> List[Dict[str, Any]]:
        url = "http://universities.hipolabs.com/search"
        request = requests.get(url)
        request.raise_for_status()
        return request.json()

    @task()
    def transform(data: List[Dict[str, Any]], existing_records: List[Tuple[str]]) -> pd.DataFrame:
        df = pd.DataFrame(data).drop(["web_pages", "domains"], axis=1)
        # Исключаем уже существующие записи
        df = df[~df[["name"]].apply(tuple, axis=1).isin(existing_records)]
        if df.empty:
            return df
        df.rename(columns={"state-province": "state_province"})
        """
        Ищем заведения с ключевыми словами в названии для определения типа заведения: Колледж, университет или институт
        Поиск по ключевым словам ведется на 4 самых популярных языках
        """
        # На английском языке
        df.loc[df["name"].str.contains(r"College", case=False), "type"] = "College"
        df.loc[df["name"].str.contains(r"University", case=False), "type"] = "University"
        df.loc[df["name"].str.contains(r"Institute", case=False), "type"] = "Institute"

        # На испанском языке
        df.loc[df["name"].str.contains(r"Сolegio", case=False), "type"] = "College"
        df.loc[df["name"].str.contains(r"Universidad", case=False), "type"] = "University"
        df.loc[df["name"].str.contains(r"Instituto", case=False), "type"] = "Institute"

        # На немецком языке
        df.loc[df["name"].str.contains(r"Hochschule", case=False), "type"] = "College"
        df.loc[df["name"].str.contains(r"Universität", case=False), "type"] = "University"
        df.loc[df["name"].str.contains(r"Institut", case=False), "type"] = "Institute"

        # На французском языке
        df.loc[df["name"].str.contains(r"Collège", case=False), "type"] = "College"
        df.loc[df["name"].str.contains(r"Université", case=False), "type"] = "University"
        df.loc[df["name"].str.contains(r"Institut", case=False), "type"] = "Institute"
        return df

    @task()
    def load(data: pd.DataFrame) -> None:
        if data.empty:
            logging.info("Нет новых записей для загрузки")
            return
        # Загрузка новых записей в базу данных
        target_fields = ["state_province", "name", "alpha_two_code", "country", "type"]
        PostgresHook(postgres_conn_id="postgres").insert_rows("universities", data.values, target_fields=target_fields)
        logging.info("Данные успешно загружены")

    check_existing = check_existing_records()
    extracted_data = extract()
    transformed_data = transform(extracted_data, check_existing)
    load(transformed_data)


universities_data()
