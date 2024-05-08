## Как развернуть проект?

Создать .env файл в корне проекта со следующим содержимым:

    export AIRFLOW_UID=50000
    POSTGRES_USER=<ИМЯ_ПОЛЬЗОВАТЕЛЯ>
    POSTGRES_PASSWORD=<ПАРОЛЬ>
    POSTGRES_DB=<НАЗВАНИЕ_БД>
    _AIRFLOW_WWW_USER_USERNAME=<ИМЯ_ПОЛЬЗОВАТЕЛЯ>w
    _AIRFLOW_WWW_USER_PASSWORD=<ПАРОЛЬ>

Поднять проект с помощью Docker

    docker-compose up -d

*Запущенный сервер доступен по адресу 127.0.0.1:8080*

Авторизироваться и создать соединение Admin -> Connections
    connection id: postgres
    connection type: Postgres
    host: postgres
    login: <ИМЯ_ПОЛЬЗОВАТЕЛЯ>
    port: 5432