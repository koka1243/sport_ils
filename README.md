# sport_ils
Код выполняет сл. действия:
1) Вставляет данные из GA в ClickHouse по дням, избегая семплирования
2) Загружает результаты SQL из Postgress в ClickHouse
3) Выполняет SQL запрос в ClickHouse и вставляет результат в таблицу ClickHouse
4) Переносит таблицу из ClickHouse в Posgress

- Для корректной работы помимо импортируемых библиотек в коде union_script.py нужно импортировать библиотеку google-api-python-client.
- Корректно указать ссылки на csv файлы на строках C:\Users\user\PycharmProjects\sport_ils\public\transaction_source.csv и C:/Users/user/PycharmProjects/sport_ils/public/pg_2021_05_12.csv
- Прописать переменные окружения
