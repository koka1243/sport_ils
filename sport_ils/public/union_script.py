from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from time import sleep
from clickhouse_driver import Client
import csv
import psycopg2
import logging
import sys
from datetime import datetime
import pandas as pd
import os


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.basicConfig(stream=sys.stderr, level=logging.ERROR)
logging.captureWarnings(True)
start = (datetime.now())
logging.info('Script start at: ' + str(start))
logging.info('======')

#Подключаем ClickHouse
client = Client(
    host=os.getenv("HOST_CH"),
    user=os.getenv("USER_CH"),
    password=os.getenv("PASSWORD_CH"),
    database=os.getenv('DB_CH'),
    port=9000,
)

logging.info('Подключен ClickHouse')


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = ('credentials.json')  # Подключаем токен доступа к GA
VIEW_ID = os.getenv('VIEW_ID')  # Выбираем id представления откуда забираем данные

logging.info('Подключен GA')

# Старт выполнения кода Google Analytics
def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    analytics = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)

    return analytics

#Генерируем календарь дат из ClickHouse меняем ilss_new.cid_uid_test на верную таблицу
result = client.execute('''   select distinct toString(toDate(toStartOfInterval(start + dd, interval 1 day))) as monday                                                                     
                        FROM (
                           select /*toDate('2022-05-09') as start*/   max (toDate(`Date`))+1 as start  
                            ,toDate(today()) end
                                  ,range(toUInt32( dateDiff('day', start, end)) ) d
                        from ilss_new.cid_uid_final         
                             ) 
                        ARRAY JOIN d as dd             ''' )

#Создаем цикл, где в start_date и end_date подставляем значения из календаря дат выше
for row in result:

    def get_report(analytics, start_date, end_date):
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'pageSize': 100000,
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        # указываем дату начала и дату конца запроса
                        'metrics': [{'expression': 'ga:Users'}],  # указываем показатели
                        'dimensions': [{'name': 'ga:dimension2'}, {'name': 'ga:dimension4'}, {'name': 'ga:date'}]
                        # указываем параметры
                    }]
            }
        ).execute()  # выполняем запрос



    start_date = row[0]   # Выбираем дату начала и окончания для выборки
    end_date = row[0]   # Выбираем дату начала и окончания для выборки


    analytics = initialize_analyticsreporting()
    response = get_report(analytics, start_date, end_date)

# Финал выполнения кода Google Analytics на выходе получаем ответ


## Анализируем полученный json ответ
    reports = response['reports'][0]
    columnHeader = reports['columnHeader']['dimensions']
    metricHeader = reports['columnHeader']['metricHeader']['metricHeaderEntries']

    columns = columnHeader
    for metric in metricHeader:
        columns.append(metric['name'])

    data = pd.json_normalize(reports['data']['rows'])
    data_dimensions = pd.DataFrame(data['dimensions'].tolist())
    data_metrics = pd.DataFrame(data['metrics'].tolist())
    data_metrics = data_metrics.applymap(lambda x: x['values'])
    data_metrics = pd.DataFrame(data_metrics[0].tolist())
    result = pd.concat([data_dimensions, data_metrics], axis=1, ignore_index=True)

    result.rename(columns={0: 'CustomDimension002', 1: 'CustomDimension004', 2: 'Date', 3: 'Users'}, inplace=True)
    result = result[['Date', 'CustomDimension002', 'CustomDimension004', 'Users']]
    ## Получаем финальные значения анализа


    ##Переводим значения в нужный тип данных
    result['Users'] = result['Users'].astype(int)
    result['Date'] = pd.to_datetime(result['Date'].astype(str), format='%Y%m%d').dt.date
    result['CustomDimension002'] = result['CustomDimension002'].astype(str)
    result['CustomDimension004'] = result['CustomDimension004'].astype(str)
    sleep(2)
# Сохраняем в csv
    result.to_csv('cid_uid.csv', index=False)

# Create a generator to fetch parsed rows.
    def row_reader():
        with open('cid_uid.csv', encoding="utf-8") as result_csv:
            for line in csv.DictReader(result_csv):
                yield {
                    'Date': str(line['Date']),
                    'CustomDimension002': str(line['CustomDimension002']),
                    'CustomDimension004': str(line['CustomDimension004']),
                    'Users': int(line['Users'])
                }

#записываем итоговый результат в базу данных за n-ый день
    client.execute("INSERT INTO ilss_new.cid_uid_final VALUES", (line for line in row_reader()))

logging.info('Результат успешно записан в ilss_new.cid_uid_final')

#после цикл повторяется и результат пишется уде за день n+1
#в бд за какой последний день была запись и начинать писать данные со следующего дня


# Старт выполнения кода Google Analytics
def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    analytics = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)

    return analytics

#Генерируем календарь дат из ClickHouse
result = client.execute(''' select distinct toString(toDate(toStartOfInterval(start + dd, interval 1 day))) as monday                                                                     
                        FROM (
                           select /*toDate('2022-05-09') as start*/  max (assumeNotNull(toDate(parseDateTimeBestEffortOrNull(concat(DateHourandMinute, '00')))))+1 as start  
                                  ,toDate(today()) end
                                  ,range(toUInt32( dateDiff('day', start, end)) ) d
                        from ilss_new.click_streem_ga_final  
                             ) 
                        ARRAY JOIN d as dd     ''')

#Создаем цикл, где в start_date и end_date подставляем значения из календаря дат выше
for row in result:

    def get_report(analytics, start_date, end_date):
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'pageSize': 100000,
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        # указываем дату начала и дату конца запроса
                        'metrics': [{'expression': 'ga:Users'}],  # указываем показатели
                        'dimensions': [{'name': 'ga:dateHourMinute'}, {'name': 'ga:dimension2'}, {'name': 'ga:SourceMedium'}, {'name': 'ga:Campaign'}, {'name': 'ga:pagePath'}]
                        # указываем параметры
                    }]
            }
        ).execute()  # выполняем запрос



    start_date = row[0]   # Выбираем дату начала и окончания для выборки
    end_date = row[0]   # Выбираем дату начала и окончания для выборки


    analytics = initialize_analyticsreporting()
    response = get_report(analytics, start_date, end_date)

# Финал выполнения кода Google Analytics на выходе получаем ответ


## Анализируем полученный json ответ
    reports = response['reports'][0]
    columnHeader = reports['columnHeader']['dimensions']
    metricHeader = reports['columnHeader']['metricHeader']['metricHeaderEntries']

    columns = columnHeader
    for metric in metricHeader:
        columns.append(metric['name'])

    data = pd.json_normalize(reports['data']['rows'])
    data_dimensions = pd.DataFrame(data['dimensions'].tolist())
    data_metrics = pd.DataFrame(data['metrics'].tolist())
    data_metrics = data_metrics.applymap(lambda x: x['values'])
    data_metrics = pd.DataFrame(data_metrics[0].tolist())
    result = pd.concat([data_dimensions, data_metrics], axis=1, ignore_index=True)

    result.rename(columns={0: 'DateHourandMinute', 1: 'CustomDimension002', 2: 'SourceMedium', 3: 'Campaign', 4: 'Page', 5: 'Users'}, inplace=True)
    result = result[['DateHourandMinute', 'CustomDimension002', 'SourceMedium', 'Campaign', 'Page', 'Users']]
    ## Получаем финальные значения анализа


    ##Переводим значения в нужный тип данных
    result['Users'] = result['Users'].astype(int)
    result['DateHourandMinute'] = result['DateHourandMinute'].astype(str)
    result['CustomDimension002'] = result['CustomDimension002'].astype(str)
    result['SourceMedium'] = result['SourceMedium'].astype(str)
    result['Campaign'] = result['Campaign'].astype(str)
    result['Page'] = result['Page'].astype(str)
    sleep(1)
# Сохраняем в csv
    result.to_csv('click_streem_ga.csv', index=False)


# Create a generator to fetch parsed rows.
    def row_reader():
        with open('click_streem_ga.csv', encoding="utf-8") as result_csv:
            for line in csv.DictReader(result_csv):
                yield {
                    'DateHourandMinute': str(line['DateHourandMinute']),
                    'CustomDimension002': str(line['CustomDimension002']),
                    'SourceMedium': str(line['SourceMedium']),
                    'Campaign': str(line['Campaign']),
                    'Page': str(line['Page']),
                    'Users': int(line['Users'])
                }

#записываем итоговый результат в базу данных за n-ый день
    client.execute("INSERT INTO ilss_new.click_streem_ga_final VALUES", (line for line in row_reader()))

logging.info('Результат успешно записан в ilss_new.click_streem_ga_final')






# Старт выполнения кода Google Analytics
def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    analytics = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)

    return analytics

#Генерируем календарь дат из ClickHouse
result = client.execute(''' select distinct toString(toDate(toStartOfInterval(start + dd, interval 1 day))) as monday                                                                     
                        FROM (
                           select /*toDate('2022-05-09') as start*/  max (assumeNotNull(toDate(parseDateTimeBestEffortOrNull(concat(DateHourandMinute, '00')))))+1 as start  
                                  ,toDate(today()) end
                                  ,range(toUInt32( dateDiff('day', start, end)) ) d
                        from ilss_new.click_streem_ga_action_final 
                             ) 
                        ARRAY JOIN d as dd   ''')

#Создаем цикл, где в start_date и end_date подставляем значения из календаря дат выше
for row in result:

    def get_report(analytics, start_date, end_date):
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'pageSize': 100000,
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        # указываем дату начала и дату конца запроса
                        'metrics': [{'expression': 'ga:Users'}],  # указываем показатели
                        'dimensions': [{'name': 'ga:dateHourMinute'}, {'name': 'ga:dimension2'}, {'name': 'ga:SourceMedium'}, {'name': 'ga:Campaign'}, {'name': 'ga:eventAction'}]
                        # указываем параметры
                    }]
            }
        ).execute()  # выполняем запрос



    start_date = row[0]   # Выбираем дату начала и окончания для выборки
    end_date = row[0]   # Выбираем дату начала и окончания для выборки


    analytics = initialize_analyticsreporting()
    response = get_report(analytics, start_date, end_date)

# Финал выполнения кода Google Analytics на выходе получаем ответ


## Анализируем полученный json ответ
    reports = response['reports'][0]
    columnHeader = reports['columnHeader']['dimensions']
    metricHeader = reports['columnHeader']['metricHeader']['metricHeaderEntries']

    columns = columnHeader
    for metric in metricHeader:
        columns.append(metric['name'])

    data = pd.json_normalize(reports['data']['rows'])
    data_dimensions = pd.DataFrame(data['dimensions'].tolist())
    data_metrics = pd.DataFrame(data['metrics'].tolist())
    data_metrics = data_metrics.applymap(lambda x: x['values'])
    data_metrics = pd.DataFrame(data_metrics[0].tolist())
    result = pd.concat([data_dimensions, data_metrics], axis=1, ignore_index=True)

    result.rename(columns={0: 'DateHourandMinute', 1: 'CustomDimension002', 2: 'SourceMedium', 3: 'Campaign', 4: 'EventAction', 5: 'Users'}, inplace=True)
    result = result[['DateHourandMinute', 'CustomDimension002', 'SourceMedium', 'Campaign', 'EventAction', 'Users']]
    ## Получаем финальные значения анализа


    ##Переводим значения в нужный тип данных
    result['Users'] = result['Users'].astype(int)
    result['DateHourandMinute'] = result['DateHourandMinute'].astype(str)
    result['CustomDimension002'] = result['CustomDimension002'].astype(str)
    result['SourceMedium'] = result['SourceMedium'].astype(str)
    result['Campaign'] = result['Campaign'].astype(str)
    result['EventAction'] = result['EventAction'].astype(str)
    sleep(1)
# Сохраняем в csv
    result.to_csv('click_streem_ga_action.csv', index=False)


# Create a generator to fetch parsed rows.
    def row_reader():
        with open('click_streem_ga_action.csv', encoding="utf-8") as result_csv:
            for line in csv.DictReader(result_csv):
                yield {
                    'DateHourandMinute': str(line['DateHourandMinute']),
                    'CustomDimension002': str(line['CustomDimension002']),
                    'SourceMedium': str(line['SourceMedium']),
                    'Campaign': str(line['Campaign']),
                    'EventAction': str(line['EventAction']),
                    'Users': int(line['Users'])
                }

#записываем итоговый результат в базу данных за n-ый день
    client.execute("INSERT INTO ilss_new.click_streem_ga_action_final VALUES", (line for line in row_reader()))

logging.info('Результат успешно записан в ilss_new.click_streem_ga_action_final' )







#Подключаем PostgreSQL
cnx = psycopg2.connect(user=os.getenv('USER_PG'),
                               password=os.getenv('PASSWORD_PG'),
                               host=os.getenv('HOST_PG'),
                               port=os.getenv('PORT_PG'),
                               database=os.getenv('DB_PG'))

cnx.set_client_encoding('UTF8')
cursor = cnx.cursor()

logging.info('Подключен PG')

#Выполняем запрос в PostgreSQL и записывем результат в CSV
query = ''' COPY (select dt,
                    period_type,
                    id,
                    reg_name,
                    country,
                    user_id,
                    program_title,
                    directions_title,
                    sm,
                    rating_in_section,
                    number_transactions,
                    rating_in_direction,
                    number_transactions_direction,
                    start_date,
                    first_user_id,
                    first_dt,
                    is_personal
from 
(
select *
from 
(	
		SELECT   
		         payments.created_at as dt
		         ,'Daily' as period_type
		         ,payments.id
		         ,regions.title as reg_name
		         ,regions.country as country
		         ,users.id as user_id
		        , programs.title AS program_title
		        , directions.title as directions_title
		        , payments.amount_cents/100 AS sm
		        ,row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC)  AS rating_in_section
		        ,case when row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC) in (1,2,3,4) 
		              then row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC)::text else '5+' end as number_transactions
		         ,row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC)  AS rating_in_direction
		         ,case when row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC) in (1,2,3,4) 
		              then row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC)::text else '5+' end as number_transactions_direction
		         ,booking_groups.start_date as start_date  -- дата начала программы		
		          ,case when programs.program_kind = 6 then 1 else 0 end as is_personal        
		FROM payments
		INNER JOIN bookings
		ON payments.resource_id = bookings.id 
		INNER JOIN users
		ON bookings.user_id = users.id
		INNER JOIN booking_groups
		ON bookings.booking_group_id = booking_groups.id
		INNER JOIN programs
		ON booking_groups.program_id = programs.id
		INNER JOIN directions
		ON programs.direction_id = directions.id
		INNER JOIN regions
		ON directions.region_id = regions.id
		WHERE (directions.title = 'TRIATHLON' 
		        or directions.title = 'SWIMMING'
		        or directions.title = 'RUNNING'
		        or directions.title = 'CYCLING'
		        or directions.title = 'SKIING'
		        )
		--and regions.id = 1
		 and payments.resource_type = 'Booking'
		AND payments.created_at >= '2019-01-01'
		AND payments.status IN (6,9) 
		AND payments.amount_cents > 100
		AND programs.program_kind NOT IN (7)
	order by user_id,dt	
)t	
left join 
(
		SELECT   		            
		         users.id as first_user_id		        
		         ,min (payments.created_at) first_dt
		FROM payments
		INNER JOIN bookings
		ON payments.resource_id = bookings.id 
		INNER JOIN users
		ON bookings.user_id = users.id
		INNER JOIN booking_groups
		ON bookings.booking_group_id = booking_groups.id
		INNER JOIN programs
		ON booking_groups.program_id = programs.id
		INNER JOIN directions
		ON programs.direction_id = directions.id
		INNER JOIN regions
		ON directions.region_id = regions.id
		WHERE (directions.title = 'TRIATHLON' 
		        or directions.title = 'SWIMMING'
		        or directions.title = 'RUNNING'
		        or directions.title = 'CYCLING'
		        or directions.title = 'SKIING'
		        )
		--and regions.id = 1
		 and payments.resource_type = 'Booking'
		AND payments.created_at >= '2019-01-01'
		AND payments.status IN (6,9) 
		AND payments.amount_cents > 100
		AND programs.program_kind NOT IN (7)
	group by users.id
)as t_min
on first_user_id=user_id 
union all
select * 
from
(
	   SELECT   
		         date_trunc('week', payments.created_at) as dt
		         ,'Weekly' as period_type
		         ,payments.id
		         ,regions.title as reg_name
		         ,regions.country as country
		         ,users.id as user_id
		        , programs.title AS program_title
		        , directions.title as directions_title
		        , payments.amount_cents/100 AS sm
		          ,row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC)  AS rating_in_section
		        ,case when row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC) in (1,2,3,4) 
		              then row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC)::text else '5+' end as number_transactions
		         ,row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC)  AS rating_in_direction
		         ,case when row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC) in (1,2,3,4) 
		              then row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC)::text else '5+' end as number_transactions_direction
		         , date_trunc('week', booking_groups.start_date) as start_date
		          ,case when programs.program_kind = 6 then 1 else 0 end as is_personal
		       --  ,booking_groups.start_date -- дата начала программы		        
		FROM payments
		INNER JOIN bookings
		ON payments.resource_id = bookings.id 
		INNER JOIN users
		ON bookings.user_id = users.id
		INNER JOIN booking_groups
		ON bookings.booking_group_id = booking_groups.id
		INNER JOIN programs
		ON booking_groups.program_id = programs.id
		INNER JOIN directions
		ON programs.direction_id = directions.id
		INNER JOIN regions
		ON directions.region_id = regions.id
		WHERE  (directions.title = 'TRIATHLON' 
		        or directions.title = 'SWIMMING'
		        or directions.title = 'RUNNING'
		        or directions.title = 'CYCLING'
		        or directions.title = 'SKIING'
		        )
		-- and regions.id = 1
		 and payments.resource_type = 'Booking'
		AND payments.created_at >= '2019-01-01'
		AND payments.status IN (6,9) 
		AND payments.amount_cents > 100
		AND programs.program_kind NOT IN (7)
		order by user_id,dt	
)t1
left join 
(
		SELECT   		            
		         users.id as first_user_id		        
		         ,date_trunc( 'week', min (payments.created_at)) first_dt
		FROM payments
		INNER JOIN bookings
		ON payments.resource_id = bookings.id 
		INNER JOIN users
		ON bookings.user_id = users.id
		INNER JOIN booking_groups
		ON bookings.booking_group_id = booking_groups.id
		INNER JOIN programs
		ON booking_groups.program_id = programs.id
		INNER JOIN directions
		ON programs.direction_id = directions.id
		INNER JOIN regions
		ON directions.region_id = regions.id
		WHERE (directions.title = 'TRIATHLON' 
		        or directions.title = 'SWIMMING'
		        or directions.title = 'RUNNING'
		        or directions.title = 'CYCLING'
		        or directions.title = 'SKIING'
		        )
		--and regions.id = 1
		 and payments.resource_type = 'Booking'
		AND payments.created_at >= '2019-01-01'
		AND payments.status IN (6,9) 
		AND payments.amount_cents > 100
		AND programs.program_kind NOT IN (7)
	group by users.id
)as t_min_1
on first_user_id=user_id 
	union all
select *
from 
(	
	SELECT   
		         date_trunc('month', payments.created_at) as dt
		         ,'Monthly' as period_type
		         ,payments.id
		         ,regions.title as reg_name
		         ,regions.country as country
		         ,users.id as user_id
		        , programs.title AS program_title
		        , directions.title as directions_title
		        , payments.amount_cents/100 AS sm
		         ,row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC)  AS rating_in_section
		        ,case when row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC) in (1,2,3,4) 
		              then row_number() OVER (PARTITION BY user_id ORDER BY payments.created_at ASC)::text else '5+' end as number_transactions
		         ,row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC)  AS rating_in_direction
		         ,case when row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC) in (1,2,3,4) 
		              then row_number() OVER (PARTITION BY user_id,directions.title ORDER BY payments.created_at ASC)::text else '5+' end as number_transactions_direction
		              ,date_trunc('month', booking_groups.start_date) as start_date
		        ,case when programs.program_kind = 6 then 1 else 0 end as is_personal
		      --   ,booking_groups.start_date -- дата начала программы		                     
		FROM payments
		INNER JOIN bookings
		ON payments.resource_id = bookings.id
		INNER JOIN users
		ON bookings.user_id = users.id
		INNER JOIN booking_groups
		ON bookings.booking_group_id = booking_groups.id
		INNER JOIN programs
		ON booking_groups.program_id = programs.id
		INNER JOIN directions
		ON programs.direction_id = directions.id
		INNER JOIN regions
		ON directions.region_id = regions.id
		WHERE --regions.id = 1
		 (directions.title = 'TRIATHLON' 
		        or directions.title = 'SWIMMING'
		        or directions.title = 'RUNNING'
		        or directions.title = 'CYCLING'
		        or directions.title = 'SKIING'
		        )
		 --and regions.id = 1
		 and payments.resource_type = 'Booking'
		AND payments.created_at >= '2019-01-01'
		AND payments.status IN (6,9) 
		AND payments.amount_cents > 100
		and programs.program_kind not IN (7)
		order by user_id,dt
)t2
left join 
(
		SELECT   		            
		         users.id as first_user_id		        
		         ,date_trunc( 'month', min (payments.created_at)) first_dt
		FROM payments
		INNER JOIN bookings
		ON payments.resource_id = bookings.id 
		INNER JOIN users
		ON bookings.user_id = users.id
		INNER JOIN booking_groups
		ON bookings.booking_group_id = booking_groups.id
		INNER JOIN programs
		ON booking_groups.program_id = programs.id
		INNER JOIN directions
		ON programs.direction_id = directions.id
		INNER JOIN regions
		ON directions.region_id = regions.id
		WHERE (directions.title = 'TRIATHLON' 
		        or directions.title = 'SWIMMING'
		        or directions.title = 'RUNNING'
		        or directions.title = 'CYCLING'
		        or directions.title = 'SKIING'
		        )
		--and regions.id = 1
		AND payments.created_at >= '2019-01-01'
		and  payments.resource_type = 'Booking'
		AND payments.status IN (6,9) 
		AND payments.amount_cents > 100
		AND programs.program_kind NOT IN (7)
	group by users.id
)as t_min_1
on first_user_id=user_id 
) finsl_t
) TO STDOUT WITH CSV DELIMITER ',' HEADER '''

with open("C:/Users/user/PycharmProjects/sport_ils/public/pg_2021_05_12.csv", "w" , encoding="utf-8") as file:
    cursor.copy_expert(query, file) # записывем результат в CSV

logging.info('Выполнен запрос PG и записан в CSV pg_2021_05_12')

#Читаем результат запроса из CSV
def row_reader():
    with open('pg_2021_05_12.csv', encoding="utf-8") as result_csv:
        for line in csv.DictReader(result_csv):
            yield {
                'dt': str(line['dt']),
                'period_type': str(line['period_type']),
                'id': int(line['id']),
                'reg_name': str(line['reg_name']),
                'country': str(line['country']),
                'user_id': int(line['user_id']),
                'program_title': str(line['program_title']),
                'directions_title': str(line['directions_title']),
                'sm': int(line['sm']),
                'rating_in_section': int(line['rating_in_section']),
                'number_transactions': str(line['number_transactions']),
                'rating_in_direction': int(line['rating_in_direction']),
                'number_transactions_direction': str(line['number_transactions_direction']),
                'start_date': str(line['start_date']),
                'first_user_id': int(line['first_user_id']),
                'first_dt': str(line['first_dt']),
                'is_personal': int(line['is_personal'])
                }

logging.info('Преобразован  CSV в формат с правильными колонками pg_2021_05_12')

#удаляем данные в таблице ClickHouse, которые были записаны ранее
client.execute("TRUNCATE TABLE ilss_new.pg_2021_05_12_final")

logging.info('Очищена таблица в ClickHouse ilss_new.pg_2021_05_12_final')

#записываем итоговый результат в таблицу ClickHouse целиком
client.execute("INSERT INTO ilss_new.pg_2021_05_12_final VALUES", (line for line in row_reader()))

logging.info('Записаны значения из CSV в ClickHouse pg_2021_05_12_final')

#удаляем данные в таблице ClickHouse, которые были записаны ранее в таблице ilss_new.transaction_source
client.execute("TRUNCATE TABLE ilss_new.transaction_source_final")

logging.info('Очищена таблица в ClickHouse transaction_source_final')

#записываем итоговый результат в таблицу ClickHouse ilss_new.transaction_source целиком нужно заменить часть запроса после выдачи прав
client.execute(''' INSERT INTO ilss_new.transaction_source_final
                select id
                                        ,last_date_ga
                                        ,last_medium
                                        ,last_source
                                        ,last_campaign
                                        ,last_src
                                        ,first_date_ga
                                        ,first_medium
                                        ,first_source
                                        ,first_campaign
                                        ,first_src
                                        ,dt_ga_pes_first
                                        ,medium_pes_first
                                        ,source_pes_first
                                        ,campaign_pes_first
                                        ,src_pes_first
                                        ,dt_ga_pes_last
                                        ,medium_pes_last
                                        ,source_pes_last
                                        ,campaign_pes_last
                                        ,src_pes_last
       from
    (
            select not_pes.id
                                            ,last_date_ga
                                            ,last_medium
                                            ,last_source
                                            ,last_campaign
                                            ,last_src
                                            ,first_date_ga
                                            ,first_medium
                                            ,first_source
                                            ,first_campaign
                                            ,first_src
                                            ,dt_ga_pes_first
                                            ,medium_pes_first
                                            ,source_pes_first
                                            ,campaign_pes_first
                                            ,src_pes_first
                from
            (
            ---первый и последний источник в окне 6 дней без пессимизации органики и директа
                        select id
                                            ,dt_ga[-1] as last_date_ga
                                            ,medium_arr[-1] as last_medium
                                            ,source_arr[-1] as last_source
                                            ,campaign_arr[-1] as last_campaign
                                            ,src_arr[-1] as last_src
                                            ,dt_ga[1] as first_date_ga
                                            ,medium_arr[1] as first_medium
                                            ,source_arr[1] as first_source
                                            ,campaign_arr[1] as first_campaign
                                            ,src_arr[1] as first_src
                                            ,if(last_medium=first_medium,1,0) as zero_not
                        from
                        (
                            select correct_dt_ga
                                            ,id
                                            ,reg_name
                                            ,directions_title
                                            ,program_title
                                            ,user_id
                                            ,groupArray(date_time_ga) as dt_ga
                                            ,groupArray(medium) as medium_arr
                                            ,groupArray(source) as source_arr
                                            ,groupArray(Campaign) as campaign_arr
                                            ,groupArray(src) as src_arr     
                                            ,groupArray(src_priority) as priority
                                    from
                                    (
                                    select correct_dt_ga
                                            ,id
                                            ,reg_name
                                            ,directions_title
                                            ,program_title
                                            ,date_time_ga
                                            ,user_id
                                            ,source
                                            ,src
                                            ,medium
                                            ,Campaign
                                            ,CustomDimension002
                                            ,action_name
                                            ,multiIf(src in ('social','cpm','cpc') or (src = 'email' and medium='newsfeed' and source='facebook')  ,2                                           
                                                     ,src in ('smm','модалка','исключаемый источник') or (src = 'email' and medium ='email' and source in ('es','chats'))
                                                        or (src = 'telegram' and medium ='chat' and source in ('telegram')), 1                                             	   
                                            	         ,0) as src_priority
                                    from
                                    (
                                    select toDateTime(parseDateTimeBestEffortOrNull(dt)) as dtr,
								period_type,
								id,
								reg_name,
								country,
								user_id,
								program_title,
								directions_title,
								sm,
								rating_in_section,
								number_transactions,
								rating_in_direction,
								number_transactions_direction,
								start_date,
								first_user_id,
								first_dt,
								is_personal						
								, multiIf((reg_name='Москва'
										or reg_name='Санкт-Петербург'
										or reg_name='Владимир'
										or reg_name='Мытищи'
										or  reg_name='Ростов-на-Дону'
										or reg_name='Севастополь'
										or reg_name='Реутов'
										or reg_name='Тверь'
										or reg_name='Ярославль'
										or reg_name='Киров'
										or  reg_name='Королев'
										or reg_name='Набережные Челны'
										or reg_name='Казань'
										or reg_name='Одинцово'
										or reg_name='Белгород'
										or reg_name='Пятигорск'
										or reg_name='Симферополь'
										or reg_name='Нижний Новгород'
										or reg_name='Тамбов'
										or reg_name='Химки'
										) , addHours(dtr,3),
										reg_name='Одесса' ,  addHours(dtr,2),
										reg_name='Самара' or reg_name='Ижевск' or reg_name='Dubai' ,  addHours(dtr,4),
										(reg_name='Уфа' or reg_name='Екатеринбург' or reg_name='Пермь' or reg_name='Нижний Тагил' or reg_name='Ханты-Мансийск') ,  addHours(dtr,5),
									       reg_name='Алматы' or reg_name='Астана' ,  addHours(dtr,6),
									        (reg_name='Хабаровск' or reg_name='Владивосток' ),  addHours(dtr,10),
									         (reg_name='Новосибирск' or reg_name='Красноярск' or reg_name='Кемерово' or reg_name='Абакан' or reg_name='Томск')  ,  addHours(dtr,7), null) as correct_dt
									   ,addHours(dtr,3) as correct_dt_ga
						 from pg_2021_05_12_final 
						where period_type ='Daily'
						  and toDate(dtr) >= '2021-01-01'
                                    ) r1
                                    left join
                                    (
								---click streem
								select distinct date_time_ga
												,source
												,medium
												,Campaign
												,CustomDimension002
												,action_name
												,CustomDimension005 as CustomDimension004
												,multiIf(
											    medium IN ('virtual') ,'модалка' ,
											    source IN ('telegram' ,'event') and medium in ('chat' ,'breakfast'),'telegram' ,
											    medium IN ('cpc','CPC' , 'google_adw') or source LIKE ('%yabs%') and medium IN ('referral') ,'cpc' ,
											    medium IN ('cpm') , 'cpm',
											    medium IN ('twitter','max') ,'partners' ,
											    source LIKE ('amo%') AND medium IN ('email' , 'referral'), 'amo',
											    medium IN ('email',  'e-mail' ,'newsfeed') ,'email' ,											    
											    medium IN ('organic') or ( source IN ('yandex.ru', 'go.mail.ru','mail.ru' ,'yandex.by', 'yandex.ua', 'yandex.kz','yandex.fr' )) and medium IN ('referral') ,'organic' ,
											    medium IN ('sms', 'message' )  ,'sms',
											    medium IN ('referral', '(notset)') and source IN ('ok.ru' , 'm.ok.ru' ,'away.vk.com', 'vk.com', 'm.vk.com', 'youtube.com' ,'pinterest.com',
											    'pinterest.ru','m.youtube.com', 'web.telegram.org', 'web.telegram.org.ru' , 'web.whatsapp.com','l.instagram.com','instagram.com' ,'lm.facebook.com'
											    ,'m.facebook.com')  ,'social',
											    medium IN ('telegrampost','telegram','taplink_swimming','taplink_running','taplink','swipe' ,'social','post','ironstartelegram','ironstar','instapost'
											    			,'instagram_post','instagram','insta_post','direct') and source IN ('smm', 'inst','vk','instagram','fb','ad','facebook') 
											    			OR medium in ('post') ,'smm',
											   source IN ( 'blog') ,'blog',
											    (medium IN ('swimming' ,'running','cycling') and source IN ('instagram', 'telegram'))  ,'link' ,
											    medium like ('%lectorium') or (medium in ('registration') and source IN ('Lectorium'))  ,'lectorium',
											    medium in ('referral') and source IN ('3ds.cloudpayments.ru', 'secure5.arcot.com', '3ds.sdm.ru' ,'acs3.sbrf.ru')  ,'платежные системы',
											     (medium in ('referral') and source like ('%ilovesupersport%')) OR (source in ('n459011.yclients.com'))   ,'переход внутри сайта',
											   (medium in ('referral') and source IN ('taplink.cc'))   ,'исключаемый источник',  											     
											    medium IN ('referral')  ,'referral',
											    medium IN ('(none)') and source IN ('(direct)') ,'direct'											 
											    ,'other') src
								from
								    (
										select parseDateTimeBestEffortOrNull(concat(DateHourandMinute, '00'))+3600 as date_time_ga ---привели к московскому времени
												,splitByChar('/',replace(SourceMedium, ' ', '')) [1] as source
												,splitByChar('/',replace(SourceMedium, ' ', '')) [2] as medium
												,Campaign
												,CustomDimension002
												,Page as action_name
										from ilss_new.click_streem_ga_final
									union all
										select parseDateTimeBestEffortOrNull(concat(DateHourandMinute, '00'))+3600 as date_time_ga  ---привели к московскому времени
												,splitByChar('/',replace(SourceMedium, ' ', '')) [1] as source
												,splitByChar('/',replace(SourceMedium, ' ', '')) [2] as medium
												,Campaign
												,CustomDimension002
												,EventAction as action_name
										from ilss_new.click_streem_ga_action_final 
									)t1
								 join
								       (
									select
										Date
										,CustomDimension002
										,toInt32OrNull(CustomDimension004) as CustomDimension005
										,Users
										from ilss_new.cid_uid_final 
										)t2
									on t1.CustomDimension002=t2.CustomDimension002 --and toDate(date_time_ga)=t2.Date
									where CustomDimension005!=0
								---click streem
								) streem
                                    on user_id = CustomDimension004
                                      where (streem.date_time_ga BETWEEN correct_dt_ga-86400*6 AND correct_dt_ga+2)---окно в 6 дней
                                    order by dtr, id, streem.date_time_ga
                                    ) fd
                                    group by  correct_dt_ga, id, reg_name ,directions_title ,program_title , user_id
                        ) arr_el
            ---первый и последний источник в окне 6 дней без пессимизации органики и директа
            ) not_pes
            left join
            (
                ---пессимизированы органика и директ, выбраны былиже началу каналы
                            select id
                                            ,argMax(date_time_ga,src_priority) as dt_ga_pes_first
                                            ,argMax(medium ,src_priority) as medium_pes_first
                                            ,argMax(source,src_priority) as source_pes_first
                                            ,argMax(Campaign,src_priority) as campaign_pes_first
                                            ,argMax(src,src_priority) as src_pes_first
                                    from
                                    (
                                    select correct_dt_ga
                                            ,id
                                            ,reg_name
                                            ,directions_title
                                            ,program_title
                                            ,date_time_ga
                                            ,user_id
                                            ,source
                                            ,medium
                                            ,Campaign
                                            ,CustomDimension002
                                            ,action_name
                                            ,src
                                             ,multiIf(src in ('social','cpm','cpc') or (src = 'email' and medium='newsfeed' and source='facebook')  ,2                                           
                                                     ,src in ('smm','модалка','исключаемый источник') or (src = 'email' and medium ='email' and source in ('es','chats'))
                                                        or (src = 'telegram' and medium ='chat' and source in ('telegram')), 1                                             	   
                                            	         ,0) as src_priority
                                    from
                                    (
                                    select toDateTime(parseDateTimeBestEffortOrNull(dt)) as dtr,
								period_type,
								id,
								reg_name,
								country,
								user_id,
								program_title,
								directions_title,
								sm,
								rating_in_section,
								number_transactions,
								rating_in_direction,
								number_transactions_direction,
								start_date,
								first_user_id,
								first_dt,
								is_personal						
								, multiIf((reg_name='Москва'
										or reg_name='Санкт-Петербург'
										or reg_name='Владимир'
										or reg_name='Мытищи'
										or  reg_name='Ростов-на-Дону'
										or reg_name='Севастополь'
										or reg_name='Реутов'
										or reg_name='Тверь'
										or reg_name='Ярославль'
										or reg_name='Киров'
										or  reg_name='Королев'
										or reg_name='Набережные Челны'
										or reg_name='Казань'
										or reg_name='Одинцово'
										or reg_name='Белгород'
										or reg_name='Пятигорск'
										or reg_name='Симферополь'
										or reg_name='Нижний Новгород'
										or reg_name='Тамбов'
										or reg_name='Химки'
										) , addHours(dtr,3),
										reg_name='Одесса' ,  addHours(dtr,2),
										reg_name='Самара' or reg_name='Ижевск' or reg_name='Dubai' ,  addHours(dtr,4),
										(reg_name='Уфа' or reg_name='Екатеринбург' or reg_name='Пермь' or reg_name='Нижний Тагил' or reg_name='Ханты-Мансийск') ,  addHours(dtr,5),
									       reg_name='Алматы' or reg_name='Астана' ,  addHours(dtr,6),
									        (reg_name='Хабаровск' or reg_name='Владивосток' ),  addHours(dtr,10),
									         (reg_name='Новосибирск' or reg_name='Красноярск' or reg_name='Кемерово' or reg_name='Абакан' or reg_name='Томск')  ,  addHours(dtr,7), null) as correct_dt
									   ,addHours(dtr,3) as correct_dt_ga
						 from pg_2021_05_12_final 
						where period_type ='Daily'
						  and toDate(dtr) >= '2021-01-01'
                                    ) r1
                                    left join
                                    (
								---click streem
								select distinct date_time_ga
												,source
												,medium
												,Campaign
												,CustomDimension002
												,action_name
												,CustomDimension005 as CustomDimension004
												,multiIf(
											    medium IN ('virtual') ,'модалка' ,
											    source IN ('telegram' ,'event') and medium in ('chat' ,'breakfast'),'telegram' ,
											    medium IN ('cpc','CPC' , 'google_adw') or source LIKE ('%yabs%') and medium IN ('referral') ,'cpc' ,
											    medium IN ('cpm') , 'cpm',
											    medium IN ('twitter','max') ,'partners' ,
											    source LIKE ('amo%') AND medium IN ('email' , 'referral'), 'amo',
											    medium IN ('email',  'e-mail' ,'newsfeed') ,'email' ,											    
											    medium IN ('organic') or ( source IN ('yandex.ru', 'go.mail.ru','mail.ru' ,'yandex.by', 'yandex.ua', 'yandex.kz','yandex.fr' )) and medium IN ('referral') ,'organic' ,
											    medium IN ('sms', 'message' )  ,'sms',
											    medium IN ('referral', '(notset)') and source IN ('ok.ru' , 'm.ok.ru' ,'away.vk.com', 'vk.com', 'm.vk.com', 'youtube.com' ,'pinterest.com',
											    'pinterest.ru','m.youtube.com', 'web.telegram.org', 'web.telegram.org.ru' , 'web.whatsapp.com','l.instagram.com','instagram.com' ,'lm.facebook.com'
											    ,'m.facebook.com')  ,'social',
											    medium IN ('telegrampost','telegram','taplink_swimming','taplink_running','taplink','swipe' ,'social','post','ironstartelegram','ironstar','instapost'
											    			,'instagram_post','instagram','insta_post','direct') and source IN ('smm', 'inst','vk','instagram','fb','ad','facebook') 
											    			OR medium in ('post') ,'smm',
											   source IN ( 'blog') ,'blog',
											    (medium IN ('swimming' ,'running','cycling') and source IN ('instagram', 'telegram'))  ,'link' ,
											    medium like ('%lectorium') or (medium in ('registration') and source IN ('Lectorium'))  ,'lectorium',
											    medium in ('referral') and source IN ('3ds.cloudpayments.ru', 'secure5.arcot.com', '3ds.sdm.ru' ,'acs3.sbrf.ru')  ,'платежные системы',
											     (medium in ('referral') and source like ('%ilovesupersport%')) OR (source in ('n459011.yclients.com'))   ,'переход внутри сайта',
											   (medium in ('referral') and source IN ('taplink.cc'))   ,'исключаемый источник',  											     
											    medium IN ('referral')  ,'referral',
											    medium IN ('(none)') and source IN ('(direct)') ,'direct'											 
											    ,'other') src
								from
								    (
										select parseDateTimeBestEffortOrNull(concat(DateHourandMinute, '00'))+3600 as date_time_ga ---привели к московскому времени
												,splitByChar('/',replace(SourceMedium, ' ', '')) [1] as source
												,splitByChar('/',replace(SourceMedium, ' ', '')) [2] as medium
												,Campaign
												,CustomDimension002
												,Page as action_name
										from ilss_new.click_streem_ga_final
									union all
										select parseDateTimeBestEffortOrNull(concat(DateHourandMinute, '00'))+3600 as date_time_ga  ---привели к московскому времени
												,splitByChar('/',replace(SourceMedium, ' ', '')) [1] as source
												,splitByChar('/',replace(SourceMedium, ' ', '')) [2] as medium
												,Campaign
												,CustomDimension002
												,EventAction as action_name
										from ilss_new.click_streem_ga_action_final 
									)t1
								 join
								       (
									select
										Date
										,CustomDimension002
										,toInt32OrNull(CustomDimension004) as CustomDimension005
										,Users
										from ilss_new.cid_uid_final 
										)t2
									on t1.CustomDimension002=t2.CustomDimension002 --and toDate(date_time_ga)=t2.Date
									where CustomDimension005!=0
								---click streem
								) streem
                                    on user_id = CustomDimension004
                                      where (streem.date_time_ga BETWEEN correct_dt_ga-86400*6 AND correct_dt_ga+2)---окно в 6 дней
                                    order by dtr, id, streem.date_time_ga
                                    ) fd
                                    group by id
                        ---пессимизированы органика и директ, выбраны былиже началу каналы
            )pes_first
            on not_pes.id=pes_first.id
    ) t2
left join
(
---пессимизированы органика и директ, выбраны былиже концу каналы
				select id
								,argMax(date_time_ga,src_priority) as dt_ga_pes_last
								,argMax(medium ,src_priority) as medium_pes_last
								,argMax(source,src_priority) as source_pes_last
								,argMax(Campaign,src_priority) as campaign_pes_last
								,argMax(src,src_priority) as src_pes_last
						from
						(
						select correct_dt_ga
								,id
								,reg_name
								,directions_title
								,program_title
								,date_time_ga
								,user_id
								,source
								,medium
								,Campaign
								,CustomDimension002
								,action_name
								,src
								 ,multiIf(src in ('social','cpm','cpc') or (src = 'email' and medium='newsfeed' and source='facebook')  ,2                                           
                                                     ,src in ('smm','модалка','исключаемый источник') or (src = 'email' and medium ='email' and source in ('es','chats'))
                                                        or (src = 'telegram' and medium ='chat' and source in ('telegram')), 1                                             	   
                                            	         ,0) as src_priority
						from
						(
						select toDateTime(parseDateTimeBestEffortOrNull(dt)) as dtr,
								period_type,
								id,
								reg_name,
								country,
								user_id,
								program_title,
								directions_title,
								sm,
								rating_in_section,
								number_transactions,
								rating_in_direction,
								number_transactions_direction,
								start_date,
								first_user_id,
								first_dt,
								is_personal						
								, multiIf((reg_name='Москва'
										or reg_name='Санкт-Петербург'
										or reg_name='Владимир'
										or reg_name='Мытищи'
										or  reg_name='Ростов-на-Дону'
										or reg_name='Севастополь'
										or reg_name='Реутов'
										or reg_name='Тверь'
										or reg_name='Ярославль'
										or reg_name='Киров'
										or  reg_name='Королев'
										or reg_name='Набережные Челны'
										or reg_name='Казань'
										or reg_name='Одинцово'
										or reg_name='Белгород'
										or reg_name='Пятигорск'
										or reg_name='Симферополь'
										or reg_name='Нижний Новгород'
										or reg_name='Тамбов'
										or reg_name='Химки'
										) , addHours(dtr,3),
										reg_name='Одесса' ,  addHours(dtr,2),
										reg_name='Самара' or reg_name='Ижевск' or reg_name='Dubai' ,  addHours(dtr,4),
										(reg_name='Уфа' or reg_name='Екатеринбург' or reg_name='Пермь' or reg_name='Нижний Тагил' or reg_name='Ханты-Мансийск') ,  addHours(dtr,5),
									       reg_name='Алматы' or reg_name='Астана' ,  addHours(dtr,6),
									        (reg_name='Хабаровск' or reg_name='Владивосток' ),  addHours(dtr,10),
									         (reg_name='Новосибирск' or reg_name='Красноярск' or reg_name='Кемерово' or reg_name='Абакан' or reg_name='Томск')  ,  addHours(dtr,7), null) as correct_dt
									   ,addHours(dtr,3) as correct_dt_ga
						 from pg_2021_05_12_final 
						where period_type ='Daily'
						  and toDate(dtr) >= '2021-01-01'
						) r1
						left join
						(
								---click streem
								select distinct date_time_ga
												,source
												,medium
												,Campaign
												,CustomDimension002
												,action_name
												,CustomDimension005 as CustomDimension004
												,multiIf(
											    medium IN ('virtual') ,'модалка' ,
											    source IN ('telegram' ,'event') and medium in ('chat' ,'breakfast'),'telegram' ,
											    medium IN ('cpc','CPC' , 'google_adw') or source LIKE ('%yabs%') and medium IN ('referral') ,'cpc' ,
											    medium IN ('cpm') , 'cpm',
											    medium IN ('twitter','max') ,'partners' ,
											    source LIKE ('amo%') AND medium IN ('email' , 'referral'), 'amo',
											    medium IN ('email',  'e-mail' ,'newsfeed') ,'email' ,											    
											    medium IN ('organic') or ( source IN ('yandex.ru', 'go.mail.ru','mail.ru' ,'yandex.by', 'yandex.ua', 'yandex.kz','yandex.fr' )) and medium IN ('referral') ,'organic' ,
											    medium IN ('sms', 'message' )  ,'sms',
											    medium IN ('referral', '(notset)') and source IN ('ok.ru' , 'm.ok.ru' ,'away.vk.com', 'vk.com', 'm.vk.com', 'youtube.com' ,'pinterest.com',
											    'pinterest.ru','m.youtube.com', 'web.telegram.org', 'web.telegram.org.ru' , 'web.whatsapp.com','l.instagram.com','instagram.com' ,'lm.facebook.com'
											    ,'m.facebook.com')  ,'social',
											    medium IN ('telegrampost','telegram','taplink_swimming','taplink_running','taplink','swipe' ,'social','post','ironstartelegram','ironstar','instapost'
											    			,'instagram_post','instagram','insta_post','direct') and source IN ('smm', 'inst','vk','instagram','fb','ad','facebook') 
											    			OR medium in ('post') ,'smm',
											   source IN ( 'blog') ,'blog',
											    (medium IN ('swimming' ,'running','cycling') and source IN ('instagram', 'telegram'))  ,'link' ,
											    medium like ('%lectorium') or (medium in ('registration') and source IN ('Lectorium'))  ,'lectorium',
											    medium in ('referral') and source IN ('3ds.cloudpayments.ru', 'secure5.arcot.com', '3ds.sdm.ru' ,'acs3.sbrf.ru')  ,'платежные системы',
											     (medium in ('referral') and source like ('%ilovesupersport%')) OR (source in ('n459011.yclients.com'))   ,'переход внутри сайта',
											   (medium in ('referral') and source IN ('taplink.cc'))   ,'исключаемый источник',  											     
											    medium IN ('referral')  ,'referral',
											    medium IN ('(none)') and source IN ('(direct)') ,'direct'											 
											    ,'other') src
								from
								    (
										select parseDateTimeBestEffortOrNull(concat(DateHourandMinute, '00'))+3600 as date_time_ga ---привели к московскому времени
												,splitByChar('/',replace(SourceMedium, ' ', '')) [1] as source
												,splitByChar('/',replace(SourceMedium, ' ', '')) [2] as medium
												,Campaign
												,CustomDimension002
												,Page as action_name
										from ilss_new.click_streem_ga_final
									union all
										select parseDateTimeBestEffortOrNull(concat(DateHourandMinute, '00'))+3600 as date_time_ga  ---привели к московскому времени
												,splitByChar('/',replace(SourceMedium, ' ', '')) [1] as source
												,splitByChar('/',replace(SourceMedium, ' ', '')) [2] as medium
												,Campaign
												,CustomDimension002
												,EventAction as action_name
										from ilss_new.click_streem_ga_action_final 
									)t1
								 join
								       (
									select
										Date
										,CustomDimension002
										,toInt32OrNull(CustomDimension004) as CustomDimension005
										,Users
										from ilss_new.cid_uid_final 
										)t2
									on t1.CustomDimension002=t2.CustomDimension002 --and toDate(date_time_ga)=t2.Date
									where CustomDimension005!=0
								---click streem
								) streem
						on user_id = CustomDimension004
						  where (streem.date_time_ga BETWEEN correct_dt_ga-86400*6 AND correct_dt_ga+2)---окно в 6 дней
						order by dtr, id, streem.date_time_ga desc
						) fd
						group by
								id
---пессимизированы органика и директ, выбраны былиже концу каналы
) pes_last
on t2.id = pes_last.id ''')

logging.info('Вставлен результат в таблицу ClickHouse transaction_source_final')

#Выполняем запрос из ClickHouse + приводим все в строковый формат
a=client.execute('''SELECT id
                                        ,toString(last_date_ga) as last_date_ga
                                        ,toString(last_medium) as last_medium
                                        ,toString(last_source) as last_source
                                        ,toString(last_campaign) as last_campaign
                                        ,toString(last_src) as last_src
                                        ,toString(first_date_ga) as first_date_ga
                                        ,toString(first_medium) as first_medium
                                        ,toString(first_source) as first_source
                                        ,toString(first_campaign) as first_campaign
                                        ,toString(first_src) as first_src
                                        ,toString(dt_ga_pes_first) as dt_ga_pes_first
                                        ,toString(medium_pes_first) as medium_pes_first
                                        ,toString(source_pes_first) as source_pes_first
                                        ,toString(campaign_pes_first) as campaign_pes_first
                                        ,toString(src_pes_first) as src_pes_first
                                        ,toString(dt_ga_pes_last) as dt_ga_pes_last
                                        ,toString(medium_pes_last) as medium_pes_last
                                        ,toString(source_pes_last) as source_pes_last
                                        ,toString(campaign_pes_last) as campaign_pes_last
                                        ,toString(src_pes_last) as src_pes_last 
                        FROM ilss_new.transaction_source_final ''')

logging.info('Выполнен запрос из ClickHouse transaction_source_final ')

#создаем датафрейм на основе списка
my_df = pd.DataFrame(a)

logging.info('Создан датафрейм на основе данных ClickHouse transaction_source_final')

#заводим заголовки
my_df.rename(columns={0: 'id', 1: 'last_date_ga', 2: 'last_medium', 3: 'last_source',4: 'last_campaign',5: 'last_src',
                      6: 'first_date_ga', 7: 'first_medium', 8: 'first_source', 9: 'first_campaign',10: 'first_src',11: 'dt_ga_pes_first',
                      12: 'medium_pes_first', 13: 'source_pes_first', 14: 'campaign_pes_first', 15: 'src_pes_first',16: 'dt_ga_pes_last',17: 'medium_pes_last',
                      18: 'source_pes_last', 19: 'campaign_pes_last', 20: 'src_pes_last'}, inplace=True)

#формируем список
sa = my_df[['id', 'last_date_ga', 'last_medium', 'last_source','last_campaign','last_src','first_date_ga','first_medium','first_source','first_campaign',
               'first_src','dt_ga_pes_first','medium_pes_first','source_pes_first','campaign_pes_first','src_pes_first','dt_ga_pes_last','medium_pes_last',
               'source_pes_last','campaign_pes_last','src_pes_last']]

#сохраняем значения в csv файл
sa.to_csv('transaction_source.csv',sep=';', index=False, header=True)

logging.info('Датафрейм из ClickHouse сохранен в CSV transaction_source')

#создаем таблицу, если отсутствует
query = '''CREATE TABLE if not exists public.transaction_source_final
            (
                id int,
                last_date_ga varchar(800),
                last_medium varchar(800),
                last_source varchar(800),
                last_campaign varchar(800),
                last_src varchar(800),
                first_date_ga  varchar(800),
                first_medium varchar(800),
                first_source varchar(800),
                first_campaign varchar(800),
                first_src varchar(800),
                dt_ga_pes_first varchar(800),
                medium_pes_first varchar(800),
                source_pes_first varchar(800),
                campaign_pes_first varchar(800),
                src_pes_first varchar(800),
                dt_ga_pes_last varchar(800),
                medium_pes_last varchar(800),
                source_pes_last varchar(800),
                campaign_pes_last varchar(800),
                src_pes_last varchar(800)
            ) '''

cursor.execute(query)
cnx.commit()

#Очищаем таблицу от предыдущих данных
truncates = '''TRUNCATE TABLE  transaction_source_final  '''

cursor.execute(truncates)
cnx.commit()

logging.info('Очищена таблица в PG transaction_source_final')


#Вставляем данные из CSV в таблицу Postgress
f = open(r'C:\Users\user\PycharmProjects\sport_ils\public\transaction_source.csv', 'r' , encoding="utf-8")
next(f)
cursor.copy_from(f, 'transaction_source_final', sep=';')
cnx.commit()

logging.info('Данные успешно вставлены из CSV в PG transaction_source')

end = (datetime.now())
logging.info('=====')
logging.info('Script end at: ' + str(end))
logging.info('Time elapsed: ' + str(end - start))

