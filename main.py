import datetime
import yaml
import calendar

from custom_package.db import Database

config_file = 'config.yml'


def read_sql(sql_path):
    """Getting sql from a file"""
    with open(sql_path, mode='r', encoding='utf-8-sig') as sql_file:
        sql = sql_file.read()
    return sql


def cyclones_monthly_upload(year=None, month=None):
    """Upload data for the specified month and year. For each day in the view file cyclones_20140128.csv."""
    import calendar
    import datetime
    year = int(input('Input year: ')) if year is None else year
    month = int(input('Input month: ')) if month is None else month
    days = calendar.monthrange(year, month)[1]
    for day in range(1, days + 1):
        cyclones_in_file = read_sql('sql/cyclones_in_file.sql')
        date = datetime.date(year, month, day)
        cyclones_in_file = cyclones_in_file.format(date=date.strftime('%Y-%m-%d'), copy_params='WITH CSV')
        db.copy_expert(cyclones_in_file, f'/tmp/cyclones_{date.strftime("%Y%m%d")}.csv')


def copy_cyclones_history(date_load):
    """Loading cyclone history"""
    import datetime
    # Загрузка в Redshift
    cyclones_into_db = read_sql('sql/cyclones_into_db.sql')
    cyclones_into_db = cyclones_into_db.format(delimiter=',')
    db.exec('Truncate table tmp.cyclones_tmp;')
    db.copy_expert(cyclones_into_db, f'/tmp/cyclones_{date_load.strftime("%Y%m%d")}.csv')
    # Обновление целевой таблицы
    before_date_load = date_load - datetime.timedelta(days=1)
    processing_data = read_sql('sql/processing_data.sql')
    processing_data = processing_data.format(date_load=date_load, before_date_load=before_date_load)
    db.exec(processing_data)


with open(config_file) as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
    f.close()
db = Database(**config['connection_properties'])

# Выполнение п.3
date_run = datetime.date(2013, 1, 1)
while date_run <= datetime.date.today():
    cyclones_monthly_upload(date_run.year, date_run.month)
    days_in_month = calendar.monthrange(date_run.year, date_run.month)[1]
    date_run = date_run + datetime.timedelta(days=days_in_month)

# Выполнение п.5
date_run = datetime.date(2013, 1, 1)
while date_run <= datetime.date.today():
    copy_cyclones_history(date_run)
    date_run = date_run + datetime.timedelta(days=1)

