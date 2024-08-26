'''
author = @mfrknince on social
'''

import requests
import time, json

import streamlit
from influxdb_client import InfluxDBClient, Point, WriteOptions
import pandas as pd


# pip install tabulate

# pip install -r requirements.txt

#TODO MORE CLEAN CODE

class WeatherApp:
    def __init__(self, city_name):
        self.weather_key = "2d300cd42548efadf84eb44c352f98cf"
        self.weather_url = "http://api.openweathermap.org"
        self.db_token = "fBAUCTCV-fhFJP-5O-2OX7wCjuLdGXAL2XiLsHqD2wj1ZFkS-_uWXC9oqTE2PK6-UpAv74uennRGTJDEBKYWqA=="
        self.host = "https://eu-central-1-1.aws.cloud2.influxdata.com"
        self.db_org = "Team"
        self.db_name = "Weather"
        self.city_name = city_name
        self.city_lat = 0
        self.city_lon = 0
        self.point_name = "weather_data"
        self.client = self.create_client()
        self.weekly_pivot_df, self.current_pivot_df = pd.DataFrame(), pd.DataFrame()

    def get_weather_daily_data_from_db(self):
        return self.get_data_from_db()

    def find_coordinates(self):

        print('Welcome to Weather App')
        print('\n' * 5)
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={self.city_name}&appid={self.weather_key}'

        res = requests.get(url)
        data = res.json()

        self.city_name = data[0]['name']
        self.city_lat = data[0]['lat']
        self.city_lon = data[0]['lon']

    def get_weather_daily_data(self):
        df_current = None
        df_daily = None

        url = f'https://api.openweathermap.org/data/3.0/onecall?lat={self.city_lat}&lon={self.city_lon}&units=metric&appid={self.weather_key}'

        res = requests.get(url)
        data = res.json()

        if isinstance(data['current'], dict):
            df_current = pd.DataFrame([data['current']])
        elif isinstance(data['current'], list):
            df_current = pd.DataFrame(data['current'])
        else:
            raise ValueError("data['current'] should be a list or a dict")

        if isinstance(data['daily'], dict):
            df_daily = pd.DataFrame([data['daily']])
        elif isinstance(data['daily'], list):
            df_daily = pd.DataFrame(data['daily'])
        else:
            raise ValueError("data['daily'] should be a list or a dict")

        df_current['icon'] = df_current.iloc[0]['weather'][0]['icon'][:-1]
        df_daily['icon'] = df_daily['weather'].apply(lambda x: x[0]['icon'][:-1]).to_frame(name='icon')

        df_current = df_current[['dt', 'temp', 'wind_speed', 'humidity', 'feels_like', 'icon']]
        df_daily = df_daily[['dt', 'temp', 'wind_speed', 'humidity', 'icon']]

        df_daily['city'] = self.city_name
        df_current['city'] = self.city_name

        df_current['datetime'] = pd.to_datetime(df_current['dt'], unit='s')
        df_current['datetime'] = df_current['datetime'].dt.strftime('%Y-%m-%d')

        df_daily['datetime'] = pd.to_datetime(df_daily['dt'], unit='s')
        df_daily['datetime'] = df_daily['datetime'].dt.strftime('%Y-%m-%d')

        df_current['icon'] = df_daily['icon'].astype(float)
        df_daily['icon'] = df_daily['icon'].astype(float)

        return df_current, df_daily

    def analyse_weather_data(self, df_current, df_daily):

        print('Current data has ' + str(df_current.isna().sum().sum()) + ' NaN values')
        print('Current data has ' + str(df_daily.isna().sum().sum()) + ' NaN values')

        if df_current.isna().sum().sum() > 0:
            streamlit.error('Please check your current weather data')

        if df_daily.isna().sum().sum() > 0:
            df_daily.fillna(df_daily.mean(), inplace=True)

        print('Here your current df info')
        print(df_current.info())
        print(df_current.describe())

        print('\n' * 3)

        print('Here your daily df info')
        print(df_daily.info())
        print(df_daily.describe())

    def create_client(self):

        token = self.db_token
        org = self.db_org
        host = self.host

        client = InfluxDBClient(url=host, token=token, org=org)

        return client

    def modify_daily_data(self):

        self.current_data, self.weather_daily_data = self.get_weather_daily_data()

        current_data_json = self.current_data.to_json(orient='records')
        weekly_data_json = self.weather_daily_data.to_json(orient='records')

        weekly_data_dict = json.loads(weekly_data_json)
        current_data_dict = json.loads(current_data_json)

        data = {
            "city": [],
            "day": [],
            "humidity": [],
            "max": [],
            "min": [],
            "time": [],
            "wind_speed": [],
            "icon": []
        }

        for entry in weekly_data_dict:
            data["city"].append(entry['city'].split(' ')[0])
            data["day"].append(entry['temp']['day'])
            data["humidity"].append(entry['humidity'])
            data["max"].append(entry['temp']['max'])
            data["min"].append(entry['temp']['min'])
            data["time"].append(entry['datetime'])
            data["wind_speed"].append(entry['wind_speed'])
            data["icon"].append(entry['icon'])

        self.weather_daily_data = data
        self.weather_daily_data_df = pd.DataFrame(data=self.weather_daily_data)

        self.weather_daily_data_df['day'] = self.weather_daily_data_df['day'].clip(lower=-100, upper=60)
        self.weather_daily_data_df['max'] = self.weather_daily_data_df['max'].clip(lower=-100, upper=60)
        self.weather_daily_data_df['min'] = self.weather_daily_data_df['min'].clip(lower=-100, upper=60)
        self.weather_daily_data_df['humidity'] = self.weather_daily_data_df['humidity'].clip(lower=0, upper=100)
        self.weather_daily_data_df['wind_speed'] = self.weather_daily_data_df['wind_speed'].clip(lower=0, upper=200)

        data2 = {
            "city": [],
            "temp": [],
            "feels_like": [],
            "humidity": [],
            "time": [],
            "wind_speed": [],
            "icon": []
        }

        for entry in current_data_dict:
            data2["city"].append(entry['city'].split(' ')[0])
            data2["temp"].append(entry['temp'])
            data2["feels_like"].append(entry['feels_like'])
            data2["humidity"].append(entry['humidity'])
            data2["time"].append(entry['datetime'])
            data2["wind_speed"].append(entry['wind_speed'])
            data2["icon"].append(entry['icon'])

        self.weather_current_data = data2
        self.weather_current_data_df = pd.DataFrame(data=self.weather_current_data)

        self.weather_current_data_df['temp'] = self.weather_current_data_df['temp'].clip(lower=-100, upper=60)
        self.weather_current_data_df['feels_like'] = self.weather_current_data_df['feels_like'].clip(lower=-100,
                                                                                                     upper=60)
        self.weather_current_data_df['humidity'] = self.weather_current_data_df['humidity'].clip(lower=0, upper=100)
        self.weather_current_data_df['wind_speed'] = self.weather_current_data_df['wind_speed'].clip(lower=0, upper=200)

        self.analyse_weather_data(self.weather_current_data_df, self.weather_daily_data_df)

        return self.weather_current_data_df, self.weather_daily_data_df

    def store_weather_daily_data(self):

        current_df, weekly_df = self.modify_daily_data()

        write_api = self.client.write_api(write_options=WriteOptions(batch_size=10, flush_interval=10))

        for index, row in weekly_df.iterrows():
            point = Point(self.point_name) \
                .tag("city", row["city"]) \
                .field("day", row["day"]) \
                .field("humidity", row["humidity"]) \
                .field("max", row["max"]) \
                .field("min", row["min"]) \
                .field("wind_speed", row["wind_speed"]) \
                .field("icon", row["icon"]) \
                .time(row["time"])
            print(point)
            write_api.write(bucket=self.db_name, org=self.db_org, record=point)

        time.sleep(10)

        for index, row in current_df.iterrows():
            point = Point(self.point_name + '_current') \
                .tag("city", row["city"]) \
                .field("temp", float(row["temp"])) \
                .field("feels_like", float(row["feels_like"])) \
                .field("humidity", row["humidity"]) \
                .field("wind_speed", row["wind_speed"]) \
                .field("icon", row["icon"]) \
                .time(row["time"])
            print(point)
            write_api.write(bucket=self.db_name, org=self.db_org, record=point)

        print("Complete. Return to the InfluxDB UI.")

    def query_with_retries(self, query_api, query, max_retries=5, wait_time=2):
        retries = 0
        result_current = None

        while retries < max_retries:
            result_current = query_api.query(org=self.db_org, query=query)

            if len(result_current) > 0:
                return result_current

            retries += 1
            print(f"No results found. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

        raise Exception("Max retries exceeded. No results found.")

    def get_data_from_db(self):

        query_api = self.client.query_api()

        query = f"""
        from(bucket: "{self.db_name}")
          |> range(start: -1d, stop: 6d)
          |> filter(fn: (r) => r["_measurement"] == "{self.point_name}")
           |> filter(fn: (r) => r["city"] == "{self.city_name.split(' ')[0]}")
        """

        result = self.query_with_retries(query_api, query)

        records = []
        for table in result:
            for record in table.records:
                records.append({
                    "time": record.get_time(),
                    "measurement": record.get_measurement(),
                    "field": record.get_field(),
                    "value": record.get_value()
                })

        df = pd.DataFrame(records)

        pivot_df = df.pivot_table(index='time', columns='field', values='value', aggfunc='mean').reset_index()

        query_curr = f"""
                        from(bucket: "{self.db_name}")
                          |> range(start: -24h)
                          |> filter(fn: (r) => r["_measurement"] == "{self.point_name + '_current'}")
                           |> filter(fn: (r) => r["city"] == "{self.city_name.split(' ')[0]}")
                        """

        result_current = self.query_with_retries(query_api, query_curr)

        if result_current:
            records_curr = []

            for table in result_current:
                for record_curr in table.records:

                    time_val = record_curr.get_time()
                    measurement_val = record_curr.get_measurement()
                    field_val = record_curr.get_field()
                    value_val = record_curr.get_value()

                    if time_val and field_val is not None and value_val is not None:
                        records_curr.append({
                            "time": time_val,
                            "measurement": measurement_val,
                            "field": field_val,
                            "value": value_val
                        })

            df_curr = pd.DataFrame(records_curr)

            if not df_curr.empty:
                df_curr['time'] = pd.to_datetime(df_curr['time'])
                result_current = df_curr.pivot_table(index='time', columns='field', values='value',
                                                     aggfunc='mean').reset_index()
                print(result_current)
            else:
                print("No data")
        else:
            print("result_current is None")

        self.weekly_pivot_df = pivot_df
        self.current_pivot_df = result_current
