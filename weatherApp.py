import requests
import time, json
from influxdb_client_3 import InfluxDBClient3, Point, WritePrecision
from influxdb_client import InfluxDBClient
import pandas as pd
import tabulate

class WeatherApp:
    def __init__(self):
        self.weather_key = "2d300cd42548efadf84eb44c352f98cf"
        self.weather_url = "http://api.openweathermap.org"
        self.db_token = "fBAUCTCV-fhFJP-5O-2OX7wCjuLdGXAL2XiLsHqD2wj1ZFkS-_uWXC9oqTE2PK6-UpAv74uennRGTJDEBKYWqA=="
        self.host = "https://eu-central-1-1.aws.cloud2.influxdata.com"
        self.db_org = "Team"
        self.db_name = "Test"
        self.city_name = ""
        self.city_lat = 0
        self.city_lon = 0
        self.client = self.create_client()
        self.weather_daily_data = self.get_weather_daily_data()


    def find_coordinates(self):
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={self.city_name}&appid={self.weather_key}'


        res = requests.get(url)
        data = res.json()


        self.city_name = data[0]['name']
        self.city_lat = data[0]['lat']
        self.city_lon = data[0]['lon']


    def get_weather_daily_data(self):
        url = f'https://api.openweathermap.org/data/3.0/onecall?lat={self.city_lat}&lon={self.city_lon}&units=metric&appid={self.weather_key}'

        res = requests.get(url)
        data = res.json()

        df = pd.DataFrame(data['daily'])

        df = df[['dt', 'temp', 'wind_speed', 'humidity']]

        return df

    def create_client(self):

        token = self.db_token
        org = self.db_org
        host = self.host

        self.client = InfluxDBClient3(host=host, token=token, org=org)

    def delete_unnecessary_data(self):

        data_json = self.weather_daily_data.to_json(orient='records')

        data_dict = json.loads(data_json)

        for entry in data_dict:
            temp = entry['temp']
            if 'night' in temp:
                del temp['night']
            if 'eve' in temp:
                del temp['eve']
            if 'morn' in temp:
                del temp['morn']

        self.weather_daily_data = data_dict


    def store_weather_daily_data(self):


        database = self.db_name

        self.delete_unnecessary_data()

        for entry in self.weather_daily_data:
            point = Point("weather") \
                .tag("city", entry["city"]) \
                .time("_time", entry["datetime"]) \
                .field("day", entry["temp"]["day"]) \
                .field("min", entry["temp"]["min"]) \
                .field("max", entry["temp"]["max"]) \
                .field("wind_speed", entry["wind_speed"]) \
                .field("humidity", entry["humidity"]) \
                .time(pd.to_datetime(entry["dt"], unit='s'), WritePrecision.S)

            self.client.write(database=database, record=point)
            time.sleep(1)


        print("Complete. Return to the InfluxDB UI.")

    def get_data_from_db(self):


        client = InfluxDBClient(url=self.host, token=self.db_token, org=self.db_org)

        query = '''
        from(bucket: "Test")
          |> range(start: -1d)  // Adjust the time range as needed
          |> filter(fn: (r) => r["_measurement"] == "weather")
        '''

        # Execute the query
        tables = client.query_api().query(query=query, org=self.db_org)

        # Convert to DataFrame
        df = pd.DataFrame([record.values for table in tables for record in table.records])

        return df

