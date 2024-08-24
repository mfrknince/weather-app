import requests
import time, json
from influxdb_client import InfluxDBClient, Point, WriteOptions
import pandas as pd
#!pip install tabulate

class WeatherApp:
    def __init__(self,city_name):
        self.weather_key = "2d300cd42548efadf84eb44c352f98cf"
        self.weather_url = "http://api.openweathermap.org"
        self.db_token = "fBAUCTCV-fhFJP-5O-2OX7wCjuLdGXAL2XiLsHqD2wj1ZFkS-_uWXC9oqTE2PK6-UpAv74uennRGTJDEBKYWqA=="
        self.host = "https://eu-central-1-1.aws.cloud2.influxdata.com"
        self.db_org = "Team"
        self.db_name = "Test"
        self.city_name = city_name
        self.city_lat = 0
        self.city_lon = 0
        self.client = self.create_client()
        self.weather_daily_data = self.get_weather_daily_data()
        self.pivot_df = pd.DataFrame()


    def get_weather_daily_data_from_db(self):
        return self.get_data_from_db()



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

        print('>>>>>>>>>>>>>>>>>>>>>')
        print(self.city_name)
        print('>>>>>>>>>>>>>>>>>>>>>')
        df['city'] = self.city_name
        df['datetime'] = pd.to_datetime(df['dt'], unit='s')
        df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')

        return df

    def create_client(self):

        token = self.db_token
        org = self.db_org
        host = self.host

        client = InfluxDBClient(url=host, token=token, org=org)

        return client

    def modify_daily_data(self):

        data_json = self.weather_daily_data.to_json(orient='records')

        data_dict = json.loads(data_json)

        data = {
            "city": [],
            "day": [],
            "humidity": [],
            "max": [],
            "min": [],
            "time": [],
            "wind_speed": []
        }

        for entry in data_dict:
            data["city"].append(entry['city'])
            data["day"].append(entry['temp']['day'])
            data["humidity"].append(entry['humidity'])
            data["max"].append(entry['temp']['max'])
            data["min"].append(entry['temp']['min'])
            data["time"].append(f"{entry['datetime']}T10:00:00Z")  #
            data["wind_speed"].append(entry['wind_speed'])

        self.weather_daily_data = data
        print(data)


    def store_weather_daily_data(self):


        self.modify_daily_data()

        write_api = self.client.write_api(write_options=WriteOptions(batch_size=1_000, flush_interval=10_000))

        df = pd.DataFrame(self.weather_daily_data)

        for index, row in df.iterrows():
            point = Point("weather_data") \
                .tag("city", row["city"]) \
                .field("day", row["day"]) \
                .field("humidity", row["humidity"]) \
                .field("max", row["max"]) \
                .field("min", row["min"]) \
                .field("wind_speed", row["wind_speed"]) \
                .time(row["time"])
            write_api.write(bucket=self.db_name, org=self.db_org, record=point)


        print("Complete. Return to the InfluxDB UI.")

    def get_data_from_db(self):

        query_api = self.client.query_api()

        query = """
        from(bucket: "Test")
          |> range(start: 2024-08-01T00:00:00Z, stop: 2024-08-31T23:59:59Z)
          |> filter(fn: (r) => r["_measurement"] == "weather_data")
        """

        try:
            result = query_api.query(org=self.db_org, query=query)

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

            df['time'] = pd.to_datetime(df['time'])

            pivot_df = df.pivot_table(index='time', columns='field', values='value', aggfunc='mean').reset_index()

            print(pivot_df)

            self.pivot_df = pivot_df

        except Exception as e:
            print(f"Bir hata oluştu: {e}")



