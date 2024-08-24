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
        self.point_name ="weather_data"
        self.client = self.create_client()
        self.weekly_pivot_df,self.current_pivot_df =pd.DataFrame(),pd.DataFrame()
        #self.weather_daily_data = self.get_weather_daily_data()


    def get_current_weather_data(self):
        response = requests.get(self)

    def get_weather_daily_data_from_db(self):
        return self.get_data_from_db()



    def find_coordinates(self):

        print('\n'*5)
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={self.city_name}&appid={self.weather_key}'


        res = requests.get(url)
        data = res.json()


        self.city_name = data[0]['name']
        self.city_lat = data[0]['lat']
        self.city_lon = data[0]['lon']


    def get_weather_daily_data(self):
        df_current = None

        url = f'https://api.openweathermap.org/data/3.0/onecall?lat={self.city_lat}&lon={self.city_lon}&units=metric&appid={self.weather_key}'

        res = requests.get(url)
        data = res.json()


        df_current = pd.DataFrame(data['current'])
        df_daily = pd.DataFrame(data['daily'])

        df_current = df_current[['dt', 'temp', 'wind_speed', 'humidity']]
        df_daily = df_daily[['dt', 'temp', 'wind_speed', 'humidity']]

        #print('>>>>>>>>>>>>>>>>>>>>>')
        # print(self.city_name)
        # print('>>>>>>>>>>>>>>>>>>>>>')
        df_daily['city'] = self.city_name
        df_current['city'] = self.city_name

        df_current['datetime'] = pd.to_datetime(df_current['dt'], unit='s')
        df_current['datetime'] = df_current['datetime'].dt.strftime('%Y-%m-%d')

        df_daily['datetime'] = pd.to_datetime(df_daily['dt'], unit='s')
        df_daily['datetime'] = df_daily['datetime'].dt.strftime('%Y-%m-%d')

        return df_current,df_daily

    def create_client(self):

        token = self.db_token
        org = self.db_org
        host = self.host

        client = InfluxDBClient(url=host, token=token, org=org)

        return client

    def modify_daily_data(self):

        #TODO more clean code

        self.current_data,self.weather_daily_data= self.get_weather_daily_data()

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
            "wind_speed": []
        }

        for entry in weekly_data_dict:
            data["city"].append(entry['city'].split(' ')[0])
            data["day"].append(entry['temp']['day'])
            data["humidity"].append(entry['humidity'])
            data["max"].append(entry['temp']['max'])
            data["min"].append(entry['temp']['min'])
            data["time"].append(entry['datetime'])
            data["wind_speed"].append(entry['wind_speed'])

        self.weather_daily_data = data
        self.weather_daily_data_df = pd.DataFrame(data=self.weather_daily_data)

        data2 = {
            "city": [],
            "temp": [],
            "humidity": [],
            "time": [],
            "wind_speed": []
        }

        print(current_data_dict)

        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

        for entry in current_data_dict:
            data2["city"].append(entry['city'].split(' ')[0])
            data2["temp"].append(entry['temp'])
            data2["humidity"].append(entry['humidity'])
            data2["time"].append(entry['datetime'])
            data2["wind_speed"].append(entry['wind_speed'])

        self.weather_current_data = data2
        self.weather_current_data_df = pd.DataFrame(data=self.weather_current_data)

        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

        print(self.weather_current_data_df.info())




        return self.weather_current_data_df,self.weather_daily_data_df


    def store_weather_daily_data(self):


        current_df,weekly_df = self.modify_daily_data()

        print(current_df)

        #print(df)

        write_api = self.client.write_api(write_options=WriteOptions(batch_size=10, flush_interval=10))


        for index, row in weekly_df.iterrows():
            point = Point(self.point_name) \
                .tag("city", row["city"]) \
                .field("day", row["day"]) \
                .field("humidity", row["humidity"]) \
                .field("max", row["max"]) \
                .field("min", row["min"]) \
                .field("wind_speed", row["wind_speed"]) \
                .time(row["time"])
            print(point)
            write_api.write(bucket=self.db_name, org=self.db_org, record=point)

        time.sleep(10)

        for index, row in current_df.iterrows():
            point = Point(self.point_name+'_current') \
                .tag("city", row["city"]) \
                .field("temp", float(row["temp"])) \
                .field("humidity", row["humidity"]) \
                .field("wind_speed", row["wind_speed"]) \
                .time(row["time"])
            print(point)
            write_api.write(bucket=self.db_name, org=self.db_org, record=point)



        print("Complete. Return to the InfluxDB UI.")

    def get_data_from_db(self):

        query_api = self.client.query_api()

        query = f"""
        from(bucket: "{self.db_name}")
          |> range(start: 2024-08-01T00:00:00Z, stop: 2024-08-31T23:59:59Z)
          |> filter(fn: (r) => r["_measurement"] == "{self.point_name}")
           |> filter(fn: (r) => r["city"] == "{self.city_name.split(' ')[0]}")
        """

        query_curr = f"""
                from(bucket: "{self.db_name}")
                  |> range(start: -2d)
                  |> filter(fn: (r) => r["_measurement"] == "{self.point_name+'_current'}")
                   |> filter(fn: (r) => r["city"] == "{self.city_name.split(' ')[0]}")
                """

        print(query)

        print('_'*20)

        print(query_curr)



        result = query_api.query(org=self.db_org, query=query)


        #weekly
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



        #df['time'] = pd.to_datetime(df['time'])
        #df["temp"] = df["temp"].astype(float)

        pivot_df = df.pivot_table(index='time', columns='field', values='value', aggfunc='mean').reset_index()

        print(pivot_df)

        result_current = query_api.query(org=self.db_org, query=query_curr)
        print(result_current)

        while len(result_current)<1:
            result_current = query_api.query(org=self.db_org, query=query_curr)


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

            # DataFrame'e verileri aktaralım
            df_curr = pd.DataFrame(records_curr)

            print('_' * 20)
            print(df_curr)

            # Eğer DataFrame boş değilse pivot_table işlemini gerçekleştirelim
            if not df_curr.empty:
                df_curr['time'] = pd.to_datetime(df_curr['time'])  # Zamanı datetime formatına çevir
                result_current = df_curr.pivot_table(index='time', columns='field', values='value',
                                                     aggfunc='mean').reset_index()
                print(result_current)
            else:
                print("Veri yok veya boş veri döndü.")
        else:
            print("result_current değişkeni boş!")

        self.weekly_pivot_df = pivot_df
        self.current_pivot_df = result_current



