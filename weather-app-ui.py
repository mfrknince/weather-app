'''
author = @mfrknince on social
'''

from weatherApp import WeatherApp
import pandas as pd
import streamlit as st
import time

st.title('Weather App')
input_ui = st.container()
show = st.button('Show')
placeholder = st.empty()

with input_ui:

    city_name = st.text_input('Select your Area')

if show:
    wApp = WeatherApp(city_name)

    wApp.find_coordinates()
    with st.spinner('Wait for it...'):

        wApp.store_weather_daily_data()
        placeholder.success('Data correctly uploaded to InfluxDB')

        st.write(city_name)
        placeholder.empty()
        placeholder.info('Getting data from InfluxDB')
        wApp.get_weather_daily_data_from_db()
        time.sleep(3)
    placeholder.empty()
    placeholder.success("Done!")
    placeholder.empty()

    st.write('Current')
    st.write(wApp.current_pivot_df)
    st.write('Forecast')
    st.write(wApp.weekly_pivot_df)


