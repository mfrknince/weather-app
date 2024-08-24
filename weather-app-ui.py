from weatherApp import WeatherApp

import streamlit as st
import numpy as np

st.title('Weather App')
input_ui = st.container()
show = st.button('Show')


with input_ui:

    city_name = st.text_input('Select your Area')

if show:
    wApp = WeatherApp(city_name)
    wApp.find_coordinates()

    wApp.store_weather_daily_data()
    st.write(city_name)
    wApp.get_weather_daily_data_from_db()

    st.write(wApp.pivot_df)


