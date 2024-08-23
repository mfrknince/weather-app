from weatherApp import WeatherApp

import streamlit as st
import numpy as np

st.title('Weather App')
input_ui = st.container()
show = st.button('Show')


with input_ui:

    city_name = st.text_input('Select your Area')

if show:
    wApp = WeatherApp()
    wApp.city_name = city_name
    wApp.find_coordinates()
    #wApp.get_weather_daily_data()
    #wApp.store_weather_daily_data()
    st.write(wApp.get_data_from_db())
    st.write(city_name)
    st.write(wApp.weather_daily_data)

