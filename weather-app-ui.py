'''
author = @mfrknince on social
'''

from weatherApp import WeatherApp
import plotly.graph_objs as go
import streamlit as st
import time

st.set_page_config(page_title="Weather App", layout="wide")
st.title('Weather App')
input_ui = st.container()
show = st.button('Show')
placeholder = st.empty()

result = st.container()

CityCol, TempCol, HumCol, WindCol = st.columns([3, 3, 2, 2])
with input_ui:
    city_name = st.text_input('Select your Area')

if show:
    if city_name:
        with result:

            wApp = WeatherApp(city_name)

            wApp.find_coordinates()
            with st.spinner('Wait for it...'):
                placeholder.info('Weather data getting from OpenWeatherMap...')
                time.sleep(2)
                placeholder.empty()
                placeholder.info('Weather data uploading to InfluxDB')
                time.sleep(2)
                wApp.store_weather_daily_data()
                placeholder.empty()
                placeholder.success('Data correctly uploaded to InfluxDB')
                time.sleep(3)
                placeholder.empty()
                placeholder.info('Getting data from InfluxDB')
                wApp.get_weather_daily_data_from_db()
                time.sleep(3)
            placeholder.empty()
            placeholder.success("Done!")
            placeholder.empty()

            # current
            with st.container():
                with CityCol:
                    st.title(wApp.city_name)
                    st.caption(str(wApp.current_pivot_df.iloc[0]['time'].strftime('%m/%d/%Y')))

                with TempCol:
                    icon, info = st.columns([1, 3])

                    with icon:
                        icon = str(wApp.current_pivot_df.iloc[0]['icon'])
                        icon = '0' + icon.replace('.0', 'd') + '.png'
                        st.image('icons/' + icon)

                    with info:
                        st.caption('Temperature')
                        st.subheader(str((round(wApp.current_pivot_df.iloc[0]['temp']))) + str('°C'))
                        st.caption(str((round(wApp.current_pivot_df.iloc[0]['feels_like']))) + str('°C Feels'))

                with HumCol:
                    st.caption('Humidity')
                    st.subheader(str(wApp.current_pivot_df.iloc[0]['humidity']) + str(' %'))

                with WindCol:
                    st.caption('Wind Speed')
                    st.subheader(str(wApp.current_pivot_df.iloc[0]['wind_speed']) + str(' km/h'))

        with st.spinner():
            # forecast
            columns = st.columns(5)

            for i, col in enumerate(columns):
                with col:
                    with st.container(border=True):
                        st.subheader(str(wApp.weekly_pivot_df.iloc[i + 1]['time'].strftime('%m/%d/%Y')))
                        st.write(str(round(wApp.weekly_pivot_df.iloc[i + 1]['max'])) + str('°C') + "  -  " + str(
                            round(wApp.weekly_pivot_df.iloc[i + 1]['min'])) + str('°C'))

                        col1, iconCol, col2 = st.columns([1, 2, 1])
                        with iconCol:
                            icon = str(wApp.weekly_pivot_df.iloc[i + 1]['icon'])
                            icon = '0' + icon.replace('.0', 'd') + '.png'
                            st.image('icons/' + icon, width=75)

                        hum, wind = st.columns(2)

                        with hum:
                            st.caption('Humidity')
                            st.write(str(wApp.weekly_pivot_df.iloc[i + 1]['humidity']) + str(' %'))

                        with wind:
                            st.caption('Wind Speed')
                            st.write(str(wApp.weekly_pivot_df.iloc[i + 1]['wind_speed']) + str(' km/h'))

            fig = go.Figure()

            fig.add_trace(
                go.Scatter(x=wApp.weekly_pivot_df['time'], y=wApp.weekly_pivot_df['max'], mode='lines',
                           name='Max Temperature',
                           line=dict(color='red')))
            fig.add_trace(
                go.Scatter(x=wApp.weekly_pivot_df['time'], y=wApp.weekly_pivot_df['min'], mode='lines',
                           name='Min Temperature',
                           line=dict(color='blue')))
            fig.add_trace(
                go.Scatter(x=wApp.weekly_pivot_df['time'], y=wApp.weekly_pivot_df['day'], mode='lines',
                           name='Day Temperature',
                           line=dict(color='green')))

            fig.update_layout(title='Daily Temperatures',
                              xaxis_title='Date',
                              yaxis_title='Temperature')

            st.plotly_chart(fig)
    else:
        st.error('Please select your area')

with st.expander('developed by M. Furkan Ince'):
    st.markdown('This project is open to all kinds of improvements')
    st.markdown('if you would like to contact: mfrknince@gmail.com or :gray-background[@mfrknince] on social ')
