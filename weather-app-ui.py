import streamlit as st
import numpy as np

st.title('Weather App')
input_ui = st.container()
show = st.button('Show')


with input_ui:
    number_of_person = st.text_input('Select your Area')

