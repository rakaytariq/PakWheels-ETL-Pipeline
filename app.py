# app.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob

st.set_page_config(page_title="PakWheels Dashboard", layout="wide")
st.title("🚗 PakWheels Live Listings Dashboard")

# Load latest parquet data
def load_parquet_data():
    files = sorted(glob.glob("parquet_output/*.parquet"), reverse=True)
    if not files:
        st.warning("No data available yet.")
        return pd.DataFrame()
    return pd.read_parquet(files[0])

# Load and preprocess
df = load_parquet_data()
if not df.empty:
    df['Brand'] = df['title'].str.split().str[0]
    df['Price (PKR)'] = pd.to_numeric(df['price'].str.replace(r'[^0-9]', '', regex=True), errors='coerce')
    df['Mileage (km)'] = pd.to_numeric(df['mileage'].str.replace(r'[^0-9]', '', regex=True), errors='coerce')

    st.metric("Total Listings", len(df))
    st.metric("Avg. Price", f"{df['Price (PKR)'].mean():,.0f} PKR")
    st.metric("Avg. Mileage", f"{df['Mileage (km)'].mean():,.0f} km")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Brands")
        brand_counts = df['Brand'].value_counts().head(10)
        st.bar_chart(brand_counts)

    with col2:
        st.subheader("Top Cities")
        if 'location' in df.columns:
            city_counts = df['location'].str.split(',').str[0].value_counts().head(10)
            st.bar_chart(city_counts)

    st.subheader("Price Distribution")
    fig, ax = plt.subplots()
    sns.histplot(df['Price (PKR)'].dropna(), bins=30, kde=True, ax=ax)
    st.pyplot(fig)

    st.subheader("Full Listings Table")
    st.dataframe(df)
else:
    st.info("Waiting for data to stream in...")
