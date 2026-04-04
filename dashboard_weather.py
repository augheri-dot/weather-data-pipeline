import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import plotly.express as px

load_dotenv()

st.set_page_config(
    page_title="Multi-City Weather Dashboard",
    layout="wide"
)

# ---------- Custom Styling ----------
st.markdown("""
<style>
    .block-container {
        padding-top: 1.8rem;
        padding-bottom: 2rem;
        padding-left: 2.5rem;
        padding-right: 2.5rem;
    }
    .hero-title {
        font-size: 44px;
        font-weight: 800;
        margin-bottom: 0.2rem;
        color: #1f2937;
    }
    .hero-subtitle {
        font-size: 16px;
        color: #6b7280;
        margin-bottom: 1.4rem;
    }
    .section-title {
        font-size: 24px;
        font-weight: 700;
        margin-top: 0.8rem;
        margin-bottom: 0.8rem;
        color: #1f2937;
    }
    .info-card {
        background-color: #f8fafc;
        border: 1px solid #e5e7eb;
        padding: 14px 18px;
        border-radius: 14px;
        margin-bottom: 1rem;
    }
    .small-muted {
        color: #6b7280;
        font-size: 14px;
    }
</style>
""", unsafe_allow_html=True)

# ---------- Database ----------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT"),
        sslmode="require"
    )

@st.cache_data
def load_data():
    conn = get_connection()
    query = """
        SELECT city, time, temperature, humidity
        FROM weather_raw
        ORDER BY city, time
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df["time"] = pd.to_datetime(df["time"])
    return df

df = load_data()

# ---------- Header ----------
st.markdown('<div class="hero-title">🌦️ Multi-City Weather Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="hero-subtitle">Interactive weather monitoring dashboard powered by PostgreSQL, Python ETL, and Streamlit.</div>',
    unsafe_allow_html=True
)

if df.empty:
    st.warning("No data is available in the weather_raw table yet.")
    st.stop()

# ---------- Sidebar ----------
st.sidebar.header("Dashboard Filters")

city_list = sorted(df["city"].dropna().unique().tolist())
selected_city = st.sidebar.selectbox("Select City", city_list)

city_df = df[df["city"] == selected_city].copy()

min_date = city_df["time"].dt.date.min()
max_date = city_df["time"].dt.date.max()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = min_date
    end_date = max_date

filtered_df = city_df[
    (city_df["time"].dt.date >= start_date) &
    (city_df["time"].dt.date <= end_date)
].copy()

if filtered_df.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

# ---------- Hero Info ----------
last_updated = filtered_df["time"].max().strftime("%Y-%m-%d %H:%M:%S")
st.markdown(
    f"""
    <div class="info-card">
        <b>Selected City:</b> {selected_city}<br>
        <span class="small-muted">Date range: {start_date} to {end_date} | Last record: {last_updated}</span>
    </div>
    """,
    unsafe_allow_html=True
)

# ---------- KPI Section ----------
avg_temp = round(filtered_df["temperature"].mean(), 2)
avg_humidity = round(filtered_df["humidity"].mean(), 2)
max_temp = round(filtered_df["temperature"].max(), 2)
min_temp = round(filtered_df["temperature"].min(), 2)
max_humidity = round(filtered_df["humidity"].max(), 2)

st.markdown('<div class="section-title">📌 Key Metrics</div>', unsafe_allow_html=True)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Records", len(filtered_df))
k2.metric("Avg Temp", f"{avg_temp} °C")
k3.metric("Avg Humidity", f"{avg_humidity} %")
k4.metric("Max Temp", f"{max_temp} °C")
k5.metric("Max Humidity", f"{max_humidity} %")

st.markdown("---")

# ---------- City Comparison ----------
st.markdown('<div class="section-title">🏙️ City Comparison</div>', unsafe_allow_html=True)

comparison_df = (
    df.groupby("city", as_index=False)
      .agg(
          avg_temperature=("temperature", "mean"),
          avg_humidity=("humidity", "mean"),
          records=("city", "count")
      )
)

comp_col1, comp_col2 = st.columns(2)

with comp_col1:
    fig_city_temp = px.bar(
        comparison_df.sort_values("avg_temperature", ascending=False),
        x="city",
        y="avg_temperature",
        title="Average Temperature by City",
        text_auto=".2f"
    )
    fig_city_temp.update_layout(
        xaxis_title="City",
        yaxis_title="Temperature (°C)",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    st.plotly_chart(fig_city_temp, use_container_width=True)

with comp_col2:
    fig_city_hum = px.bar(
        comparison_df.sort_values("avg_humidity", ascending=False),
        x="city",
        y="avg_humidity",
        title="Average Humidity by City",
        text_auto=".2f"
    )
    fig_city_hum.update_layout(
        xaxis_title="City",
        yaxis_title="Humidity (%)",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    st.plotly_chart(fig_city_hum, use_container_width=True)

st.markdown("---")

# ---------- Trend Charts ----------
st.markdown('<div class="section-title">📈 Weather Trends</div>', unsafe_allow_html=True)

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    fig_temp = px.line(
        filtered_df,
        x="time",
        y="temperature",
        title=f"Temperature Trend - {selected_city}",
        markers=True
    )
    fig_temp.update_layout(
        xaxis_title="Time",
        yaxis_title="Temperature (°C)",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    st.plotly_chart(fig_temp, use_container_width=True)

with chart_col2:
    fig_hum = px.line(
        filtered_df,
        x="time",
        y="humidity",
        title=f"Humidity Trend - {selected_city}",
        markers=True
    )
    fig_hum.update_layout(
        xaxis_title="Time",
        yaxis_title="Humidity (%)",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    st.plotly_chart(fig_hum, use_container_width=True)

st.markdown("---")

# ---------- Summary Statistics ----------
st.markdown('<div class="section-title">📊 Summary Statistics</div>', unsafe_allow_html=True)

summary_df = pd.DataFrame({
    "Metric": [
        "Average Temperature",
        "Maximum Temperature",
        "Minimum Temperature",
        "Average Humidity",
        "Maximum Humidity",
        "Minimum Humidity"
    ],
    "Value": [
        f"{filtered_df['temperature'].mean():.2f} °C",
        f"{filtered_df['temperature'].max():.2f} °C",
        f"{filtered_df['temperature'].min():.2f} °C",
        f"{filtered_df['humidity'].mean():.2f} %",
        f"{filtered_df['humidity'].max():.2f} %",
        f"{filtered_df['humidity'].min():.2f} %"
    ]
})

st.dataframe(summary_df, use_container_width=True, hide_index=True)

st.markdown("---")

# ---------- Detailed Data ----------
st.markdown('<div class="section-title">🗂️ Detailed Weather Data</div>', unsafe_allow_html=True)

display_df = filtered_df.copy()
display_df["time"] = display_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")

st.dataframe(display_df, use_container_width=True, hide_index=True)

st.download_button(
    "⬇️ Download Filtered CSV",
    filtered_df.to_csv(index=False),
    file_name=f"weather_{selected_city.lower()}.csv",
    mime="text/csv"
)