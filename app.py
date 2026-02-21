#imports
import streamlit as st
import polars as pl
import duckdb
import requests
import plotly.express as px
from pathlib import Path


st.set_page_config(
    page_title='NYC Yellow Taxi Dashboard',
    page_icon='taxi',
    layout='wide'
)

st.title('NYC Yellow Taxi Dashboard')

def day(num:int)-> str:
    days = ["Monday", "Tuesday", "Wednesday","Thursday", "Friday", "Saturday", "Sunday"]
    if num <=0 or num > 7:
        return "Invalid"
    return days[num-1]

def payment(num:int)-> str:
    payment_type = ["Credit Card", "Cash", "No Charge", "Dispute", "Unknown"]
    if num <=0 or num > 5:
        return "Invalid"
    return payment_type[num-1]

@st.cache_data
def load_parquet():
    raw_data_dir = Path("data/raw")  # Create only if it doesn't already exist
    raw_data_dir.mkdir(parents=True, exist_ok=True)
    file_name = 'data/raw/yellow_taxi.parquet'
    file_path = Path(file_name)
    if not file_path.exists():
        parquet_response = requests.get('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet')
        if parquet_response.status_code == 200:
            with open(file_name, 'wb') as file:
                    file.write(parquet_response.content)
        else:
            st.error("Cannot fine Yellow Taxi Parquet!")
            st.info(
                "Please ensure the parquet file exists before proceeding.")
            st.stop()
            return None

    yellow_taxi = pl.read_parquet(file_name)


    #cleaning

    #1. Ensuring we have all necessary columns
    expected_columns = {
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
        "DOLocationID", "passenger_count", "trip_distance",
        "fare_amount", "total_amount", "payment_type"
    }

    for col in expected_columns:
        if col not in yellow_taxi.columns:
            st.error(f"{col} is not present in the parquet file")
            st.stop()

    #2. Removing nulls
    critical_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "fare_amount"]
    yellow_taxi = yellow_taxi.drop_nulls(critical_columns)

    #3. Filtering
    yellow_taxi= yellow_taxi.filter((pl.col("trip_distance") > 0) & (pl.col("fare_amount") > 0) & (pl.col("fare_amount") < 500))
    yellow_taxi= yellow_taxi.filter(pl.col("tpep_dropoff_datetime") > pl.col("tpep_pickup_datetime"))

    #4 adding and aggregating columns
    yellow_taxi = yellow_taxi.with_columns([
        ((pl.col('tpep_dropoff_datetime') - pl.col("tpep_pickup_datetime")).dt.total_seconds() / 60).alias(
            'trip_duration_minutes')])

    yellow_taxi = yellow_taxi.with_columns([
        pl.when(pl.col("trip_duration_minutes") <= 0)
        .then(0)
        .otherwise(pl.col("trip_distance") / pl.col("trip_duration_minutes"))
        .alias('trip_speed_mph'),

        pl.col("tpep_pickup_datetime").dt.date().alias('pickup_date'),

        pl.col("tpep_dropoff_datetime").dt.date().alias('drop_date'),

        pl.col('tpep_pickup_datetime').dt.hour().alias('pickup_hour'),

        pl.col('payment_type').map_elements(payment).alias('payment_type'),

        pl.col('tpep_pickup_datetime').dt.weekday().map_elements(day).alias('pickup_day_of_week')])

    return yellow_taxi



@st.cache_data
def load_csv():
    raw_data_dir = Path("data/raw")  # Create only if it doesn't already exist
    raw_data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_name = 'data/raw/taxi_lookup.csv'
    file_path = Path(csv_file_name)
    if not file_path.exists():
        csv_response = requests.get('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv')
        if csv_response.status_code == 200:
            with open(csv_file_name, 'wb') as file:
                file.write(csv_response.content)
        else:
            st.error("Cannot fine TAXI Lookup CSV!")
            st.info(
                "Please ensure the csv file exists before proceeding.")
            st.stop()
            return None
    return pl.read_csv(csv_file_name)


#loading data
yellow_taxi_data = load_parquet()
taxi_zone = load_csv()


st.subheader('Description')

st.markdown("""
This dashboard lets you explore NYC Yellow Taxi trip data. You can filter by date, hour of day, and payment type, 
and view insights such as top pickup zones, fare patterns, trip distances, payment breakdowns, and weekly trip trends.

Built with Streamlit and Plotly. 
""")

st.divider()



# Display key metrics
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric('Total Trips', f'{len(yellow_taxi_data):,}')
col2.metric('Avg Fare', f'${yellow_taxi_data["fare_amount"].mean():.2f}')
col3.metric('Total Revenue', f'${yellow_taxi_data["fare_amount"].sum():.2f}')
col4.metric('Avg Trip Distance', f'{yellow_taxi_data["trip_distance"].mean():.2f} mi')
col5.metric('Avg Trip Duration', f'{yellow_taxi_data["trip_duration_minutes"].mean():.2f} mins')


st.subheader('Visualizations')

st.divider()

# date range filter
min_date = yellow_taxi_data.select(pl.col("pickup_date").min()).item()
max_date = yellow_taxi_data.select(pl.col("pickup_date").max()).item()

date_range = st.date_input(
    "Pick your dates:",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Handle the annoying case where user only selects one date
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

#hour slider filter
start_hour, stop_hour = st.slider('Hour of Day', min_value=0, max_value=23, value=(0, 23), step=1)

#payment type dropdown
payment_map = {1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 5:'Unknown'}
payment_codes = yellow_taxi_data.select(pl.col("payment_type").unique()).to_series().to_list()
payment_options = [payment_map.get(code, str(code)) for code in payment_codes]

selected_options = st.multiselect(
    label='Choose your options',
    options= payment_options,
    default= payment_options)



filtered_yellow_taxi_data = (
    yellow_taxi_data
    .filter(pl.col("pickup_date").is_between(start_date, end_date))
    .filter((pl.col("pickup_hour")>= start_hour) & (pl.col("pickup_hour")<= stop_hour))
    .filter(pl.col("payment_type").is_in(selected_options) )
)

tab1, tab2, tab3, tab4 , tab5  = st.tabs(["Top 10 Pickup Zones", "Average fare by hour of day",
                                          "Distribution of trip distances", "Payment types", "Trips by day and week"])




con = duckdb.connect()
# used for querying data for some visualizations

resultQ1 = con.execute('''
    SELECT COUNT(*) as total_trips, b.Zone
    FROM filtered_yellow_taxi_data AS a
    LEFT JOIN taxi_zone AS b ON a.PULocationID = b.LocationID
    GROUP BY b.Zone
    ORDER BY total_trips DESC
    LIMIT 10
''').fetchdf()

resultQ2 = con.execute('''
    SELECT AVG(fare_amount) AS avg_fare, pickup_hour AS hour
    FROM filtered_yellow_taxi_data
    GROUP BY hour
    ORDER BY hour
''').fetchdf()

resultQ3 = con.execute('''
    SELECT (COUNT(*) * 100 / (SELECT COUNT(*) FROM filtered_yellow_taxi_data)) AS percentage , payment_type
    FROM yellow_taxi_data
    GROUP BY payment_type
''').fetchdf()



# Visualization tabs

with tab1:
    st.subheader('Top 10 Pickup Zones')
    with st.expander("About this chart"):
        st.write("The visualization shows that Midtown center is the top zone with the most trips. "
                 "It is also revealed that the airports such as JFK nad LaGuardia are also common zones that taxis pick up from.")

    st.write("Chart with Plotly")
    fig = px.bar(resultQ1, x="Zone", y="total_trips", color="Zone",
                     labels={'total_trips': 'Total Trips'})
    fig.update_layout(height=500)
    st.plotly_chart(fig, width='stretch')


with tab2:
    st.subheader('Average Fare by hour of day ')
    with st.expander("About this chart"):
        st.write("The visualization shows that the fare tends to be very high around 5 am. This could mean that the trips taken at that hour are usually very long distances. "
                 "It can also mean that 5 am is busy time and taxi drivers tend to drive up their fares due to the high demand.")

    filtered = resultQ2[(resultQ2["hour"] >= start_hour) & (resultQ2["hour"] <= stop_hour)]

    st.write("Chart with Plotly")
    fig2 = px.line(filtered, x="hour", y="avg_fare", title="Average Fare by hour of day",
                       labels={'avg_fare': 'Average Fare', 'hour': 'Hour'})
    fig2.update_layout(height=500)
    st.plotly_chart(fig2, width='stretch')

with tab3:
    st.subheader('Distribution of Trips')
    with st.expander("About this chart"):
        st.write("The visualization shows that the majority of the taxi trips in NYC are short distances, evidently speaking these trips are mostly travelling within the city and not out of city. "
                 "This suggests that walking or public transit may not be as convenient as taxis.")

    dist = yellow_taxi_data.filter(
        (pl.col("trip_distance") > 0) &
        (pl.col("trip_distance") < 50)
    )

    fig3 = px.histogram(dist, x="trip_distance", nbins=100, histnorm="percent", title="Trip Distance Distribution")
    fig3.update_layout(height=500)
    st.plotly_chart(fig3, width='stretch')

with tab4:
    st.subheader('Breakdown of Payment Types')
    with st.expander("About this chart"):
        st.write("The visualization shows that the majority of trips are paid in cash. This shows that NYC mostly relies on cash.")

    st.write("Chart with Plotly")
    fig4 = px.bar(resultQ3, x="payment_type", y="percentage", title="Breakdown of Payment Types", color="payment_type",
                      labels={'payment_type': 'Payment Type', "percentage": "Percentage of Trips"}, height=400)
    fig4.update_layout(height=500)
    st.plotly_chart(fig4, width='stretch')


with tab5:
    st.subheader('Trips by day of week and hour')
    with st.expander("About this chart"):
        st.write(" The visualization shows that between the hours of 4 to 6 pm, there is heavy taxi usage and this suggests commuters finishing work and heading home, specifically on Tuesdays to Thursdays."
                 "There is also a good number of trips in the later evenings on Fridays and Saturdays which may suggest nightlife movement.")

    fig5 = px.density_heatmap(filtered_yellow_taxi_data, x="pickup_hour", y="pickup_day_of_week",
                              labels={"pickup_hour": "Hour", "pickup_day_of_week": "Day of Week"})
    fig5.update_coloraxes(colorbar_title="Number of Trips")
    fig5.update_layout(height=500)
    st.plotly_chart(fig5, width='stretch')














