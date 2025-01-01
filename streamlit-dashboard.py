# streamlit_dashboard.py

import streamlit as st
from kafka import KafkaConsumer
import json
import threading
from collections import defaultdict, deque
import pandas as pd
import plotly.express as px
import time
import queue  # Import queue for thread-safe communication

# ---------------------------
# 1. Initialize Session State and Queue
# ---------------------------

# Initialize session state for metrics data
if 'data' not in st.session_state:
    st.session_state['data'] = defaultdict(lambda: deque(maxlen=100))  # Store up to 100 records per metric

# Initialize session state for raw messages (for debugging)
if 'raw_messages' not in st.session_state:
    st.session_state['raw_messages'] = deque(maxlen=100)  # Store raw messages for debugging

# Initialize a thread-safe queue
message_queue = queue.Queue()

# ---------------------------
# 2. Define Kafka Consumer Function
# ---------------------------

def consume_kafka():
    try:
        consumer = KafkaConsumer(
            'eda_results',
            bootstrap_servers=['localhost:29092'],  # Updated Configuration
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='streamlit-dashboard-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        for message in consumer:
            msg = message.value
            metric = msg.get('metric')
            if metric:
                message_queue.put(msg)  # Enqueue the message
            # Optional: Add a short sleep to prevent tight loop
            time.sleep(0.01)
    except Exception as e:
        message_queue.put({"error": str(e)})

# ---------------------------
# 3. Start Kafka Consumer Thread
# ---------------------------

if 'consumer_thread' not in st.session_state:
    consumer_thread = threading.Thread(target=consume_kafka, daemon=True)
    consumer_thread.start()
    st.session_state['consumer_thread'] = consumer_thread

# ---------------------------
# 4. Define Function to Process Queue Messages
# ---------------------------

def process_queue():
    while not message_queue.empty():
        msg = message_queue.get()
        if 'error' in msg:
            st.error(f"Error in Kafka consumer: {msg['error']}")
            continue  # Skip processing this message
        
        metric = msg.get('metric')
        if metric:
            st.session_state['data'][metric].append(msg)
            st.session_state['raw_messages'].append(msg)  # For debugging

# ---------------------------
# 5. Add Real-Time Updates with experimental_autorefresh
# ---------------------------

# Refresh the app every 2 seconds
st.experimental_autorefresh(
    interval=2000,  # in milliseconds
    limit=None,      # no limit on refreshes
    key="data_refresh"
)

# Process queued messages
process_queue()

# ---------------------------
# 6. Dashboard Layout with Real-Time Updates
# ---------------------------

st.title("🛒 Real-Time E-commerce Analytics Dashboard")

# ---------------------------
# 6.1. Display Raw Messages for Debugging
# ---------------------------

st.header("🔍 Raw Messages (For Debugging)")
if st.session_state['raw_messages']:
    # Convert deque to list for display
    raw_df = pd.DataFrame(list(st.session_state['raw_messages']))
    st.dataframe(raw_df)
else:
    st.info("No messages received yet.")

st.markdown("---")  # Separator

# ---------------------------
# 6.2. Define Metrics and Their Properties
# ---------------------------

metric_sections = {
    'top_products': {
        'title': '📈 Top Products',
        'description': 'Displays the top-selling products.'
    },
    'top_brands': {
        'title': '🏷️ Top Brands',
        'description': 'Shows the most popular brands.'
    },
    'sales_by_category': {
        'title': '💰 Sales by Category',
        'description': 'Aggregated sales figures across different categories.'
    },
    'quantity_by_city': {
        'title': '📦 Quantity Procured by City',
        'description': 'Total quantity of products procured per city.'
    },
    'aov_by_customer': {
        'title': '💳 Average Order Value by Customer',
        'description': 'Average amount spent per customer.'
    },
    # Add more metrics as needed
}

for metric, props in metric_sections.items():
    st.header(props['title'])
    st.write(props['description'])
    
    if metric in st.session_state['data']:
        records = list(st.session_state['data'][metric])
        if records:
            df = pd.DataFrame(records)
            
            try:
                # Visualization based on metric type
                if metric == 'top_products':
                    # Assuming each record has 'product_name' and 'count'
                    if 'product_name' in df.columns and 'count' in df.columns:
                        if pd.api.types.is_numeric_dtype(df['count']):
                            df_grouped = df.groupby('product_name')['count'].sum().reset_index()
                            df_sorted = df_grouped.sort_values(by='count', ascending=False).head(10)
                            fig = px.bar(
                                df_sorted,
                                x='product_name',
                                y='count',
                                title='Top 10 Products',
                                labels={'count': 'Sales Count', 'product_name': 'Product Name'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("The 'count' field is not numeric for Top Products.")
                    else:
                        st.warning("Insufficient data for Top Products.")
                
                elif metric == 'top_brands':
                    # Assuming each record has 'brand_name' and 'brand_count'
                    if 'brand_name' in df.columns and 'brand_count' in df.columns:
                        if pd.api.types.is_numeric_dtype(df['brand_count']):
                            df_grouped = df.groupby('brand_name')['brand_count'].sum().reset_index()
                            df_sorted = df_grouped.sort_values(by='brand_count', ascending=False).head(10)
                            fig = px.bar(
                                df_sorted,
                                x='brand_name',
                                y='brand_count',
                                title='Top 10 Brands',
                                labels={'brand_count': 'Brand Count', 'brand_name': 'Brand Name'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("The 'brand_count' field is not numeric for Top Brands.")
                    else:
                        st.warning("Insufficient data for Top Brands.")
                
                elif metric == 'sales_by_category':
                    # Assuming each record has 'l1_category' and 'total_sales'
                    if 'l1_category' in df.columns and 'total_sales' in df.columns:
                        if pd.api.types.is_numeric_dtype(df['total_sales']):
                            df_grouped = df.groupby('l1_category')['total_sales'].sum().reset_index()
                            fig = px.pie(
                                df_grouped,
                                names='l1_category',
                                values='total_sales',
                                title='Sales Distribution by Category'
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("The 'total_sales' field is not numeric for Sales by Category.")
                    else:
                        st.warning("Insufficient data for Sales by Category.")
                
                elif metric == 'quantity_by_city':
                    # Assuming each record has 'city_name' and 'total_procured_quantity'
                    if 'city_name' in df.columns and 'total_procured_quantity' in df.columns:
                        if pd.api.types.is_numeric_dtype(df['total_procured_quantity']):
                            df_grouped = df.groupby('city_name')['total_procured_quantity'].sum().reset_index()
                            df_sorted = df_grouped.sort_values(by='total_procured_quantity', ascending=False)
                            fig = px.bar(
                                df_sorted,
                                x='city_name',
                                y='total_procured_quantity',
                                title='Quantity Procured by City',
                                labels={'total_procured_quantity': 'Total Quantity', 'city_name': 'City Name'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("The 'total_procured_quantity' field is not numeric for Quantity by City.")
                    else:
                        st.warning("Insufficient data for Quantity by City.")
                
                elif metric == 'aov_by_customer':
                    # Assuming each record has 'dim_customer_key' and 'average_order_value'
                    if 'dim_customer_key' in df.columns and 'average_order_value' in df.columns:
                        if pd.api.types.is_numeric_dtype(df['average_order_value']):
                            df_grouped = df.groupby('dim_customer_key')['average_order_value'].mean().reset_index()
                            fig = px.histogram(
                                df_grouped,
                                x='average_order_value',
                                nbins=20,
                                title='Distribution of Average Order Value',
                                labels={'average_order_value': 'Average Order Value'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("The 'average_order_value' field is not numeric for Average Order Value by Customer.")
                    else:
                        st.warning("Insufficient data for Average Order Value by Customer.")
                
                else:
                    st.info(f"No visualization implemented for metric: {metric}")
            except Exception as e:
                st.error(f"Error while processing {metric}: {e}")
        else:
            st.info("No data available for this metric yet.")
    else:
        st.info("Awaiting data...")
    
    st.markdown("---")  # Separator between sections
