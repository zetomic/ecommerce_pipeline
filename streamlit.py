# streamlit_dashboard.py

import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px

# ---------------------------
# 1. Streamlit App Configuration
# ---------------------------

st.set_page_config(
    page_title="Kafka Data Pull Dashboard",
    layout="wide",
)

st.title("🛒 Kafka Data Pull Dashboard")
st.markdown("This dashboard pulls and displays all messages from the `eda_results` Kafka topic for verification.")

# ---------------------------
# 2. Function to Consume Kafka Messages
# ---------------------------

def consume_kafka_messages():
    """
    Connects to Kafka and consumes all messages from the 'eda_results' topic.
    Returns:
        List of message dictionaries.
    """
    try:
        consumer = KafkaConsumer(
            'eda_results',
            bootstrap_servers=['localhost:29092'],  # Update if different
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=None,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000  # Timeout after 10 seconds if no new messages
        )
        
        messages = []
        for message in consumer:
            msg = message.value
            if 'metric' in msg:
                # Normalize the metric field
                msg['metric'] = msg['metric'].lower().replace(' ', '_')
            messages.append(msg)
        
        consumer.close()
        return messages
    except Exception as e:
        st.error(f"An error occurred while consuming Kafka messages: {e}")
        return []

# ---------------------------
# 3. Button to Trigger Data Pull
# ---------------------------

if st.button("🔄 Pull Data from Kafka"):
    with st.spinner("Connecting to Kafka and pulling data..."):
        data = consume_kafka_messages()
    
    if data:
        st.success(f"Successfully pulled {len(data)} messages from the `eda_results` topic.")
        
        # ---------------------------
        # 4. Display Raw Messages
        # ---------------------------
        
        st.header("📄 Raw Messages")
        raw_df = pd.DataFrame(data)
        st.dataframe(raw_df)
        
        # ---------------------------
        # 5. Display Unique Metrics
        # ---------------------------
        
        st.header("📋 Unique Metrics Found")
        if 'metric' in raw_df.columns:
            unique_metrics = raw_df['metric'].unique()
            st.write(unique_metrics)
        else:
            st.warning("No 'metric' field found in the messages.")
        
        # ---------------------------
        # 6. Download Data as CSV
        # ---------------------------
        
        csv = raw_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Data as CSV",
            data=csv,
            file_name='eda_results_data.csv',
            mime='text/csv',
        )
        
        # ---------------------------
        # 7. Display Metrics Visualizations
        # ---------------------------
        
        st.header("📊 Metrics Visualizations")
        
        # Define metrics and their properties
        metric_sections = {
            'top_products': {
                'title': '📈 Top Products',
                'description': 'Displays the top-selling products based on message frequency.'
            },
            'top_brands': {
                'title': '🏷️ Top Brands',
                'description': 'Shows the most popular brands based on cumulative brand counts.'
            },
            'sales_by_category': {
                'title': '💰 Sales by Category',
                'description': 'Aggregated sales figures across different categories.'
            },
            'quantity_by_city': {
                'title': '📦 Quantity Procured by City',
                'description': 'Total quantity of products procured per city based on message frequency.'
            },
            'aov_by_customer': {
                'title': '💳 Average Order Value by Customer',
                'description': 'Average amount spent per customer.'
            },
            # Add more metrics as needed
        }
        
        for metric, props in metric_sections.items():
            st.subheader(props['title'])
            st.write(props['description'])
            
            # Filter messages for the current metric
            metric_data = raw_df[raw_df['metric'] == metric]
            
            if not metric_data.empty:
                df_metric = pd.DataFrame(metric_data)
                
                # Visualization based on metric type
                if metric == 'top_products':
                    if 'product_name' in df_metric.columns:
                        # Aggregate by counting occurrences of each product
                        df_grouped = df_metric['product_name'].value_counts().reset_index()
                        df_grouped.columns = ['product_name', 'count']
                        df_sorted = df_grouped.sort_values(by='count', ascending=False).head(10)
                        fig = px.bar(
                            df_sorted,
                            x='product_name',
                            y='count',
                            title='Top 10 Products',
                            labels={'count': 'Sales Count', 'product_name': 'Product Name'},
                            text='count'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("The 'product_name' field is missing for Top Products.")
                
                elif metric == 'top_brands':
                    if 'brand_name' in df_metric.columns:
                        if 'brand_count' in df_metric.columns:
                            # Sum the brand_count for each brand
                            df_grouped = df_metric.groupby('brand_name')['brand_count'].sum().reset_index()
                        else:
                            # If 'brand_count' is missing, count occurrences
                            df_grouped = df_metric['brand_name'].value_counts().reset_index()
                            df_grouped.columns = ['brand_name', 'brand_count']
                        df_sorted = df_grouped.sort_values(by='brand_count', ascending=False).head(10)
                        fig = px.bar(
                            df_sorted,
                            x='brand_name',
                            y='brand_count',
                            title='Top 10 Brands',
                            labels={'brand_count': 'Brand Count', 'brand_name': 'Brand Name'},
                            text='brand_count'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("The 'brand_name' field is missing for Top Brands.")
                
                elif metric == 'sales_by_category':
                    if 'l1_category' in df_metric.columns:
                        if 'total_sales' in df_metric.columns:
                            # Sum the total_sales for each category
                            df_grouped = df_metric.groupby('l1_category')['total_sales'].sum().reset_index()
                        else:
                            # If 'total_sales' is missing, count occurrences
                            df_grouped = df_metric['l1_category'].value_counts().reset_index()
                            df_grouped.columns = ['l1_category', 'total_sales']
                        fig = px.pie(
                            df_grouped,
                            names='l1_category',
                            values='total_sales',
                            title='Sales Distribution by Category'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("The 'l1_category' field is missing for Sales by Category.")
                
                elif metric == 'quantity_by_city':
                    if 'city_name' in df_metric.columns:
                        if 'total_procured_quantity' in df_metric.columns:
                            # Sum the total_procured_quantity for each city
                            df_grouped = df_metric.groupby('city_name')['total_procured_quantity'].sum().reset_index()
                        else:
                            # If 'total_procured_quantity' is missing, count occurrences
                            df_grouped = df_metric['city_name'].value_counts().reset_index()
                            df_grouped.columns = ['city_name', 'total_procured_quantity']
                        df_sorted = df_grouped.sort_values(by='total_procured_quantity', ascending=False)
                        fig = px.bar(
                            df_sorted,
                            x='city_name',
                            y='total_procured_quantity',
                            title='Quantity Procured by City',
                            labels={'total_procured_quantity': 'Total Quantity', 'city_name': 'City Name'},
                            text='total_procured_quantity'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("The 'city_name' field is missing for Quantity by City.")
                
                else:
                    st.info(f"No visualization implemented for metric: {metric}")
            else:
                st.info(f"No data available for metric: {metric}")
            
            st.markdown("---")  # Separator between sections
