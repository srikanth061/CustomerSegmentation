import streamlit as st
import pandas as pd
import plotly.express as px
import base64, tempfile,os
import boto3,json
from dotenv import load_dotenv

st.title("Customer Segmentation Dashboard")
def create_treemap(data):
    tier_data = data.groupby(['tier', 'RFMScore']).agg({'Monetary': 'sum', 'Frequency': 'mean', 'CustomerID': 'count'}).reset_index()
    tier_data.rename(columns={'CustomerID': 'Customer Count', 'Frequency': 'Average Frequency'}, inplace=True)
    tier_data_rounded = tier_data.round()

    fig = px.treemap(tier_data_rounded, 
                     path=[px.Constant('RFM'), 'tier', 'RFMScore'], 
                     values='Customer Count',
                     hover_data=['Monetary', 'Average Frequency', 'Customer Count'],
                     color='RFMScore', 
                     color_continuous_scale='RdBu',
                     title='Customer Segmentation')
    return fig
load_dotenv()
key = os.environ["KEY"]
s_key = os.environ["S_KEY"]
b_name = os.environ["B_NAME"]
r_name = os.environ["REGION_NAME"]
def invoke_lambda(file):
    with st.spinner("segmentation in process..."):
        lambda_client = boto3.client('lambda', 
        region_name=r_name,
        aws_access_key_id = key,
        aws_secret_access_key = s_key
        )

        function_name = 'customersegmentation'

        payload = {
        "body": "{\"filename\": \"%s\"}" % file
        }

        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        json_string = response['Payload'].read().decode('utf-8')
        response_data = json.loads(json_string)
        response_status = response_data['statusCode']
        if response_status == 200:
            response_body = response_data['body']
            body_dict = json.loads(response_body)
            data = body_dict['data']
            df = pd.DataFrame(data)
            return df
        else:
            st.write(response_data['body'])

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
@st.cache_data(show_spinner=False)
def upload_file_to_s3(file_upload, dataframe):
    file = dataframe.to_csv(index=False).encode()
    temp_dir = tempfile.TemporaryDirectory()
    file_path = os.path.join(temp_dir.name, file_upload.name)
    with open(file_path, 'wb') as f:
        f.write(file)
    s3_client = boto3.client('s3',
                region_name=r_name,
                aws_access_key_id = key,
                aws_secret_access_key = s_key
                )
    with st.spinner("Uploading file in progress..."):
        timestamp = pd.Timestamp.now()
        s3_file_key = f"transactiondata/{timestamp}-{file_upload.name}"
        s3_client.upload_file(file_path, b_name, s3_file_key)
        df = invoke_lambda(s3_file_key)
        return df
with st.sidebar:
    file_upload = st.file_uploader("Upload CSV file", type=['csv'])
    df = pd.DataFrame()
    if file_upload is not None:
        file_to_df = pd.read_csv(file_upload,encoding='latin-1')
        Recency_column_names = [''] + file_to_df.columns.tolist()
        Recency_column = st.selectbox("**Select Recency:**", Recency_column_names)
        Frequency_column_names = [''] + file_to_df.columns.tolist()
        Frequency_column = st.selectbox("**Select Frequency:**", Frequency_column_names)
        Monetary_column_names = [''] + file_to_df.columns.tolist()
        Monetary_column = st.selectbox("**Select Monetary:**", Monetary_column_names)
        column_rename = {
            Recency_column: 'InvoiceDate',
            Frequency_column: 'Invoice',
            Monetary_column: 'Price',
        }
        if Recency_column and Frequency_column and Monetary_column:
            file_to_df.rename(columns=column_rename,inplace=True)
            df = upload_file_to_s3(file_upload,file_to_df)
    else:
        df = pd.DataFrame()
    
if df is not None and not df.empty:
    
    st.plotly_chart(create_treemap(df),theme="streamlit", use_container_width=True)

    selected_tier = st.selectbox("**Select a tier:**", ['All'] + list(df["tier"].unique()))
    if selected_tier != 'All':
        filtered_df = df[df["tier"] == selected_tier]
        # create_treemap(df)
    else:
        filtered_df = df
    
    st.write("**Filtered Data:**")
    filtered_data_display = filtered_df[['CustomerID', 'RFMScore', 'tier', 'Cluster']]
    st.write(filtered_data_display)
    if st.button("Download Data"):
        filtered_csv = filtered_data_display.to_csv(index=False)
        b64 = base64.b64encode(filtered_csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="filtered_data.csv">Download Filtered Data</a>'
        st.markdown(href, unsafe_allow_html=True)