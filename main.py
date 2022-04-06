from calendar import week
from datetime import datetime, timedelta
import os
import json
import requests
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import sys
import pydeck as pdk
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import altair as alt


st.title("Rexburg Idaho Businesses")
# LzDerxCG1RVEVCLsa0dX08p9h3OnNlKT
api_key = st.text_input('Enter your API key here.',
                        'LzDerxCG1RVEVCLsa0dX08p9h3OnNlKT')
# st.write('Your api Key is ', api_key)

weeks = st.multiselect(
    'What Week are you looking into?',
    ['2022-03-21', '2022-03-14', '2022-03-07', '2022-02-28',
     '2022-02-21', '2022-02-14', '2022-02-7', '2022-01-31',
     '2022-01-24', '2022-01-17', '2022-01-10', '2022-01-03'])
sfkey = api_key
# st.write(weeks[0])
# %%
url = 'https://api.safegraph.com/v2/graphql'
query = """query {
  search(
      filter: {
        address: { 
            city: "Rexburg", 
            region: "ID" 
        }
    }
    ){
    places {
      results(first: 500 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            weekly_patterns (start_date: "2022-03-21" end_date: "2022-03-27") {
              placekey
              location_name
              street_address
              city
              date_range_start
              raw_visit_counts
            }
          }
        }
      }
    }
  }
}
"""


# Core API call
query2 = """
query {
  search(
      filter: {
        address: { 
            city: "Rexburg", 
            region: "ID" 
        }
    }
    ) {
    places {
      results(first: 500 after: "") {
      pageInfo {hasNextPage, endCursor}
      edges {
        node {
          safegraph_core {
          	placekey
            latitude
            longitude
            }
        	}
      	}
    	}
  	}
	}
}
"""
# %%
# Using the requests package
start = ['2022-03-21', '2022-03-14', '2022-03-07', '2022-02-28',
         '2022-02-21', '2022-02-14', '2022-02-7', '2022-01-31',
         '2022-01-24', '2022-01-17', '2022-01-10', '2022-01-03']
end = ['2022-03-27', '2022-03-20', '2022-03-13', '2022-03-06',
       '2022-02-27', '2022-02-20', '2022-02-13', '2022-02-6',
       '2022-01-30', '2022-01-23', '2022-01-16', '2022-01-09']
dat1 = pd.DataFrame()
if weeks != []:
    query1 = query.replace('2022-03-21', start[start.index(weeks[0])])\
        .replace('2022-03-27', end[start.index(weeks[0])])
else:
    query1 = query

r = requests.post(
    url,
    json={'query': query1},
    headers={'Content-Type': 'application/json', 'apikey': sfkey})

# %%
# print(r.status_code)
# print(r.text)
json_data = json.loads(r.text)
df_data = json_data['data']  # ['lookup']
print(df_data)

# %%
pract = df_data.copy()

pd.json_normalize(pract)
# %%

# https://gql.readthedocs.io/en/v3.0.0a6/
# https://github.com/graphql-python/gql

# Select your transport with a defined url endpoint
transport = RequestsHTTPTransport(
    url=url,
    verify=True,
    retries=3,
    headers={'Content-Type': 'application/json', 'apikey': sfkey})

client = Client(transport=transport, fetch_schema_from_transport=True)


# %%
results = client.execute(gql(query2))


# %%
edges = results['search']['places']['results']['edges']
resultsNorm = [dat.pop('node') for dat in edges]
resultsNorm = [dat.pop('safegraph_core') for dat in resultsNorm]

dat = pd.json_normalize(resultsNorm)

# %%
results = client.execute(gql(query))


# %%


results = client.execute(gql(query1))
edges = results['search']['places']['results']['edges']
resultsNorm = [dat.pop('node') for dat in edges]
resultsNorm = [dat.pop('weekly_patterns') for dat in resultsNorm]

for i in range(0, len(resultsNorm)):
    if resultsNorm[i] is not None:
        dat1 = dat1.append(resultsNorm[i])

df = dat1.merge(dat, on='placekey')

# from asyncio.windows_events import NULL
count = st.slider('Minimum visit count', min_value=0,
                  max_value=5000, value=0)
st.write('This is the buinesses here in Rexburg Idaho')
df1 = df.query("raw_visit_counts >= @count")
df1['date_range_start'] = pd.to_datetime(df1['date_range_start']).dt.date
st.dataframe(df1)
header = st.container()
map = st.container()


st.pydeck_chart(pdk.Deck(
    map_style='mapbox://styles/mapbox/light-v10',
    initial_view_state=pdk.ViewState(
        latitude=43.8229889,
        longitude=-111.7844691,
        zoom=11,
        min_zoom=4,
        max_zoom=15
    ),
    layers=[
        pdk.Layer(
            'ScatterplotLayer',
            data=df1[['latitude', 'longitude',
                      'city', 'location_name']].dropna(),
            get_position=['longitude', 'latitude'],
            auto_highlight=True,
            pickable=True,
            coverage=1,
            radius_min_pixels=3,
            radius_max_pixels=5)
    ],  # Added a tooltip here
    tooltip={
        'html': '<b>location_name:</b> {location_name}',
        'style': {
                'color': 'white'
        }
    }
))
