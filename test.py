# %%
# https://pypi.org/project/safegraphQL/
import sys
#!{sys.executable} -m pip install safegraphQL
# %%
# import sys
# !{sys.executable} -m pip install --pre gql
# %%
# https://docs.safegraph.com/reference#places-api-overview-new
# https://stackoverflow.com/questions/56856005/how-to-set-environment-variable-in-databricks/56863551
#import safegraphql.client as sgql

from gql.transport.requests import RequestsHTTPTransport

from gql import gql, Client
import requests
import pandas as pd
import json

import os
#from dotenv import load_dotenv

# load_dotenv()
#sfkey = os.environ.get("SAFEGRAPH_KEY")
sfkey = 'LzDerxCG1RVEVCLsa0dX08p9h3OnNlKT'
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
r = requests.post(
    url,
    json={'query': query},
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
start = ['2022-03-21', '2022-03-14', '2022-03-07', '2022-02-28',
         '2022-02-21', '2022-02-14', '2022-02-7', '2022-01-31',
         '2022-01-24', '2022-01-17', '2022-01-10', '2022-01-03']
end = ['2022-03-27', '2022-03-20', '2022-03-13', '2022-03-06', '2022-02-27',
       '2022-02-20', '2022-02-13', '2022-02-6', '2022-01-30',
       '2022-01-23', '2022-01-16', '2022-01-09']
dat1 = pd.DataFrame()

for i in range(0, len(start)):
    query2 = query.replace('--S_Date--', start[i])\
        .replace('--E_Date--', end[i])
    results = client.execute(gql(query2))
    edges = results['search']['places']['results']['edges']
    resultsNorm = [dat.pop('node') for dat in edges]
    resultsNorm = [dat.pop('weekly_patterns') for dat in resultsNorm]

    for i in range(0, len(resultsNorm)):
        if resultsNorm[i] is not None:
            dat1 = dat1.append(resultsNorm[i])

# %%
df = dat1.merge(dat, on='placekey')

# %%
