from __future__ import annotations

import os
import requests
import streamlit as st
import pandas as pd

API_BASE = os.getenv('LINEAGE_API_BASE', 'http://127.0.0.1:8000')

st.set_page_config(page_title='Enterprise Lineage MVP', layout='wide')
st.title('Enterprise Lineage MVP')
st.caption('Search assets across tools, inspect lineage, and run impact analysis.')

query = st.text_input('Search asset', 'customer_master')
if st.button('Search'):
    resp = requests.get(f'{API_BASE}/api/v1/assets', params={'query': query}, timeout=20)
    resp.raise_for_status()
    results = resp.json()['results']
    st.subheader('Search results')
    st.dataframe(pd.DataFrame(results), use_container_width=True)

asset_id = st.text_input('Asset ID for lineage', 'eld://dataset/onprem/prod/teradata/edw/customer_master')
col1, col2 = st.columns(2)
with col1:
    if st.button('Show upstream'):
        resp = requests.get(f'{API_BASE}/api/v1/lineage/upstream', params={'assetId': asset_id, 'depth': 6}, timeout=20)
        st.json(resp.json())
with col2:
    if st.button('Show downstream'):
        resp = requests.get(f'{API_BASE}/api/v1/lineage/downstream', params={'assetId': asset_id, 'depth': 6}, timeout=20)
        st.json(resp.json())

if st.button('Run impact analysis'):
    resp = requests.get(f'{API_BASE}/api/v1/impact', params={'assetId': asset_id, 'depth': 6}, timeout=20)
    st.json(resp.json())

if st.button('Publish asset to Purview export'):
    resp = requests.post(f'{API_BASE}/api/v1/publish', json={'asset_id': asset_id, 'target': 'purview_export'}, timeout=20)
    st.json(resp.json())
