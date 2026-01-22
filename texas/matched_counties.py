#!/usr/bin/env python3
"""
Texas Highway to County Matcher (Simplified)
Finds the primary county for each highway route number
"""

import pandas as pd
import geopandas as gpd
import requests
from collections import Counter

def download_txdot_highways():
    """Download TxDOT Memorial Highways GeoJSON from ArcGIS REST API"""
    print("Downloading TxDOT Memorial Highways data...")
    
    base_url = "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/TxDOT_Memorial_Highways/FeatureServer/0/query"
    
    params = {
        'where': '1=1',
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'geojson',
        'outSR': '4326'
    }
    
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    
    geojson_data = response.json()
    gdf = gpd.GeoDataFrame.from_features(geojson_data['features'])
    gdf.set_crs(epsg=4326, inplace=True)
    
    print(f"Downloaded {len(gdf)} highway segments")
    return gdf

def download_texas_counties():
    """Download Texas county boundaries from US Census Bureau"""
    print("Downloading Texas county boundaries...")
    
    # TIGER/Line 2024 Texas Counties - State FIPS code 48 is Texas
    url = "https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip"
    
    gdf = gpd.read_file(url)
    # Filter for Texas only (STATEFP = '48')
    gdf = gdf[gdf['STATEFP'] == '48']
    gdf = gdf.to_crs(epsg=4326)
    
    print(f"Downloaded {len(gdf)} Texas counties")
    return gdf

def find_primary_county_by_route(highways_gdf, counties_gdf):
    """
    For each highway route number, find the county where most of its length is located
    
    Returns:
        Dictionary mapping highway_number to primary county name
    """
    print("Finding primary county for each highway route...")
    
    route_to_county = {}
    
    # Group highways by route number
    # Try different possible field names for route number
    route_field = None
    for field in ['RTE_NM', 'HIGHWAY', 'RTE_NM', 'ROUTE', 'HWY_NUM']:
        if field in highways_gdf.columns:
            route_field = field
            break
    
    if route_field is None:
        print("Warning: Could not find route number field in highway data")
        print(f"Available fields: {list(highways_gdf.columns)}")
        return route_to_county
    
    # Get unique route numbers
    unique_routes = highways_gdf[route_field].dropna().unique()
    print(f"Processing {len(unique_routes)} unique routes...")
    
    for route_num in unique_routes:
        # Get all segments for this route
        route_segments = highways_gdf[highways_gdf[route_field] == route_num]
        
        # Track total length in each county
        county_lengths = Counter()
        
        for idx, segment in route_segments.iterrows():
            if segment.geometry is None or segment.geometry.is_empty:
                continue
            
            # Find which counties this segment intersects
            for _, county in counties_gdf.iterrows():
                if segment.geometry.intersects(county.geometry):
                    # Calculate the length of intersection
                    try:
                        intersection = segment.geometry.intersection(county.geometry)
                        length = intersection.length
                        county_lengths[county['NAME']] += length
                    except:
                        # If intersection fails, just count it as being in this county
                        county_lengths[county['NAME']] += 1
        
        # Find the county with the most length
        if county_lengths:
            primary_county = county_lengths.most_common(1)[0][0]
            route_to_county[str(route_num)] = primary_county
    
    print(f"Mapped {len(route_to_county)} routes to their primary counties")
    return route_to_county

def add_county_to_csv(csv_data, route_to_county):
    """Add county column to CSV based on highway_number lookup"""
    print("Adding county column to CSV...")
    
    csv_data['county'] = csv_data['RTE_NM'].astype(str).map(route_to_county)
    
    # Fill any unmatched routes
    unmatched = csv_data['county'].isna().sum()
    if unmatched > 0:
        print(f"Warning: {unmatched} rows could not be matched to a county")
        csv_data['county'].fillna('Unknown', inplace=True)
    
    return csv_data

def main():
    input_csv = 'C:/Users/lucas/Data_Science_Capstone/Capstone/texas/texas_raw_data.csv'
    output_csv = 'texas_data_with_counties.csv'
    
    try:
        # Load CSV
        print(f"Reading {input_csv}...")
        csv_data = pd.read_csv(input_csv)
        print(f"Loaded {len(csv_data)} rows")
        
        # Download spatial data
        highways_gdf = download_txdot_highways()
        counties_gdf = download_texas_counties()
        
        # Find primary county for each route
        route_to_county = find_primary_county_by_route(highways_gdf, counties_gdf)
        
        # Add county to CSV
        result = add_county_to_csv(csv_data, route_to_county)
        
        # Save result
        result.to_csv(output_csv, index=False)
        print(f"\n✓ Success! Saved results to {output_csv}")
        
        # Print summary
        print("\nSummary:")
        print(f"Total rows: {len(result)}")
        print(f"Unique highways: {result['RTE_NM'].nunique()}")
        print(f"Unique counties assigned: {result[result['county'] != 'Unknown']['county'].nunique()}")
        print(f"Unknown county: {(result['county'] == 'Unknown').sum()} rows")
        
        # Show sample of route-to-county mappings
        print("\nSample route-to-county mappings:")
        for route, county in list(route_to_county.items())[:10]:
            print(f"  {route}: {county}")
        
    except FileNotFoundError:
        print(f"Error: Could not find {input_csv}")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()