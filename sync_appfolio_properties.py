#!/usr/bin/env python3
"""
AppFolio Property Sync
Syncs property data from AppFolio to Supabase properties table
Runs daily via GitHub Actions
"""

import os
import sys
import requests
from datetime import datetime
from typing import Dict, List, Set

# Configuration from environment variables
APPFOLIO_BASE_URL = "https://keyrenter072.appfolio.com/api/v2/reports/"
APPFOLIO_CLIENT_ID = os.environ.get("APPFOLIO_CLIENT_ID")
APPFOLIO_CLIENT_SECRET = os.environ.get("APPFOLIO_CLIENT_SECRET")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def log(message: str):
    """Log with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def fetch_appfolio_properties() -> List[Dict]:
    """Fetch all active properties from AppFolio"""
    log("Fetching properties from AppFolio...")
    
    url = f"https://{APPFOLIO_CLIENT_ID}:{APPFOLIO_CLIENT_SECRET}@keyrenter072.appfolio.com/api/v2/reports/property_directory.json"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        "property_visibility": "active"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        properties = data.get("results", [])
        log(f"Fetched {len(properties)} active properties from AppFolio")
        return properties
        
    except requests.exceptions.RequestException as e:
        log(f"ERROR: Failed to fetch properties from AppFolio: {e}")
        sys.exit(1)

def fetch_supabase_properties() -> Dict[str, Dict]:
    """Fetch all properties from Supabase, indexed by appfolio_id"""
    log("Fetching properties from Supabase...")
    
    url = f"{SUPABASE_URL}/rest/v1/properties"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json"
    }
    
    params = {
        "select": "id,address,appfolio_id,active"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        properties = response.json()
        
        # Index by appfolio_id for quick lookup
        indexed = {p["appfolio_id"]: p for p in properties if p.get("appfolio_id")}
        log(f"Fetched {len(properties)} properties from Supabase ({len(indexed)} with appfolio_id)")
        return indexed
        
    except requests.exceptions.RequestException as e:
        log(f"ERROR: Failed to fetch properties from Supabase: {e}")
        sys.exit(1)

def insert_property(appfolio_id: str, address: str) -> bool:
    """Insert new property into Supabase"""
    url = f"{SUPABASE_URL}/rest/v1/properties"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    payload = {
        "address": address,
        "appfolio_id": appfolio_id,
        "active": True
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        log(f"ERROR: Failed to insert property {appfolio_id}: {e}")
        return False

def update_property_status(appfolio_id: str, active: bool) -> bool:
    """Update property active status in Supabase"""
    url = f"{SUPABASE_URL}/rest/v1/properties"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    params = {
        "appfolio_id": f"eq.{appfolio_id}"
    }
    
    payload = {
        "active": active
    }
    
    try:
        response = requests.patch(url, json=payload, headers=headers, params=params)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        log(f"ERROR: Failed to update property {appfolio_id}: {e}")
        return False

def sync_properties():
    """Main sync logic"""
    log("=" * 60)
    log("Starting AppFolio Property Sync")
    log("=" * 60)
    
    # Validate environment variables
    required_vars = [
        "APPFOLIO_CLIENT_ID", "APPFOLIO_CLIENT_SECRET",
        "SUPABASE_URL", "SUPABASE_SERVICE_KEY"
    ]
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        log(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    # Fetch data from both sources
    appfolio_properties = fetch_appfolio_properties()
    supabase_properties = fetch_supabase_properties()
    
    # Track statistics
    stats = {
        "added": 0,
        "reactivated": 0,
        "deactivated": 0,
        "unchanged": 0,
        "errors": 0
    }
    
    # Build set of active AppFolio property IDs
    appfolio_ids: Set[str] = set()
    
    # Process AppFolio properties
    for prop in appfolio_properties:
        appfolio_id = str(prop.get("property_id"))
        address = prop.get("property_address", "").strip()
        
        if not appfolio_id or not address:
            log(f"WARNING: Skipping property with missing ID or address: {prop}")
            continue
        
        appfolio_ids.add(appfolio_id)
        
        if appfolio_id in supabase_properties:
            # Property exists in Supabase
            existing = supabase_properties[appfolio_id]
            
            if not existing["active"]:
                # Reactivate property
                log(f"Reactivating property: {address} (ID: {appfolio_id})")
                if update_property_status(appfolio_id, True):
                    stats["reactivated"] += 1
                else:
                    stats["errors"] += 1
            else:
                # Property already active and synced
                stats["unchanged"] += 1
        else:
            # New property - insert it
            log(f"Adding new property: {address} (ID: {appfolio_id})")
            if insert_property(appfolio_id, address):
                stats["added"] += 1
            else:
                stats["errors"] += 1
    
    # Check for properties in Supabase that are no longer in AppFolio
    for appfolio_id, prop in supabase_properties.items():
        if appfolio_id not in appfolio_ids and prop["active"]:
            # Property no longer active in AppFolio
            log(f"Deactivating property: {prop['address']} (ID: {appfolio_id})")
            if update_property_status(appfolio_id, False):
                stats["deactivated"] += 1
            else:
                stats["errors"] += 1
    
    # Print summary
    log("=" * 60)
    log("Sync Summary:")
    log(f"  Properties added: {stats['added']}")
    log(f"  Properties reactivated: {stats['reactivated']}")
    log(f"  Properties deactivated: {stats['deactivated']}")
    log(f"  Properties unchanged: {stats['unchanged']}")
    log(f"  Errors: {stats['errors']}")
    log("=" * 60)
    log("Sync completed successfully")
    
    # Exit with error code if there were errors
    if stats["errors"] > 0:
        sys.exit(1)

if __name__ == "__main__":
    sync_properties()
