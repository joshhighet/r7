import json
import logging
from utils.exceptions import APIError, AuthenticationError, ConfigurationError
from api.client import Rapid7Client

logger = logging.getLogger(__name__)


class InsightVMCloudClient(Rapid7Client):
    """Client for InsightVM Cloud API v4"""
    
    def __init__(self, api_key, region='us', cache_manager=None):
        super().__init__(api_key, region, cache_manager)
        self.base_url = f"https://{self.region}.api.insight.rapid7.com/vm/v4/integration"
    
    def _handle_pagination(self, response_data):
        """Extract pagination metadata from response"""
        metadata = response_data.get('metadata', {})
        return {
            'cursor': metadata.get('cursor'),
            'number': metadata.get('number'),
            'size': metadata.get('size'),
            'totalResources': metadata.get('totalResources'),
            'totalPages': metadata.get('totalPages')
        }
    
    # Assets API
    def search_assets(self, cursor=None, current_time=None, comparison_time=None, 
                     size=50, asset_ids=None, site_ids=None, vuln_filters=None):
        """Search assets with filtering and pagination"""
        url = f"{self.base_url}/assets"
        
        params = {}
        if cursor:
            params['cursor'] = cursor
        if current_time:
            params['currentTime'] = current_time
        if comparison_time:
            params['comparisonTime'] = comparison_time
        if size:
            params['size'] = size
            
        # Build request body for POST
        body = {}
        if asset_ids:
            body['assetIds'] = asset_ids if isinstance(asset_ids, list) else [asset_ids]
        if site_ids:
            body['siteIds'] = site_ids if isinstance(site_ids, list) else [site_ids]
        if vuln_filters:
            body['vulnerabilityFilters'] = vuln_filters
            
        response = self.make_request("POST", url, data=body, params=params)
        if response.status_code != 200:
            raise APIError(f"Error searching assets: {response.status_code} - {response.text}")
        return response.json()
    
    def get_asset(self, asset_id):
        """Get asset details by ID"""
        url = f"{self.base_url}/assets/{asset_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching asset {asset_id}: {response.status_code} - {response.text}")
        return response.json()
    
    # Sites API
    def get_sites(self, cursor=None, page=None, size=50, include_details=False):
        """List sites"""
        url = f"{self.base_url}/sites"
        
        params = {}
        if cursor:
            params['cursor'] = cursor
        if page is not None:
            params['page'] = page
        if size:
            params['size'] = size
        if include_details:
            params['includeDetails'] = 'true'
            
        # Sites endpoint uses POST with empty body
        response = self.make_request("POST", url, data={}, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching sites: {response.status_code} - {response.text}")
        return response.json()
    
    # Scans API
    def get_scans(self, include_details=False, page=None, size=50):
        """Get scans list"""
        url = f"{self.base_url}/scan"
        
        params = {}
        if include_details:
            params['includeDetails'] = include_details
        if page is not None:
            params['page'] = page
        if size:
            params['size'] = size
            
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching scans: {response.status_code} - {response.text}")
        return response.json()
    
    def start_scan(self, site_id, scan_name=None, scan_template_id=None):
        """Start a scan"""
        url = f"{self.base_url}/scan"
        
        body = {
            "siteId": site_id
        }
        if scan_name:
            body['name'] = scan_name
        if scan_template_id:
            body['scanTemplateId'] = scan_template_id
            
        response = self.make_request("POST", url, data=body)
        if response.status_code not in (200, 201, 202):
            raise APIError(f"Error starting scan: {response.status_code} - {response.text}")
        return response.json()
    
    def get_scan(self, scan_id):
        """Get scan details"""
        url = f"{self.base_url}/scan/{scan_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching scan {scan_id}: {response.status_code} - {response.text}")
        return response.json()
    
    def stop_scan(self, scan_id):
        """Stop a running scan"""
        url = f"{self.base_url}/scan/{scan_id}/stop"
        response = self.make_request("POST", url, data={})
        if response.status_code not in (200, 202):
            raise APIError(f"Error stopping scan {scan_id}: {response.status_code} - {response.text}")
        return response.json()
    
    # Scan Engines API
    def get_scan_engines(self, page=None, size=50):
        """Get scan engines list"""
        url = f"{self.base_url}/scan/engine"
        
        params = {}
        if page is not None:
            params['page'] = page
        if size:
            params['size'] = size
            
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching scan engines: {response.status_code} - {response.text}")
        return response.json()
    
    def get_scan_engine(self, engine_id):
        """Get scan engine details"""
        url = f"{self.base_url}/scan/engine/{engine_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching scan engine {engine_id}: {response.status_code} - {response.text}")
        return response.json()
    
    def update_scan_engine_configuration(self, engine_id, config_data):
        """Update scan engine configuration"""
        url = f"{self.base_url}/scan/engine/{engine_id}/configuration"
        response = self.make_request("PUT", url, data=config_data)
        if response.status_code not in (200, 204):
            raise APIError(f"Error updating scan engine {engine_id}: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "updated"}
    
    def remove_scan_engine_configuration(self, engine_id):
        """Remove scan engine configuration"""
        url = f"{self.base_url}/scan/engine/{engine_id}/configuration"
        response = self.make_request("DELETE", url)
        if response.status_code not in (200, 204):
            raise APIError(f"Error removing scan engine configuration {engine_id}: {response.status_code} - {response.text}")
        return {"status": "removed"}
    
    # Vulnerabilities API
    def search_vulnerabilities(self, cursor=None, current_time=None, comparison_time=None, 
                              size=50, asset_ids=None, site_ids=None, vuln_ids=None):
        """Search vulnerabilities with filtering"""
        url = f"{self.base_url}/vulnerabilities"
        
        params = {}
        if cursor:
            params['cursor'] = cursor
        if current_time:
            params['currentTime'] = current_time
        if comparison_time:
            params['comparisonTime'] = comparison_time
        if size:
            params['size'] = size
            
        # Build request body for POST
        body = {}
        if asset_ids:
            body['assetIds'] = asset_ids if isinstance(asset_ids, list) else [asset_ids]
        if site_ids:
            body['siteIds'] = site_ids if isinstance(site_ids, list) else [site_ids]
        if vuln_ids:
            body['vulnerabilityIds'] = vuln_ids if isinstance(vuln_ids, list) else [vuln_ids]
            
        response = self.make_request("POST", url, data=body, params=params)
        if response.status_code != 200:
            raise APIError(f"Error searching vulnerabilities: {response.status_code} - {response.text}")
        return response.json()