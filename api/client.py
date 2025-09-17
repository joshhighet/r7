import requests
import json
import time
import re
import logging
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from utils.exceptions import AuthenticationError, APIError, RateLimitError, QueryError, ConfigurationError
logger = logging.getLogger(__name__)
class Rapid7Client:
    def __init__(self, api_key, region='us', cache_manager=None):
        if not api_key:
            raise AuthenticationError("API key is required")
        if len(api_key.strip()) < 10:
            raise AuthenticationError("API key appears to be invalid (too short)")
        
        self.api_key = api_key.strip()
        self.region = region
        self.cache_manager = cache_manager
        self.headers = {
            'x-api-key': self.api_key,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    def get_base_url(self, product='idr'):
        """Get base URL for different Rapid7 products"""
        urls = {
            'idr': f"https://{self.region}.api.insight.rapid7.com/log_search",
            'idr_query': f"https://{self.region}.rest.logs.insight.rapid7.com",
            'idr_health': f"https://{self.region}.api.insight.rapid7.com/idr/v1",
            'asm': f"https://{self.region}.api.insight.rapid7.com/surface/graph-api/objects/table",
            'asm_apps': f"https://{self.region}.api.insight.rapid7.com/surface/apps-api",
            'asm_profile': f"https://{self.region}.api.insight.rapid7.com/surface/auth-api/profile",
            'account': f"https://{self.region}.api.insight.rapid7.com/account/api/1",
            'appsec': f"https://{self.region}.api.insight.rapid7.com/ias/v1",
            'usage': f"https://{self.region}.rest.logs.insight.rapid7.com/usage",
            'ic': f"https://{self.region}.api.insight.rapid7.com/connect",
            'vm_export': f"https://{self.region}.api.insight.rapid7.com/export/graphql"
        }
        return urls.get(product)
    def make_request(self, method, url, data=None, params=None, retries=5, timeout=30):
        """Make HTTP request with retry logic and error handling"""
        for attempt in range(retries):
            try:
                if method == 'POST':
                    response = requests.post(url, headers=self.headers, json=data,
                                           params=params, timeout=timeout)
                elif method == 'GET':
                    response = requests.get(url, headers=self.headers,
                                          params=params, timeout=timeout)
                elif method == 'PUT':
                    response = requests.put(url, headers=self.headers, json=data,
                                          params=params, timeout=timeout)
                elif method == 'DELETE':
                    response = requests.delete(url, headers=self.headers,
                                             params=params, timeout=timeout)
                elif method == 'PATCH':
                    response = requests.patch(url, headers=self.headers, json=data,
                                            params=params, timeout=timeout)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                logger.debug(f"Request: {method} {url} - Status: {response.status_code}")
                if response.status_code == 401:
                    raise AuthenticationError("Invalid API key or insufficient permissions")
                elif response.status_code == 403:
                    raise AuthenticationError("Access forbidden - check API key permissions")
                elif response.status_code == 429:
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise RateLimitError("Rate limit exceeded after maximum retries")
                elif response.status_code >= 400:
                    # Try to parse JSON error for better messaging
                    error_data = None
                    try:
                        error_data = response.json()
                        message = error_data.get('message', response.text)
                        error_msg = f"API request failed: {response.status_code} - {message}"
                    except (ValueError, KeyError):
                        error_msg = f"API request failed: {response.status_code}"
                        if response.text:
                            error_msg += f" - {response.text}"
                    
                    raise APIError(
                        error_msg,
                        status_code=response.status_code,
                        response_text=response.text,
                        error_data=error_data
                    )
                return response
            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    logger.warning(f"Request timeout. Retrying... ({attempt + 1}/{retries})")
                    continue
                else:
                    raise APIError("Request timeout after maximum retries")
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    logger.warning(f"Request failed: {e}. Retrying... ({attempt + 1}/{retries})")
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise APIError(f"Request failed: {e}")
        raise APIError("Maximum retries exceeded")
    def poll_query(self, url, show_progress=True, max_result_pages=None, query_timeout=300):
        """Poll query results with separate query completion and result pagination phases"""
        accumulated_data = {'events': [], 'statistics': None}
        start_time = time.time()
        
        if show_progress:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=None
            ) as progress:
                task = progress.add_task("Waiting for query to complete...", total=None)
                
                # Phase 1: Wait for query completion (unlimited polling with timeout)
                poll_url = url
                while True:
                    if time.time() - start_time > query_timeout:
                        progress.update(task, description=f"Query timeout after {query_timeout}s")
                        break
                        
                    response = self.make_request("GET", poll_url)
                    if response.status_code not in (200, 202):
                        raise APIError(f"Polling failed: {response.status_code} - {response.text}")
                    data = response.json()
                    logger.debug(f"Polled data: {json.dumps(data, indent=2)}")
                    
                    # Update poll URL to use the one returned by the API to maintain consistent timestamps
                    if 'links' in data and data['links']:
                        poll_url = data['links'][0]['href']
                    
                    # Check if query is still processing
                    query_progress = data.get('progress', 100)
                    if query_progress < 100 and response.status_code == 202:
                        progress.update(task, description=f"Query processing... {query_progress}%")
                        time.sleep(2)  # Wait longer for query processing
                        continue
                    
                    # Query is complete, collect first page of results
                    if 'events' in data:
                        accumulated_data['events'].extend(data['events'])
                    if 'statistics' in data:
                        accumulated_data['statistics'] = data['statistics']
                    
                    # Phase 2: Result pagination (limited by max_result_pages)
                    result_pages = 0
                    while 'links' in data and data['links'] and (max_result_pages is None or result_pages < max_result_pages):
                        result_pages += 1
                        progress.update(task, description=f"Fetching results page {result_pages}... ({len(accumulated_data['events'])} events)")
                        
                        url = data['links'][0]['href']
                        time.sleep(1)
                        
                        response = self.make_request("GET", url)
                        if response.status_code not in (200, 202):
                            raise APIError(f"Result pagination failed: {response.status_code} - {response.text}")
                        data = response.json()
                        
                        if 'events' in data:
                            accumulated_data['events'].extend(data['events'])
                        if 'statistics' in data:
                            accumulated_data['statistics'] = data['statistics']
                    
                    if max_result_pages is not None and result_pages >= max_result_pages:
                        progress.update(task, description=f"Retrieved {result_pages} result pages ({len(accumulated_data['events'])} events)")
                    else:
                        progress.update(task, description=f"Query completed! ({len(accumulated_data['events'])} events)")
                    break
        else:
            # Non-progress version with same logic
            poll_url = url
            while True:
                if time.time() - start_time > query_timeout:
                    break
                    
                response = self.make_request("GET", poll_url)
                if response.status_code not in (200, 202):
                    raise APIError(f"Polling failed: {response.status_code} - {response.text}")
                data = response.json()
                
                # Update poll URL to use the one returned by the API to maintain consistent timestamps
                if 'links' in data and data['links']:
                    poll_url = data['links'][0]['href']
                
                query_progress = data.get('progress', 100)
                if query_progress < 100 and response.status_code == 202:
                    time.sleep(2)
                    continue
                
                if 'events' in data:
                    accumulated_data['events'].extend(data['events'])
                if 'statistics' in data:
                    accumulated_data['statistics'] = data['statistics']
                
                result_pages = 0
                while 'links' in data and data['links'] and (max_result_pages is None or result_pages < max_result_pages):
                    result_pages += 1
                    url = data['links'][0]['href']
                    time.sleep(1)
                    
                    response = self.make_request("GET", url)
                    if response.status_code not in (200, 202):
                        raise APIError(f"Result pagination failed: {response.status_code} - {response.text}")
                    data = response.json()
                    
                    if 'events' in data:
                        accumulated_data['events'].extend(data['events'])
                    if 'statistics' in data:
                        accumulated_data['statistics'] = data['statistics']
                break
                
        return accumulated_data if accumulated_data['events'] or accumulated_data['statistics'] else data

    # ASM Surface Apps Methods
    def list_surface_apps(self):
        """List Surface Command apps"""
        base_url = self.get_base_url('asm_apps')
        url = f"{base_url}/apps"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching surface apps: {response.status_code} - {response.text}")
        return response.json()
    
    def test_connection(self):
        """Test API connectivity and authentication"""
        try:
            # Try a simple endpoint that should always work with valid auth
            base_url = self.get_base_url('account')
            url = f"{base_url}/organizations"
            response = self.make_request("GET", url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                # The organizations endpoint returns a list directly
                org_count = len(data) if isinstance(data, list) else len(data.get('organizations', []))
                return {
                    'success': True, 
                    'message': 'Connection successful',
                    'organizations_count': org_count
                }
            else:
                return {
                    'success': False,
                    'message': f'Connection failed: HTTP {response.status_code}'
                }
        except AuthenticationError:
            return {
                'success': False,
                'message': 'Authentication failed - check your API key'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Connection test failed: {str(e)}'
            }
    
    def is_uuid(self, value):
        """Check if value is a valid UUID"""
        return bool(re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', value, re.I))
    def get_log_id_by_name(self, name):
        """Get log ID by name"""
        cache_key = f"log_name_to_id_{name.lower()}"
        if self.cache_manager:
            cached_id = self.cache_manager.get('log_lookup', cache_key)
            if cached_id:
                return cached_id
        base_url = self.get_base_url('idr')
        url = f"{base_url}/management/logs"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching logs: {response.status_code} - {response.text}")
        logs = response.json()['logs']
        matches = [log['id'] for log in logs if log['name'].lower() == name.lower()]
        if not matches:
            raise QueryError(f"No log found with name '{name}'")
        if len(matches) > 1:
            raise QueryError(f"Multiple logs found with name '{name}'; use UUID instead")
        log_id = matches[0]
        if self.cache_manager:
            self.cache_manager.set('log_lookup', cache_key, log_id)
        return log_id

    def get_logset_id_by_name(self, name):
        """Get logset ID by name"""
        cache_key = f"logset_name_to_id_{name.lower()}"
        if self.cache_manager:
            cached_id = self.cache_manager.get('logset_lookup', cache_key)
            if cached_id:
                return cached_id
        
        base_url = self.get_base_url('idr')
        url = f"{base_url}/management/logs"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching logs: {response.status_code} - {response.text}")
        
        logs = response.json()['logs']
        logset_ids = set()
        
        # Find all logset IDs that match the name
        for log in logs:
            logsets_info = log.get('logsets_info', [])
            for logset in logsets_info:
                if logset.get('name', '').lower() == name.lower():
                    logset_ids.add(logset.get('id'))
        
        if not logset_ids:
            raise QueryError(f"No logset found with name '{name}'")
        if len(logset_ids) > 1:
            raise QueryError(f"Multiple logsets found with name '{name}'; use UUID instead")
        
        logset_id = list(logset_ids)[0]
        if self.cache_manager:
            self.cache_manager.set('logset_lookup', cache_key, logset_id)
        return logset_id


    def query_logset(self, logset_name_or_id, query, query_params, max_result_pages=None):
        """Query a logset using the /query/logsets/{id} endpoint"""
        # Resolve logset ID if name provided
        if not self.is_uuid(logset_name_or_id):
            logset_id = self.get_logset_id_by_name(logset_name_or_id)
        else:
            logset_id = logset_name_or_id
        
        base_url = self.get_base_url('idr_query')
        from urllib.parse import quote
        
        # Build URL with appropriate time parameters
        url = f"{base_url}/query/logsets/{logset_id}?"
        url_params = []
        if query:
            encoded_query = quote(query)
            url_params.append(f"query={encoded_query}")
        
        if query_params.get('time_range'):
            encoded_time = quote(query_params['time_range'])
            url_params.append(f"time_range={encoded_time}")
        elif query_params.get('from') and query_params.get('to'):
            url_params.append(f"from={query_params['from']}")
            url_params.append(f"to={query_params['to']}")
        
        url += "&".join(url_params)
        
        return self.poll_query(url, show_progress=True, max_result_pages=max_result_pages)

    def query_all_logsets(self, query, query_params, max_result_pages=None):
        """Query all logsets using the /query/logsets endpoint with multiple logset_name parameters"""
        import logging
        logger = logging.getLogger(__name__)
        
        # First, get all available logset names
        base_url = self.get_base_url('idr')
        url = f"{base_url}/management/logs"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching logs: {response.status_code} - {response.text}")
        
        logs_data = response.json()['logs']
        logset_names = set()
        
        # Extract unique logset names
        for log in logs_data:
            logsets_info = log.get('logsets_info', [])
            for logset in logsets_info:
                logset_name = logset.get('name')
                if logset_name:
                    logset_names.add(logset_name)
        
        if not logset_names:
            raise QueryError("No logsets found in organization")
        
        # Build URL with multiple logset_name parameters
        from urllib.parse import urlencode
        query_base_url = self.get_base_url('idr_query')
        
        # Create query parameters - multiple logset_name params
        params = []
        for logset_name in sorted(logset_names):  # Sort for consistent URLs
            params.append(('logset_name', logset_name))
        
        # Add time range parameters
        if query_params.get('time_range'):
            params.append(('time_range', query_params['time_range']))
        elif query_params.get('from') and query_params.get('to'):
            params.append(('from', str(query_params['from'])))
            params.append(('to', str(query_params['to'])))
        
        if query:  # Only add query param if not empty
            params.append(('query', query))
        
        query_string = urlencode(params)
        url = f"{query_base_url}/query/logsets?{query_string}"
        
        logger.info(f"query_all_logsets() - Found {len(logset_names)} logsets: {sorted(logset_names)}")
        logger.info(f"query_all_logsets() - Constructed URL: {url}")
        logger.info(f"query_all_logsets() - Query params: {query_params}")
        
        result = self.poll_query(url, show_progress=True, max_result_pages=max_result_pages)
        
        logger.info(f"query_all_logsets() - API response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
        if isinstance(result, dict):
            logger.info(f"query_all_logsets() - Events count: {len(result.get('events', []))}")
            logger.info(f"query_all_logsets() - Logs array: {result.get('logs', 'Not found')}")
        
        return result

    # AppSec Methods
    def list_apps(self):
        """List all applications"""
        base_url = self.get_base_url('appsec')
        url = f"{base_url}/apps"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching apps: {response.status_code} - {response.text}")
        return response.json()

    def get_app(self, app_id):
        """Get application details (basic)"""
        base_url = self.get_base_url('appsec')
        url = f"{base_url}/apps/{app_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching app {app_id}: {response.status_code} - {response.text}")
        return response.json()

    def list_scans(self, app_id=None, index=0, size=50):
        """List scans (optionally filtered by application) with pagination"""
        base_url = self.get_base_url('appsec')
        url = f"{base_url}/scans"
        params = {
            "index": index,
            "size": size
        }
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching scans: {response.status_code} - {response.text}")
        
        result = response.json()
        
        # Client-side filtering by app_id if provided
        if app_id and 'data' in result:
            filtered_data = []
            for scan in result['data']:
                if scan.get('app', {}).get('id') == app_id:
                    filtered_data.append(scan)
            result['data'] = filtered_data
        
        return result

    def get_scan(self, scan_id):
        """Get scan details"""
        base_url = self.get_base_url('appsec')
        url = f"{base_url}/scans/{scan_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching scan {scan_id}: {response.status_code} - {response.text}")
        return response.json()

    def search_vulnerabilities(self, query, size=50, sort=None):
        """Search for vulnerabilities using the search API"""
        base_url = self.get_base_url('appsec')
        url = f"{base_url}/search"
        
        # Build search request
        search_data = {
            "type": "VULNERABILITY",
            "query": query,
            "size": size
        }
        
        if sort:
            search_data["sort"] = sort
        
        response = self.make_request("POST", url, data=search_data)
        if response.status_code != 200:
            raise APIError(f"Error searching vulnerabilities: {response.status_code} - {response.text}")
        return response.json()

    def get_scan_vulnerabilities(self, scan_id, size=50):
        """Get vulnerabilities found in a specific scan"""
        query = f"vulnerability.scans.id = '{scan_id}'"
        sort = [{"field": "vulnerability.severity", "order": "DESC"}]
        return self.search_vulnerabilities(query, size=size, sort=sort)

    # IDR Methods
    def list_investigations(self, params=None):
        """List investigations with optional filtering"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v2/investigations"
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching investigations: {response.status_code} - {response.text}")
        return response.json()

    def get_investigation(self, investigation_id):
        """Get investigation details"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v2/investigations/{investigation_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching investigation {investigation_id}: {response.status_code} - {response.text}")
        return response.json()

    def create_investigation(self, investigation_data):
        """Create a new investigation"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v2/investigations"
        response = self.make_request("POST", url, data=investigation_data)
        if response.status_code not in (200, 201):
            raise APIError(f"Error creating investigation: {response.status_code} - {response.text}")
        return response.json()

    def set_investigation_status(self, investigation_id, status):
        """Set investigation status"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v2/investigations/{investigation_id}/status/{status}"
        response = self.make_request("PUT", url)
        if response.status_code not in (200, 204):
            raise APIError(f"Error setting investigation status: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "success"}

    def set_investigation_priority(self, investigation_id, priority):
        """Set investigation priority"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v2/investigations/{investigation_id}/priority/{priority}"
        response = self.make_request("PUT", url)
        if response.status_code not in (200, 204):
            raise APIError(f"Error setting investigation priority: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "success"}

    def assign_investigation(self, investigation_id, assignee_email):
        """Assign investigation to a user"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v2/investigations/{investigation_id}/assignee"
        data = {"user_email_address": assignee_email}
        response = self.make_request("PUT", url, data=data)
        if response.status_code not in (200, 204):
            raise APIError(f"Error assigning investigation: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "success"}

    def update_investigation(self, investigation_id, update_data, multi_customer=False):
        """Update multiple fields in a single operation for an investigation"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v2/investigations/{investigation_id}"
        
        params = {}
        if multi_customer:
            params['multi-customer'] = 'true'
        
        response = self.make_request("PATCH", url, data=update_data, params=params)
        if response.status_code not in [200, 202]:
            raise APIError(f"Error updating investigation {investigation_id}: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "success"}

    # Comment Methods
    def list_comments(self, target=None, params=None):
        """List comments with optional filtering by target"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v1/comments"
        
        query_params = params or {}
        if target:
            query_params['target'] = target
            
        response = self.make_request("GET", url, params=query_params)
        if response.status_code != 200:
            raise APIError(f"Error fetching comments: {response.status_code} - {response.text}")
        return response.json()

    def create_comment(self, target, body):
        """Create a comment for a target (investigation, etc.)"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v1/comments"
        
        data = {
            "target": target,
            "body": body
        }
        
        response = self.make_request("POST", url, data=data)
        if response.status_code not in (200, 201):
            raise APIError(f"Error creating comment: {response.status_code} - {response.text}")
        return response.json()

    def delete_comment(self, comment_rrn):
        """Delete a comment"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v1/comments/{comment_rrn}"
        response = self.make_request("DELETE", url)
        if response.status_code not in (200, 204):
            raise APIError(f"Error deleting comment: {response.status_code} - {response.text}")
        return {"status": "success", "message": f"Comment {comment_rrn} deleted successfully"}

    # Alert Methods
    def search_alerts(self, search_criteria=None, rrns_only=False, index=0, size=20, sorts=None, field_ids=None, aggregates=None):
        """Search alerts using the IDR alert search API"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/at/alerts/ops/search"
        
        # Default search criteria if none provided (searches last 30 days)
        if search_criteria is None:
            from datetime import datetime, timedelta
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=30)
            search_criteria = {
                "start_time": start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z',
                "end_time": end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'
            }
        
        # Build request body
        data = {
            "search": search_criteria,
            "sorts": sorts or [],
            "field_ids": field_ids or [],
            "aggregates": aggregates or []
        }
        
        # Build query parameters
        params = {
            "rrns_only": str(rrns_only).lower(),
            "index": index,
            "size": size
        }
        
        response = self.make_request("POST", url, data=data, params=params)
        if response.status_code != 200:
            raise APIError(f"Error searching alerts: {response.status_code} - {response.text}")
        return response.json()

    def get_alert(self, alert_rrn):
        """Get alert details by RRN"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/at/alerts/{alert_rrn}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching alert {alert_rrn}: {response.status_code} - {response.text}")
        return response.json()

    def update_alert(self, alert_rrn, update_data):
        """Update a single alert"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/at/alerts/{alert_rrn}"
        response = self.make_request("PATCH", url, data=update_data)
        return response.json() if response.text else {"status": "success"}

    def list_investigation_alerts(self, investigation_id, index=0, size=20, multi_customer=False):
        """List alerts associated with an investigation"""
        base_url = f"https://{self.region}.api.insight.rapid7.com"
        url = f"{base_url}/idr/v2/investigations/{investigation_id}/alerts"
        
        params = {
            "index": index,
            "size": size,
            "multi-customer": str(multi_customer).lower()
        }
        
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching alerts for investigation {investigation_id}: {response.status_code} - {response.text}")
        return response.json()

    # Account Management Methods
    def list_organizations(self):
        """List all organizations"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/organizations"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching organizations: {response.status_code} - {response.text}")
        return response.json()

    def list_users(self):
        """List all users"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/users"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching users: {response.status_code} - {response.text}")
        return response.json()

    def get_user(self, user_id):
        """Get specific user details"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/users/{user_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching user {user_id}: {response.status_code} - {response.text}")
        return response.json()

    def list_api_keys(self):
        """List all API keys"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/api-keys"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching API keys: {response.status_code} - {response.text}")
        return response.json()

    def create_api_key(self, name, key_type="USER", organization_id=None):
        """Generate a new API key"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/api-keys"
        
        data = {
            "name": name,
            "type": key_type
        }
        
        if organization_id:
            data["organization_id"] = organization_id
        
        response = self.make_request("POST", url, data=data)
        if response.status_code not in (200, 201):
            raise APIError(f"Error creating API key: {response.status_code} - {response.text}")
        return response.json()

    def delete_api_key(self, api_key_id):
        """Delete an API key"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/api-keys/{api_key_id}"
        response = self.make_request("DELETE", url)
        if response.status_code not in (200, 204):
            raise APIError(f"Error deleting API key: {response.status_code} - {response.text}")
        return {"status": "success", "message": f"API key {api_key_id} deleted successfully"}

    # Products Methods

    # --------------------------
    # InsightConnect (Workflows)
    # --------------------------
    def ic_list_workflows(self, limit: int = 30, offset: int = 0):
        """List InsightConnect workflows (v2)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v2/workflows"
        params = {
            "limit": max(0, min(limit, 30)),
            "offset": max(0, offset),
        }
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching workflows: {response.status_code} - {response.text}")
        return response.json()

    def ic_get_workflow(self, workflow_id: str):
        """Get a single InsightConnect workflow (v2)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v2/workflows/{workflow_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching workflow {workflow_id}: {response.status_code} - {response.text}")
        return response.json()

    # ----------------------
    # InsightConnect (Jobs)
    # ----------------------
    def ic_list_jobs(self, limit: int = 30, offset: int = 0, status: str | None = None):
        """List InsightConnect jobs (v1)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/jobs"
        params = {
            "limit": max(0, min(limit, 30)),
            "offset": max(0, offset),
        }
        if status:
            params["status"] = status
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching jobs: {response.status_code} - {response.text}")
        return response.json()

    def ic_get_job(self, job_id: str):
        """Get an InsightConnect job by ID (v1)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/jobs/{job_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching job {job_id}: {response.status_code} - {response.text}")
        return response.json()

    def ic_wait_for_job(self, job_id: str, timeout: int = 600, interval: int = 3):
        """Poll a job until it reaches a terminal state or times out.

        Returns the final job response JSON.
        """
        terminal_status = {"succeeded", "failed", "canceled", "cancelled"}
        start = time.time()
        last_status = None
        while time.time() - start < timeout:
            data = self.ic_get_job(job_id)
            try:
                job = data.get("data", {}).get("job", {})
                # some responses wrap again under job.job
                if isinstance(job, dict) and "status" not in job and "job" in job:
                    job = job.get("job", {})
                status = job.get("status")
            except Exception:
                status = None
            if status and status.lower() in terminal_status:
                return data
            if status and status != last_status:
                logger.debug(f"Job {job_id} status: {status}")
                last_status = status
            time.sleep(interval)
        raise TimeoutError(f"Timed out waiting for job {job_id} to complete")

    # ------------------------------
    # InsightConnect (Workflow Exec)
    # ------------------------------
    def ic_execute_workflow(self, workflow_id: str, body: dict | None = None):
        """Execute a workflow's active version asynchronously (v1).

        POST /connect/v1/execute/async/workflows/{workflowId}

        Args:
            workflow_id: Workflow UUID
            body: Optional request JSON to pass inputs/parameters; if None, sends an empty payload
        Returns: JSON response (typically includes job reference or error)
        """
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/execute/async/workflows/{workflow_id}"
        data = body or {}
        response = self.make_request("POST", url, data=data)
        if response.status_code not in (200, 202):
            raise APIError(f"Error executing workflow {workflow_id}: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "accepted"}

    # ------------------------------
    # InsightConnect (Workflows v2)
    # ------------------------------
    def ic_activate_workflow(self, workflow_id: str):
        """Activate a workflow (v2)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v2/workflows/{workflow_id}/activate"
        response = self.make_request("POST", url)
        if response.status_code != 200:
            raise APIError(f"Error activating workflow {workflow_id}: {response.status_code} - {response.text}")
        return response.json()

    def ic_inactivate_workflow(self, workflow_id: str):
        """Inactivate (deactivate) a workflow (v2)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        # API uses 'deactivate' for deactivation
        url = f"{base_url}/v2/workflows/{workflow_id}/deactivate"
        response = self.make_request("POST", url)
        if response.status_code != 200:
            raise APIError(f"Error deactivating workflow {workflow_id}: {response.status_code} - {response.text}")
        return response.json()

    def ic_export_workflow(self, workflow_id: str, exclude_config_details: bool = False):
        """Export a workflow definition (v2)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v2/workflows/{workflow_id}/export"
        params = {"excludeConfigDetails": bool(exclude_config_details)} if exclude_config_details else None
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error exporting workflow {workflow_id}: {response.status_code} - {response.text}")
        return response.json()

    # --------------------------------
    # InsightConnect (Global Artifacts)
    # --------------------------------
    def ic_list_global_artifacts(self, limit: int = 30, offset: int = 0, name: str | None = None, tags: list[str] | None = None):
        """List Global Artifacts (v1)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/globalArtifacts"
        params: dict[str, object] = {
            "limit": max(0, min(limit, 30)),
            "offset": max(0, offset),
        }
        if name:
            params["name"] = name
        if tags:
            # API allows multiple tags parameters; requests will handle list -> repeated params
            params["tags"] = tags
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error listing global artifacts: {response.status_code} - {response.text}")
        return response.json()

    def ic_get_global_artifact(self, artifact_id: str):
        """Get a single Global Artifact (v1)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/globalArtifacts/{artifact_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching global artifact {artifact_id}: {response.status_code} - {response.text}")
        return response.json()

    def ic_create_global_artifact(self, name: str, description: str | None = None, schema: dict | None = None, tags: list[str] | None = None):
        """Create a Global Artifact (v1)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/globalArtifacts"
        body = {
            "name": name,
            "description": description or "",
            "schema": schema or {},
            "tags": tags or [],
        }
        response = self.make_request("POST", url, data=body)
        if response.status_code != 200:
            raise APIError(f"Error creating global artifact: {response.status_code} - {response.text}")
        return response.json()

    def ic_delete_global_artifact(self, artifact_id: str):
        """Delete a Global Artifact (v1)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/globalArtifacts/{artifact_id}"
        response = self.make_request("DELETE", url)
        if response.status_code != 200:
            raise APIError(f"Error deleting global artifact {artifact_id}: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "deleted", "id": artifact_id}

    def ic_list_global_artifact_entities(self, artifact_id: str, limit: int = 30, offset: int = 0):
        """List Entities of a Global Artifact (v1)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/globalArtifacts/{artifact_id}/entities"
        params = {
            "limit": max(0, min(limit, 30)),
            "offset": max(0, offset),
        }
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error listing entities for artifact {artifact_id}: {response.status_code} - {response.text}")
        return response.json()

    def ic_add_global_artifact_entity(self, artifact_id: str, data: str):
        """Add an entity to a Global Artifact (v1).

        Args:
            artifact_id: Global Artifact UUID
            data: The string payload for the entity (per API schema)
        """
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/globalArtifacts/{artifact_id}/entities"
        body = [{"data": data}]
        response = self.make_request("POST", url, data=body)
        if response.status_code != 200:
            raise APIError(f"Error adding entity to artifact {artifact_id}: {response.status_code} - {response.text}")
        return response.json()

    def ic_delete_global_artifact_entity(self, artifact_id: str, entity_id: str):
        """Delete an entity from a Global Artifact (v1)."""
        base_url = self.get_base_url('ic')
        if not base_url:
            raise ConfigurationError("InsightConnect base URL could not be determined from region")
        url = f"{base_url}/v1/globalArtifacts/{artifact_id}/entities/{entity_id}"
        response = self.make_request("DELETE", url)
        if response.status_code != 200:
            raise APIError(f"Error deleting entity {entity_id} from artifact {artifact_id}: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "deleted", "artifact_id": artifact_id, "entity_id": entity_id}
    def list_products(self):
        """List all products"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/products"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching products: {response.status_code} - {response.text}")
        return response.json()

    def get_product(self, product_token):
        """Get product details"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/products/{product_token}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching product {product_token}: {response.status_code} - {response.text}")
        return response.json()

    def list_product_users(self, product_token):
        """List users with access to a product"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/products/{product_token}/users"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching users for product {product_token}: {response.status_code} - {response.text}")
        return response.json()

    # Roles Methods
    def list_roles(self):
        """List all roles"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/roles"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching roles: {response.status_code} - {response.text}")
        return response.json()

    def get_role(self, role_id):
        """Get role details"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/roles/{role_id}"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching role {role_id}: {response.status_code} - {response.text}")
        return response.json()

    def create_role(self, role_data):
        """Create a new role"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/roles"
        response = self.make_request("POST", url, data=role_data)
        if response.status_code not in (200, 201):
            raise APIError(f"Error creating role: {response.status_code} - {response.text}")
        return response.json()

    def update_role(self, role_id, role_data):
        """Update role attributes"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/roles/{role_id}"
        response = self.make_request("PATCH", url, data=role_data)
        if response.status_code not in (200, 204):
            raise APIError(f"Error updating role: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "success", "message": f"Role {role_id} updated successfully"}

    def delete_role(self, role_id):
        """Delete a role"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/roles/{role_id}"
        response = self.make_request("DELETE", url)
        if response.status_code not in (200, 204):
            raise APIError(f"Error deleting role: {response.status_code} - {response.text}")
        return {"status": "success", "message": f"Role {role_id} deleted successfully"}

    # Resource Groups Methods
    def list_resource_groups(self):
        """List all resource groups"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/resource-groups"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching resource groups: {response.status_code} - {response.text}")
        return response.json()

    def update_resource_group(self, resource_group_id, update_data):
        """Update resource group settings"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/resource-groups/{resource_group_id}"
        response = self.make_request("PATCH", url, data=update_data)
        if response.status_code not in (200, 204):
            raise APIError(f"Error updating resource group: {response.status_code} - {response.text}")
        return response.json() if response.text else {"status": "success", "message": f"Resource group {resource_group_id} updated successfully"}

    # Features Methods
    def list_features(self):
        """List all features and permissions"""
        base_url = self.get_base_url('account')
        url = f"{base_url}/features"
        response = self.make_request("GET", url)
        if response.status_code != 200:
            raise APIError(f"Error fetching features: {response.status_code} - {response.text}")
        return response.json()

    # Usage/Analytics Methods
    def get_total_log_usage(self, from_date, to_date):
        """Get total log data usage across all logs for date range"""
        base_url = self.get_base_url('usage')
        url = f"{base_url}/organizations"
        params = {
            'from': from_date,
            'to': to_date
        }
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching total log usage: {response.status_code} - {response.text}")
        return response.json()

    def get_log_usage_by_log(self, from_date=None, to_date=None, time_range=None):
        """Get log data usage broken down by individual logs"""
        base_url = self.get_base_url('usage')
        url = f"{base_url}/organizations/logs"
        
        params = {}
        if time_range:
            params['time_range'] = time_range
        else:
            if not from_date or not to_date:
                raise ValueError("Either time_range or both from_date and to_date must be provided")
            params['from'] = from_date
            params['to'] = to_date
            
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching log usage by log: {response.status_code} - {response.text}")
        return response.json()

    def get_specific_log_usage(self, log_key, from_date, to_date):
        """Get log data usage for a specific log"""
        base_url = self.get_base_url('usage')
        url = f"{base_url}/organizations/logs/{log_key}"
        params = {
            'from': from_date,
            'to': to_date
        }
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching usage for log {log_key}: {response.status_code} - {response.text}")
        return response.json()

    def get_health_metrics(self, size=50, index=0):
        """Get SIEM datasource health metrics"""
        base_url = self.get_base_url('idr_health')
        url = f"{base_url}/health-metrics/"
        params = {
            'size': size,
            'index': index
        }
        response = self.make_request("GET", url, params=params)
        if response.status_code != 200:
            raise APIError(f"Error fetching health metrics: {response.status_code} - {response.text}")
        return response.json()

    def get_all_health_metrics(self):
        """Get all SIEM datasource health metrics by paginating through all pages"""
        all_metrics = []
        index = 0
        size = 50

        while True:
            response = self.get_health_metrics(size=size, index=index)
            data = response.get('data', [])
            metadata = response.get('metadata', {})

            if not data:
                break

            all_metrics.extend(data)

            # Check if we've reached the end
            total_data = metadata.get('total_data', len(data))
            if len(all_metrics) >= total_data:
                break

            index += size

        return {
            'data': all_metrics,
            'metadata': {
                'total_data': len(all_metrics),
                'fetched_all': True
            }
        }


class DocsClient:
    """Client for searching Rapid7 documentation via Algolia"""
    
    def __init__(self, cache_manager=None):
        self.cache_manager = cache_manager
        # this is a search only key...
        self.algolia_app_id = "OXR3NR3FZ2"
        self.algolia_api_key = "352cb86bcf2803b74604f5e7ffb63169"
        self.base_url = f"https://{self.algolia_app_id.lower()}-dsn.algolia.net/1/indexes/*/queries"
        self.headers = {
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://docs.rapid7.com',
            'Referer': 'https://docs.rapid7.com/',
            'User-Agent': 'github.com/joshhighet/r7/1.0',
            'x-algolia-agent': 'Algolia for JavaScript (4.25.2); Browser (lite); instantsearch.js (4.79.1)',
            'x-algolia-api-key': self.algolia_api_key,
            'x-algolia-application-id': self.algolia_app_id
        }
    
    def search_docs(self, query, limit=15):
        """Search Rapid7 documentation"""
        cache_key = f"docs_search_{query.lower()}_{limit}"
        if self.cache_manager:
            cached_result = self.cache_manager.get('docs_search', cache_key)
            if cached_result:
                return cached_result
        algolia_query = {
            "requests": [{
                "indexName": "production_contentstack",
                "params": f"attributesToSnippet=[\"description\"]&facets=[\"productName\"]&filters=_content_type: 'docs'&highlightPostTag=__/ais-highlight__&highlightPreTag=__ais-highlight__&maxValuesPerFacet=30&page=0&query={query}&hitsPerPage={limit}"
            }]
        }
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=algolia_query,
                timeout=30
            )
            
            logger.debug(f"Docs search request: {response.status_code}")
            
            if response.status_code != 200:
                raise APIError(f"Docs search failed: {response.status_code} - {response.text}")
            
            data = response.json()
            results = data.get('results', [{}])[0].get('hits', [])
            
            # Process results to extract relevant information
            processed_results = []
            for hit in results:
                url = hit.get('url', '')
                if url and not url.startswith('http'):
                    if not url.startswith('/'):
                        url = '/' + url
                    url = f"https://docs.rapid7.com{url}"
                
                processed_results.append({
                    'title': hit.get('title', 'Unknown'),
                    'url': url,
                    'product': hit.get('productName', 'General'),
                    'description': hit.get('description', '')[:200] + ('...' if len(hit.get('description', '')) > 200 else ''),
                    '_highlightResult': hit.get('_highlightResult', {})
                })
            
            # Cache the result
            if self.cache_manager:
                self.cache_manager.set('docs_search', cache_key, processed_results)
            
            return processed_results
            
        except requests.exceptions.RequestException as e:
            raise APIError(f"Docs search request failed: {e}")