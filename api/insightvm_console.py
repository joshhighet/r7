import requests
import logging
import warnings
from typing import Optional, Dict, Any

from utils.exceptions import APIError, AuthenticationError

logger = logging.getLogger(__name__)


class InsightVMConsoleClient:
    """
    Minimal client for InsightVM/Nexpose Console REST API v3.

    Auth supported:
    - Basic auth (username/password)
    - API token via X-Api-Key header (optional)
    
    Base URL example: https://<console-host>:3780/api/3
    """

    def __init__(
        self,
        base_url: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_token: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        if not base_url:
            raise AuthenticationError("Console base URL is required (e.g., https://host:3780/api/3)")
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.api_token = api_token
        self.verify_ssl = verify_ssl
        self.timeout = timeout

        if not self.api_token and not (self.username and self.password):
            raise AuthenticationError("Provide either API token or username/password for console auth")

        self.session = requests.Session()
        if self.api_token:
            # Some deployments support X-Api-Key for console; include when provided
            self.session.headers.update({"X-Api-Key": self.api_token})
        if self.username and self.password:
            self.session.auth = (self.username, self.password)
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

        # If SSL verification is disabled, suppress urllib3's noisy insecure warning for this process.
        # This avoids cluttering CLI output when the user intentionally opted out of verification.
        if not self.verify_ssl:
            try:
                from urllib3.exceptions import InsecureRequestWarning
                warnings.filterwarnings("ignore", category=InsecureRequestWarning)
            except Exception:
                pass

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path if path.startswith('/') else '/' + path}"

    def _request(self, method: str, path: str, *, params: Dict[str, Any] | None = None) -> requests.Response:
        url = self._url(path)
        try:
            resp = self.session.request(method, url, params=params, timeout=self.timeout, verify=self.verify_ssl)
        except requests.exceptions.SSLError as e:
            raise APIError(f"SSL error connecting to console: {e}")
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request failed: {e}")

        logger.debug("Console API %s %s -> %s", method, url, resp.status_code)

        if resp.status_code == 401:
            raise AuthenticationError("Console authentication failed (401)")
        if resp.status_code == 403:
            raise AuthenticationError("Console access forbidden (403)")
        if resp.status_code >= 400:
            raise APIError(f"Console API error: {resp.status_code} - {resp.text}")
        return resp

    # --- Sites ---
    def list_sites(self, page: int = 0, size: int = 200) -> Dict[str, Any]:
        params = {"page": page, "size": size}
        resp = self._request("GET", "/sites", params=params)
        return resp.json()

    def get_site(self, site_id: int | str) -> Dict[str, Any]:
        resp = self._request("GET", f"/sites/{site_id}")
        return resp.json()

    # --- Assets ---
    def list_assets(self, page: int = 0, size: int = 200) -> Dict[str, Any]:
        params = {"page": page, "size": size}
        resp = self._request("GET", "/assets", params=params)
        return resp.json()

    def get_asset(self, asset_id: int | str) -> Dict[str, Any]:
        resp = self._request("GET", f"/assets/{asset_id}")
        return resp.json()

    def list_site_assets(self, site_id: int | str, page: int = 0, size: int = 200) -> Dict[str, Any]:
        params = {"page": page, "size": size}
        resp = self._request("GET", f"/sites/{site_id}/assets", params=params)
        return resp.json()
        
    def delete_asset(self, asset_id: int | str) -> Dict[str, Any]:
        """Delete an asset"""
        url = self._url(f"/assets/{asset_id}")
        
        try:
            resp = self.session.request("DELETE", url, timeout=self.timeout, verify=self.verify_ssl)
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request failed: {e}")
            
        if resp.status_code == 401:
            raise AuthenticationError("Console authentication failed (401)")
        if resp.status_code == 403:
            raise AuthenticationError("Console access forbidden (403)")
        if resp.status_code >= 400:
            raise APIError(f"Console API error: {resp.status_code} - {resp.text}")
            
        # Delete typically returns 204 No Content on success
        if resp.status_code == 204:
            return {"status": "success", "message": f"Asset {asset_id} deleted successfully"}
        
        try:
            return resp.json()
        except:
            return {"status": "success", "message": f"Asset {asset_id} deleted successfully"}

    # --- Scans ---
    def list_scans(self, page: int = 0, size: int = 200, sort: str = "endTime,desc") -> Dict[str, Any]:
        params = {"page": page, "size": size, "sort": sort}
        resp = self._request("GET", "/scans", params=params)
        return resp.json()

    def get_scan(self, scan_id: int | str) -> Dict[str, Any]:
        resp = self._request("GET", f"/scans/{scan_id}")
        return resp.json()

    def list_site_scans(self, site_id: int | str, page: int = 0, size: int = 200, sort: str = "endTime,desc") -> Dict[str, Any]:
        params = {"page": page, "size": size, "sort": sort}
        resp = self._request("GET", f"/sites/{site_id}/scans", params=params)
        return resp.json()

    def start_site_scan(self, site_id: int | str, scan_name: Optional[str] = None, 
                        template_id: Optional[str] = None, engine_id: Optional[int] = None,
                        hosts: Optional[list] = None, asset_group_ids: Optional[list] = None,
                        override_blackout: bool = False) -> Dict[str, Any]:
        """Start a scan for a site"""
        url = self._url(f"/sites/{site_id}/scans")
        
        # Build scan configuration payload
        payload = {}
        if scan_name:
            payload["name"] = scan_name
        if template_id:
            payload["templateId"] = template_id
        if engine_id:
            payload["engineId"] = engine_id
        if hosts:
            payload["hosts"] = hosts
        if asset_group_ids:
            payload["assetGroupIds"] = asset_group_ids
        if override_blackout:
            payload["overrideBlackout"] = True
            
        try:
            resp = self.session.request("POST", url, json=payload, timeout=self.timeout, verify=self.verify_ssl)
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request failed: {e}")
            
        if resp.status_code == 401:
            raise AuthenticationError("Console authentication failed (401)")
        if resp.status_code == 403:
            raise AuthenticationError("Console access forbidden (403)")
        if resp.status_code >= 400:
            raise APIError(f"Console API error: {resp.status_code} - {resp.text}")
            
        return resp.json()
        
    def update_scan_status(self, scan_id: int | str, action: str) -> Dict[str, Any]:
        """Update scan status (stop, pause, resume)"""
        url = self._url(f"/scans/{scan_id}/{action}")
        
        try:
            resp = self.session.request("POST", url, timeout=self.timeout, verify=self.verify_ssl)
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request failed: {e}")
            
        if resp.status_code == 401:
            raise AuthenticationError("Console authentication failed (401)")
        if resp.status_code == 403:
            raise AuthenticationError("Console access forbidden (403)")
        if resp.status_code >= 400:
            raise APIError(f"Console API error: {resp.status_code} - {resp.text}")
            
        # Some scan actions might return empty response with 204 No Content
        if resp.status_code == 204:
            return {"status": "success", "message": f"Scan {scan_id} {action} request successful"}
        
        try:
            return resp.json()
        except:
            return {"status": "success", "message": f"Scan {scan_id} {action} request successful"}

    # --- Vulnerabilities (definitions) ---
    def list_vulnerabilities(self, page: int = 0, size: int = 200) -> Dict[str, Any]:
        """List vulnerability definitions.

        Note: Filtering support can be added by extending params if needed.
        """
        params = {"page": page, "size": size}
        resp = self._request("GET", "/vulnerabilities", params=params)
        return resp.json()

    def get_vulnerability(self, vuln_id: str) -> Dict[str, Any]:
        """Get a vulnerability definition by ID (e.g., "ms10-001" or similar)."""
        resp = self._request("GET", f"/vulnerabilities/{vuln_id}")
        return resp.json()

    # --- Findings (per-asset vulnerabilities) ---
    def list_asset_vulnerabilities(self, asset_id: int | str, page: int = 0, size: int = 200) -> Dict[str, Any]:
        """List vulnerabilities found on a specific asset."""
        params = {"page": page, "size": size}
        resp = self._request("GET", f"/assets/{asset_id}/vulnerabilities", params=params)
        return resp.json()
