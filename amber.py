import requests
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin

class AmberClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/') + '/'
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = urljoin(self.base_url, path.lstrip('/'))
        r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def sites(self) -> List[Dict[str, Any]]:
        # Expected path (based on docs/community refs): /sites
        return self._get("sites")

    def usage(self, site_id: str, **params) -> Any:
        # Expected path: /sites/{siteId}/usage
        return self._get(f"sites/{site_id}/usage", params=params)

    def prices(self, site_id: Optional[str] = None, **params) -> Any:
        # Some deployments expose site-scoped prices; others region-scoped.
        # Try site-scoped first if provided.
        if site_id:
            return self._get(f"sites/{site_id}/prices", params=params)
        # Fallback (if supported by your token): /prices
        return self._get("prices", params=params)
