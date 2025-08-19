class Rapid7Error(Exception):
    """Base exception for all Rapid7 API errors"""
    pass
class AuthenticationError(Rapid7Error):
    """Raised when authentication fails"""
    pass
class APIError(Rapid7Error):
    """Raised when API returns an error response"""
    def __init__(self, message, status_code=None, response_text=None, error_data=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text
        self.error_data = error_data
class ConfigurationError(Rapid7Error):
    """Raised when configuration is invalid"""
    pass
class QueryError(Rapid7Error):
    """Raised when query execution fails"""
    pass
class RateLimitError(Rapid7Error):
    """Raised when rate limit is exceeded"""
    pass