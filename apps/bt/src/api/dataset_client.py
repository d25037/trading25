"""API client for dataset operations (backward compatibility re-export).

The actual implementation is in src/api/dataset/ package.
This file re-exports the main class for backward compatibility.
"""

from src.api.dataset import DatasetAPIClient

__all__ = ["DatasetAPIClient"]
