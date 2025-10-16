import logging

logger = logging.getLogger(__name__)

def fetch_data():
    """Fetch some test data (simulated)."""
    logger.info("Fetching data...")
    return {"value": 42, "source": "mock"}
