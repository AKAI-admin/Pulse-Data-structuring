"""
Shared MongoDB client with TLS CA bundle (fixes Windows SSL: CERTIFICATE_VERIFY_FAILED).
"""

import certifi
from pymongo import MongoClient

from config import MONGO_STANDARD_URI, MONGO_URI


def _client_kwargs():
    return {
        "serverSelectionTimeoutMS": 30000,
        "tlsCAFile": certifi.where(),
    }


def connect_mongo() -> MongoClient:
    """
    Try MONGO_URI first (often mongodb+srv), then fall back to MONGO_STANDARD_URI.
    Uses certifi for TLS verification (required on many Windows Python installs).
    """
    kwargs = _client_kwargs()
    uri = MONGO_URI.strip() if MONGO_URI else ""
    if uri:
        try:
            client = MongoClient(uri, **kwargs)
            client.admin.command("ping")
            return client
        except Exception:
            print("Primary MONGO_URI failed — retrying with standard connection string …")

    client = MongoClient(MONGO_STANDARD_URI, **kwargs)
    client.admin.command("ping")
    return client
