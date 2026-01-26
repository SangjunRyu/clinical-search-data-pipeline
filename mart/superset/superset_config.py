"""
Superset Configuration
"""

import os

# =========================
# Basic Settings
# =========================
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# =========================
# Secret Key (반드시 변경!)
# =========================
SECRET_KEY = os.getenv(
    "SUPERSET_SECRET_KEY",
    "tripclick-superset-secret-key-change-in-production"
)

# =========================
# Database
# =========================
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://"
    f"{os.getenv('DATABASE_USER', 'superset')}:"
    f"{os.getenv('DATABASE_PASSWORD', 'superset')}@"
    f"{os.getenv('DATABASE_HOST', 'superset-db')}:"
    f"{os.getenv('DATABASE_PORT', '5432')}/"
    f"{os.getenv('DATABASE_DB', 'superset')}"
)

# =========================
# Feature Flags
# =========================
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# =========================
# Cache
# =========================
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# =========================
# Security
# =========================
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # 프로덕션에서는 True로 설정

# =========================
# Logging
# =========================
ENABLE_TIME_ROTATE = True
TIME_ROTATE_LOG_LEVEL = "INFO"
