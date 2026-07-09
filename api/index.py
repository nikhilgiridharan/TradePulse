"""
Vercel entry point for TradePulse FastAPI app.

Vercel's serverless Python runtime executes this file.
It needs the FastAPI app object at module level.

Path manipulation ensures src.api.main can be imported
regardless of Vercel's working directory at runtime.
"""

import sys
import os

# Get the absolute path to the project root
# __file__ is api/index.py so dirname twice gives project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add project root to path so all src.* imports resolve
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set working directory to project root so relative file paths work
# This ensures src/api/static/index.html can be found at runtime
os.chdir(project_root)

# Import the FastAPI app
from src.api.main import app

# Vercel requires the app to be named `app` or `handler` at module level
handler = app
