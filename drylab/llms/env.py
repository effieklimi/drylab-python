# drylab/llms/google/env.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get environment variables
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# Optional: Add validation
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable is not set")