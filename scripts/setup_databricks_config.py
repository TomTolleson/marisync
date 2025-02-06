import os
from pathlib import Path
from dotenv import load_dotenv

def setup_databricks_config():
    # Get current directory
    current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print(f"Current directory: {current_dir}")
    
    # Load .env from project root
    env_path = os.path.join(current_dir, '.env')
    print(f"Looking for .env at: {env_path}")
    
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env file not found at {env_path}")
    
    load_dotenv(env_path)
    
    # Verify we're getting the correct values
    host = os.getenv('DATABRICKS_HOST')
    token = os.getenv('DATABRICKS_TOKEN')
    
    print(f"Using host: {host}")
    print(f"Token exists: {'Yes' if token else 'No'}")
    
    if not host or not token:
        raise ValueError("Missing required environment variables")
    
    # Create .databrickscfg
    config_content = f"""[DEFAULT]
host = {host}
token = {token}
"""
    
    # Use absolute path
    config_dir = os.path.expanduser('~/marisync')
    os.makedirs(config_dir, exist_ok=True)
    
    config_path = os.path.join(config_dir, 'marisync.databrickscfg')
    
    with open(config_path, 'w') as f:
        f.write(config_content)
    
    print(f"Databricks configuration written to {config_path}")
    print("To use this config, set DATABRICKS_CONFIG_FILE environment variable:")
    print(f"export DATABRICKS_CONFIG_FILE={config_path}")

if __name__ == "__main__":
    setup_databricks_config() 