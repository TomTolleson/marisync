import os
from dotenv import load_dotenv

def load_databricks_config():
    load_dotenv()
    
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return {
        'host': os.getenv('DATABRICKS_HOST'),
        'token': os.getenv('DATABRICKS_TOKEN')
    } 