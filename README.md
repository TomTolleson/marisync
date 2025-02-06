# Marisync

Environmental Impact Analysis Platform using Databricks

## Project Structure
- `src/main/scala`: Scala source files for streaming and processing
- `src/main/python`: Python source files for ML models
- `jobs`: Databricks job configurations
- `notebooks`: Databricks notebooks

## Setup
1. Install dependencies:
   ```bash
   sbt compile
   pip install -r requirements.txt
   ```

2. Configure Databricks CLI:
   ```bash
   pip install databricks-cli
   databricks configure --token
   ```

## Configuration
1. Copy `.env.template` to `.env`:
   ```bash
   cp .env.template .env
   ```

2. Edit `.env` with your Databricks credentials:
   ```
   DATABRICKS_HOST=your-workspace-url
   DATABRICKS_TOKEN=your-token
   DATABRICKS_CONFIG_FILE=${HOME}/marisync/marisync.databrickscfg
   ```

3. Run the setup script:
   ```bash
   python scripts/setup_databricks_config.py
   ```

4. Set the config file environment variable:
   ```bash
   export DATABRICKS_CONFIG_FILE=${HOME}/marisync/marisync.databrickscfg
   ```

## Development
... 