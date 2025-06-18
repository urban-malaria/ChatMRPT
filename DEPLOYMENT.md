# ChatMRPT Server Deployment Guide

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/ChatMRPT.git
   cd ChatMRPT
   ```

2. **IMPORTANT**: Set up settlement data (required for full functionality):
   ```bash
   # Copy the kano_settlement_data folder from your local machine or backup location
   # The folder should contain: kano_settlement_data/Kano_clustered_footprint/
   # with the following files:
   # - kano_grids_clustered.dbf (321MB)
   # - kano_grids_clustered.shp (109MB)
   # - kano_grids_clustered.shx (6MB)
   # - kano_grids_clustered.prj (145B)
   
   # Option A: If you have the data on the server already
   cp -r /path/to/backup/kano_settlement_data ./
   
   # Option B: If transferring from another machine
   scp -r user@source:/path/to/kano_settlement_data ./
   ```

3. Set up Python environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

4. Set environment variables:
   ```bash
   export OPENAI_API_KEY="your-api-key"
   export FLASK_ENV="production"
   ```

5. Run the application:
   ```bash
   gunicorn 'run:app' --bind=0.0.0.0:5000
   ```

## Settlement Data Structure

The application expects settlement data at:
```
ChatMRPT/
└── kano_settlement_data/
    └── Kano_clustered_footprint/
        ├── kano_grids_clustered.dbf (321MB)
        ├── kano_grids_clustered.shp (109MB)
        ├── kano_grids_clustered.shx (6MB)
        └── kano_grids_clustered.prj (145B)
```

Total size: ~436MB

## Verification

After setup, verify settlement data is accessible:
```bash
ls -lh kano_settlement_data/Kano_clustered_footprint/
```

You should see all 4 files (.dbf, .shp, .shx, .prj) with the sizes listed above.