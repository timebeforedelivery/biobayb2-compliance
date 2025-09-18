# BioBAYB2 Compliance -- Docker Setup

This runs a **Marimo** notebook that serves `participation_nb.py` on http://localhost:2718.

## Files
- `Dockerfile`, `docker-compose.yml`, `requirements.txt`
- `participation_nb.py`, `stage_calculation.py`
- `.cache/` (bind-mounted; persisted cache)

## Prereqs
- Docker + Docker Compose
- `UHKEY` for Ultrahuman API (if used)
- AWS creds if the notebook uses AWS (Athena/S3)
- MDH creds

## Quick start
1) Export env variables:
```bash
export UHKEY=${UHKEY}
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}
export MDH_SECRET_KEY=${MDH_SECRET_KEY}
export MDH_ACCOUNT_NAME=${MDH_ACCOUNT_NAME}
export MDH_SECRET_KEY=${MDH_SECRET_KEY}
export MDH_PROJECT_NAME=${MDH_PROJECT_NAME}
export MDH_PROJECT_ID=${MDH_PROJECT_ID}
export AWS_PROFILE_NAME=${AWS_PROFILE_NAME}
export UH_API_CALL=${UH_API_CALL} # will make requests to UH API if set, AWS otherwise.
export SEGMENT_ID=${MDH_SEGMENT_ID}
export AWS_BIOBAYB_DB_NAME=${AWS_BIOBAYB_DB_NAME}
export AWS_BIOBAYB_S3_LOCATION=${AWS_BIOBAYB_S3_LOCATION}
export AWS_BIOBAYB_WORKGROUP=${AWS_BIOBAYB_WORKGROUP}

```

2) Build & run:
```bash
docker compose up --build
```

3) Open: http://localhost:2718
<img width="1255" height="1352" alt="Screenshot 2025-09-18 at 10-24-14 participation nb" src="https://github.com/user-attachments/assets/c8531d42-0634-49b5-9e57-4654b0ac67a1" />


5) Stop:
```bash
docker compose down
```

