

# start.ps1
$ErrorActionPreference = "Stop"

Write-Host "Setting up Financial ETL Pipeline..." -ForegroundColor Green

# Create .env file with proper encoding
$envContent = @"
AIRFLOW_FERNET_KEY=ipLk01n0-IlCqnxJfpDG9ZQeELIWH8Um_OiKEFOJqFM=
ALPHA_VANTAGE_API_KEY=demo
SUPABASE_DB_PASSWORD=temp
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow
"@

[System.IO.File]::WriteAllText("$pwd\.env", $envContent, [System.Text.Encoding]::UTF8)

Write-Host "Starting Docker containers..." -ForegroundColor Yellow
docker compose up -d

Write-Host "Waiting for services to start..." -ForegroundColor Cyan
Start-Sleep -Seconds 30

Write-Host "Creating Airflow admin user..." -ForegroundColor Yellow
docker compose exec airflow-webserver airflow users create `
    --username admin `
    --password admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@example.com `
    2>$null

Write-Host "`nAirflow is running at: http://localhost:8080" -ForegroundColor Green
Write-Host "Username: admin" -ForegroundColor Green
Write-Host "Password: admin" -ForegroundColor Green