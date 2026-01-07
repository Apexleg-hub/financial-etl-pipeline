

#!/bin/bash
# CI/CD Setup Script

set -e

echo " Setting up CI/CD for Financial ETL Pipeline..."

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed."; exit 1; }

# Create necessary directories
echo " Creating directories..."
mkdir -p .github/workflows
mkdir -p .gitlab
mkdir -p scripts
mkdir -p k8s
mkdir -p tests/unit tests/integration

# Copy CI/CD templates
echo " Copying CI/CD templates..."
cp .github/workflows/ci.yml.example .github/workflows/ci.yml 2>/dev/null || true
cp .gitlab-ci.yml.example .gitlab-ci.yml 2>/dev/null || true

# Set up Python virtual environment
echo " Setting up Python environment..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip

# Install development dependencies
echo " Installing dependencies..."
pip install -r requirements-dev.txt

# Install pre-commit hooks
echo " Installing pre-commit hooks..."
pre-commit install

# Set up test environment
echo " Setting up test environment..."
cp .env.example .env.test
echo "ENVIRONMENT=test" >> .env.test
echo "LOG_LEVEL=DEBUG" >> .env.test

# Initialize Git hooks
echo " Setting up Git hooks..."
chmod +x scripts/setup-git-hooks.sh
./scripts/setup-git-hooks.sh

echo " CI/CD setup completed!"
echo ""
echo "Next steps:"
echo "1. Configure secrets in your CI/CD platform"
echo "2. Update .github/workflows/ci.yml with your registry info"
echo "3. Run 'make ci-local' to test locally"
echo "4. Push to GitHub/GitLab to trigger CI/CD"