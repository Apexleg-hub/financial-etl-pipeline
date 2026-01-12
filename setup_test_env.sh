

#!/bin/bash
# setup_test_env.sh

echo "Setting up test environment for Twelve Data extractors..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt
pip install pytest pytest-mock pytest-cov pandas numpy python-dotenv

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cat > .env << EOF
# Twelve Data API Key
# Get your API key from: https://twelvedata.com/apikey
TWELVE_DATA_API_KEY=your_api_key_here

# Other environment variables
LOG_LEVEL=INFO
TIMEZONE=UTC
EOF
    echo "  Please update .env file with your actual API key"
fi

# Create test data directory
mkdir -p test_data

echo " Setup complete!"
echo ""
echo "To run tests:"
echo "1. Update .env file with your API key"
echo "2. Run quick test: python scripts/quick_test.py"
echo "3. Run all tests: pytest tests/"
echo "4. Run end-to-end test: python scripts/test_twelve_data_end_to_end.py"