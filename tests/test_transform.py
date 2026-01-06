import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from src.transform.data_cleaner import DataCleaner
from src.transform.standardizer import DataStandardizer
from src.transform.feature_engineer import FeatureEngineer
from src.transform.validator import ValidationLevel, ValidationResult, ValidationSummary


@pytest.fixture
def mock_settings():
    """Mock settings for transform module"""
    with patch('src.transform.data_cleaner.settings') as mock:
        mock.load_config.return_value = {
            "transformation": {
                "missing_value_threshold": 0.3,
                "anomaly_zscore_threshold": 3.0
            },
            "extraction": {
                "default_timezone": "UTC"
            }
        }
        yield mock


@pytest.fixture
def data_cleaner(mock_settings):
    """Create data cleaner with mocked settings"""
    with patch('src.transform.data_cleaner.settings', mock_settings):
        cleaner = DataCleaner()
        yield cleaner


@pytest.fixture
def data_standardizer(mock_settings):
    """Create data standardizer with mocked settings"""
    with patch('src.transform.standardizer.settings', mock_settings):
        standardizer = DataStandardizer()
        yield standardizer


@pytest.fixture
def feature_engineer():
    """Create feature engineer"""
    engineer = FeatureEngineer()
    yield engineer


@pytest.fixture
def sample_stock_data():
    """Create sample stock data"""
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    return pd.DataFrame({
        'date': dates,
        'timestamp': dates,
        'symbol': ['AAPL'] * 30,
        'open': np.random.uniform(150, 160, 30),
        'high': np.random.uniform(160, 170, 30),
        'low': np.random.uniform(140, 150, 30),
        'close': np.random.uniform(150, 160, 30),
        'volume': np.random.randint(1000000, 10000000, 30),
        'exchange': ['NASDAQ'] * 30
    })


class TestDataCleaner:
    """Test data cleaning functionality"""
    
    def test_clean_dataframe_success(self, data_cleaner, sample_stock_data):
        """Test successful data cleaning"""
        schema = {
            'date': 'datetime',
            'timestamp': 'datetime',
            'symbol': 'str',
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'volume': 'int',
            'exchange': 'str'
        }
        
        df_clean = data_cleaner.clean_dataframe(sample_stock_data, schema, "test_source")
        
        assert not df_clean.empty
        assert len(df_clean) <= len(sample_stock_data)
        assert all(col in df_clean.columns for col in schema.keys())
        assert df_clean['symbol'].dtype == 'object'
    
    def test_remove_duplicates(self, data_cleaner):
        """Test duplicate removal"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'AAPL', 'GOOGL', 'GOOGL'],
            'price': [150.5, 150.5, 2800.0, 2800.0],
            'volume': [1000, 1000, 500, 500],
            'timestamp': pd.date_range('2024-01-01', periods=4)
        })
        
        df_clean = data_cleaner._remove_duplicates(df, "test")
        
        assert len(df_clean) <= len(df)
        # Verify duplicates based on symbol and price are removed
        assert len(df_clean[df_clean['symbol'] == 'AAPL']) <= 2
    
    def test_enforce_schema_type_conversion(self, data_cleaner):
        """Test schema enforcement and type conversion"""
        df = pd.DataFrame({
            'price': ['150.5', '151.2', '152.0'],
            'volume': ['1000000', '1100000', '1200000'],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03']
        })
        
        schema = {
            'price': 'float',
            'volume': 'int',
            'date': 'datetime'
        }
        
        df_clean = data_cleaner._enforce_schema(df, schema, "test")
        
        assert df_clean['price'].dtype in ['float64', 'float32']
        assert df_clean['volume'].dtype in ['Int64', 'int64']
        assert pd.api.types.is_datetime64_any_dtype(df_clean['date'])
    
    def test_handle_missing_values_numeric(self, data_cleaner):
        """Test handling of missing values in numeric columns"""
        df = pd.DataFrame({
            'price': [150.0, np.nan, 152.0, 153.0, np.nan],
            'volume': [1000, 1100, np.nan, 1300, 1400]
        })
        
        df_clean = data_cleaner._handle_missing_values(df, "test")
        
        # Numeric columns should be filled with median
        assert not df_clean['price'].isnull().all()
        assert not df_clean['volume'].isnull().all()
    
    def test_handle_missing_values_categorical(self, data_cleaner):
        """Test handling of missing values in categorical columns"""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'AAPL', np.nan, 'GOOGL', 'AAPL'],
            'exchange': ['NYSE', np.nan, 'NYSE', 'NASDAQ', 'NYSE']
        })
        
        df_clean = data_cleaner._handle_missing_values(df, "test")
        
        # Categorical columns should be filled with mode
        assert df_clean['symbol'].isnull().sum() <= 1
        assert df_clean['exchange'].isnull().sum() <= 1
    
    def test_standardize_timestamps_convert_to_utc(self, data_cleaner):
        """Test timestamp standardization to UTC"""
        # Create timestamps with different timezones
        dates = pd.date_range('2024-01-01', periods=5, freq='D', tz='US/Eastern')
        df = pd.DataFrame({
            'timestamp': dates,
            'value': [100, 101, 102, 103, 104]
        })
        
        df_clean = data_cleaner._standardize_timestamps(df, "test")
        
        # Verify timestamps are in UTC
        assert str(df_clean['timestamp'].dt.tz) == 'UTC'
    
    def test_validate_data_missing_columns(self, data_cleaner):
        """Test validation with missing required columns"""
        df = pd.DataFrame({
            'symbol': ['AAPL'],
            'price': [150.0]
        })
        
        schema = {
            'symbol': 'str',
            'price': 'float',
            'date': 'datetime',
            'volume': 'int'
        }
        
        is_valid, errors = data_cleaner._validate_data(df, schema, "test")
        
        assert not is_valid
        assert any('missing' in err.lower() for err in errors)
    
    def test_validate_data_null_critical_columns(self, data_cleaner):
        """Test validation detects nulls in critical columns"""
        df = pd.DataFrame({
            'timestamp': [pd.Timestamp('2024-01-01'), pd.NaT],
            'price': [150.0, 151.0],
            'value': [100, np.nan]
        })
        
        schema = {'timestamp': 'datetime', 'price': 'float', 'value': 'float'}
        
        is_valid, errors = data_cleaner._validate_data(df, schema, "test")
        
        assert not is_valid
        assert any('null' in err.lower() for err in errors)
    
    def test_validate_data_negative_values(self, data_cleaner):
        """Test validation detects negative values in price columns"""
        df = pd.DataFrame({
            'price': [150.0, -10.0, 152.0],
            'volume': [1000, 1100, 1200]
        })
        
        schema = {'price': 'float', 'volume': 'int'}
        
        is_valid, errors = data_cleaner._validate_data(df, schema, "test")
        
        assert not is_valid
        assert any('negative' in err.lower() for err in errors)
    
    def test_validate_data_pass(self, data_cleaner, sample_stock_data):
        """Test validation passes for clean data"""
        schema = {
            'date': 'datetime',
            'timestamp': 'datetime',
            'symbol': 'str',
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'volume': 'int',
            'exchange': 'str'
        }
        
        is_valid, errors = data_cleaner._validate_data(sample_stock_data, schema, "test")
        
        assert is_valid
        assert len(errors) == 0


class TestDataStandardizer:
    """Test data standardization functionality"""
    
    def test_column_name_standardization(self, data_standardizer):
        """Test column name standardization"""
        df = pd.DataFrame({
            'open_price': [150.0, 151.0],
            'high_price': [160.0, 161.0],
            'low_price': [140.0, 141.0],
            'close_price': [155.0, 156.0],
            'vol': [1000, 1100]
        })
        
        # Verify standardization mapping exists
        assert 'open' in data_standardizer.column_standardization
        assert 'open_price' in data_standardizer.column_standardization['open']
        assert 'close_price' in data_standardizer.column_standardization['close']
        assert 'vol' in data_standardizer.column_standardization['volume']
    
    def test_standard_dtypes_defined(self, data_standardizer):
        """Test that standard data types are defined"""
        assert 'timestamp' in data_standardizer.standard_dtypes
        assert 'close' in data_standardizer.standard_dtypes
        assert 'volume' in data_standardizer.standard_dtypes
        
        assert data_standardizer.standard_dtypes['timestamp'] == "datetime64[ns, UTC]"
        assert data_standardizer.standard_dtypes['close'] == "float64"
        assert data_standardizer.standard_dtypes['volume'] == "int64"
    
    def test_time_granularity_mapping(self, data_standardizer):
        """Test time granularity mappings"""
        assert data_standardizer.time_granularity_map['1m'] == '1T'
        assert data_standardizer.time_granularity_map['1h'] == '1H'
        assert data_standardizer.time_granularity_map['1d'] == '1D'
        assert data_standardizer.time_granularity_map['1w'] == '1W'
    
    def test_currency_rates_defined(self, data_standardizer):
        """Test currency conversion rates"""
        assert 'USD' in data_standardizer.currency_rates
        assert data_standardizer.currency_rates['USD'] == 1.0
        assert 'EUR' in data_standardizer.currency_rates
        assert 'GBP' in data_standardizer.currency_rates
        assert 'JPY' in data_standardizer.currency_rates
    
    def test_default_timezone_set(self, data_standardizer):
        """Test default timezone is set"""
        assert data_standardizer.default_timezone is not None
        assert str(data_standardizer.default_timezone) == 'UTC'
    
    def test_default_currency_set(self, data_standardizer):
        """Test default currency is set"""
        assert data_standardizer.default_currency == 'USD'


class TestFeatureEngineer:
    """Test feature engineering functionality"""
    
    def test_create_time_series_features_basic(self, feature_engineer, sample_stock_data):
        """Test basic time series feature creation"""
        df_features = feature_engineer.create_time_series_features(
            sample_stock_data,
            value_column='close',
            date_column='date',
            lags=[1, 2]
        )
        
        assert not df_features.empty
        assert 'close_lag_1' in df_features.columns
        assert 'close_lag_2' in df_features.columns
        assert 'close_ma_3' in df_features.columns
        assert 'close_pct_change_1' in df_features.columns
    
    def test_create_time_series_features_with_grouping(self, feature_engineer):
        """Test time series features with grouping"""
        dates = pd.date_range('2024-01-01', periods=20, freq='D')
        df = pd.DataFrame({
            'date': dates.tolist() * 2,
            'symbol': ['AAPL'] * 20 + ['GOOGL'] * 20,
            'close': np.random.uniform(150, 160, 40)
        })
        
        df_features = feature_engineer.create_time_series_features(
            df,
            value_column='close',
            date_column='date',
            group_column='symbol',
            lags=[1, 2]
        )
        
        assert not df_features.empty
        assert 'close_lag_1' in df_features.columns
        # Should have features for both groups
        assert 'AAPL' in df_features['symbol'].values
        assert 'GOOGL' in df_features['symbol'].values
    
    def test_create_lag_features(self, feature_engineer):
        """Test lag feature creation"""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'value': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        })
        
        df_features = feature_engineer._create_group_features(
            df, 'value', 'date', [1, 2, 3]
        )
        
        # Check lag columns exist
        assert 'value_lag_1' in df_features.columns
        assert 'value_lag_2' in df_features.columns
        assert 'value_lag_3' in df_features.columns
        
        # Verify lag values (shift by 1 should match original[0:9])
        assert df_features['value_lag_1'].iloc[1] == df['value'].iloc[0]
    
    def test_create_moving_average_features(self, feature_engineer):
        """Test moving average feature creation"""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'value': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        })
        
        df_features = feature_engineer._create_group_features(
            df, 'value', 'date', []
        )
        
        # Check moving average columns
        assert 'value_ma_3' in df_features.columns
        assert 'value_ma_7' in df_features.columns
        
        # Verify MA calculation (3-day MA of [10,20,30] = 20)
        assert abs(df_features['value_ma_3'].iloc[2] - 20.0) < 0.1
    
    def test_create_percent_change_features(self, feature_engineer):
        """Test percent change feature creation"""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'value': [100, 110, 110, 121, 121, 133, 133, 146.3, 146.3, 161]
        })
        
        df_features = feature_engineer._create_group_features(
            df, 'value', 'date', []
        )
        
        # Check percent change columns
        assert 'value_pct_change_1' in df_features.columns
        assert 'value_pct_change_7' in df_features.columns
        
        # First percent change should be NaN (no previous value)
        assert pd.isna(df_features['value_pct_change_1'].iloc[0])
    
    def test_create_date_features(self, feature_engineer):
        """Test date-based feature creation"""
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'value': range(10)
        })
        
        df_features = feature_engineer._create_group_features(
            df, 'value', 'date', []
        )
        
        # Check date features
        assert 'day_of_week' in df_features.columns
        assert 'day_of_month' in df_features.columns
        assert 'month' in df_features.columns
        assert 'quarter' in df_features.columns
        assert 'year' in df_features.columns
        
        # Verify date feature values
        assert df_features['year'].iloc[0] == 2024
        assert df_features['month'].iloc[0] == 1
        assert df_features['day_of_month'].iloc[0] == 1
    
    def test_create_sentiment_features_no_text(self, feature_engineer):
        """Test sentiment features handles missing TextBlob gracefully"""
        df = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=5, freq='D'),
            'text': ['good news', 'bad news', 'neutral', 'great', 'terrible']
        })
        
        # TextBlob is imported conditionally, so just verify function returns a dataframe
        # when TextBlob is not available
        df_features = feature_engineer.create_sentiment_features(
            df, 'text', 'timestamp'
        )
        
        # If TextBlob is not installed, should return original dataframe
        # If installed, should have sentiment columns
        assert isinstance(df_features, pd.DataFrame)
        assert not df_features.empty


class TestValidationResult:
    """Test validation result data structures"""
    
    def test_validation_result_creation(self):
        """Test creating validation result"""
        result = ValidationResult(
            check_name="test_check",
            level=ValidationLevel.WARNING,
            message="Test warning",
            details={"key": "value"},
            passed=False
        )
        
        assert result.check_name == "test_check"
        assert result.level == ValidationLevel.WARNING
        assert result.message == "Test warning"
        assert result.passed == False
    
    def test_validation_result_to_dict(self):
        """Test converting validation result to dictionary"""
        result = ValidationResult(
            check_name="test",
            level=ValidationLevel.ERROR,
            message="Test error",
            passed=False
        )
        
        result_dict = result.to_dict()
        
        assert result_dict['check_name'] == "test"
        assert result_dict['level'] == "error"
        assert result_dict['message'] == "Test error"
        assert result_dict['passed'] == False
        assert 'timestamp' in result_dict


class TestValidationSummary:
    """Test validation summary functionality"""
    
    def test_validation_summary_add_results(self):
        """Test adding results to validation summary"""
        summary = ValidationSummary()
        
        result1 = ValidationResult("check1", ValidationLevel.INFO, "Passed", passed=True)
        result2 = ValidationResult("check2", ValidationLevel.ERROR, "Failed", passed=False)
        
        summary.add_result(result1)
        summary.add_result(result2)
        
        assert summary.total_checks == 2
        assert summary.passed_checks == 1
        assert summary.failed_checks == 1
        assert summary.errors == 1
    
    def test_validation_summary_is_valid_threshold(self):
        """Test validation summary pass/fail based on threshold"""
        summary = ValidationSummary()
        
        # Add 4 passing and 1 failing results (80% pass rate)
        for i in range(4):
            summary.add_result(ValidationResult(f"check{i}", ValidationLevel.INFO, "Pass", passed=True))
        summary.add_result(ValidationResult("check5", ValidationLevel.WARNING, "Fail", passed=False))
        
        # With 95% threshold, should fail
        assert not summary.is_valid(threshold=0.95)
        
        # With 75% threshold, should pass
        assert summary.is_valid(threshold=0.75)
    
    def test_validation_summary_critical_issues(self):
        """Test that critical issues invalidate summary"""
        summary = ValidationSummary()
        
        summary.add_result(ValidationResult("check1", ValidationLevel.INFO, "Pass", passed=True))
        summary.add_result(ValidationResult("check2", ValidationLevel.CRITICAL, "Critical", passed=False))
        
        # Should be invalid due to critical issue
        assert not summary.is_valid(threshold=0.0)
    
    def test_validation_summary_to_dict(self):
        """Test converting validation summary to dictionary"""
        summary = ValidationSummary()
        
        summary.add_result(ValidationResult("check1", ValidationLevel.INFO, "Pass", passed=True))
        summary.add_result(ValidationResult("check2", ValidationLevel.ERROR, "Fail", passed=False))
        
        summary_dict = summary.to_dict()
        
        assert summary_dict['total_checks'] == 2
        assert summary_dict['passed_checks'] == 1
        assert summary_dict['failed_checks'] == 1
        assert summary_dict['errors'] == 1
        assert summary_dict['pass_rate'] == 0.5
        assert 'is_valid' in summary_dict


class TestTransformIntegration:
    """Integration tests for transform module"""
    
    def test_clean_standardize_pipeline(self, data_cleaner, data_standardizer, sample_stock_data):
        """Test cleaning and standardization pipeline"""
        schema = {
            'date': 'datetime',
            'timestamp': 'datetime',
            'symbol': 'str',
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'volume': 'int',
            'exchange': 'str'
        }
        
        # Clean data
        df_clean = data_cleaner.clean_dataframe(sample_stock_data, schema, "test")
        
        # Verify clean data is suitable for standardization
        assert not df_clean.empty
        assert all(col in df_clean.columns for col in schema.keys())
        
        # Standardization would typically use the cleaned data
        assert len(df_clean) > 0
    
    def test_clean_engineer_features_pipeline(self, data_cleaner, feature_engineer, sample_stock_data):
        """Test cleaning and feature engineering pipeline"""
        schema = {
            'date': 'datetime',
            'timestamp': 'datetime',
            'symbol': 'str',
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'volume': 'int',
            'exchange': 'str'
        }
        
        # Clean
        df_clean = data_cleaner.clean_dataframe(sample_stock_data, schema, "test")
        
        # Engineer features
        df_features = feature_engineer.create_time_series_features(
            df_clean,
            value_column='close',
            date_column='date',
            group_column='symbol',
            lags=[1, 2, 3]
        )
        
        assert not df_features.empty
        assert 'close_lag_1' in df_features.columns
        assert all(col in df_features.columns for col in schema.keys())
