# src/transform/validator.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from ..utils.logger import logger
from config.settings import settings



class ValidationLevel(Enum):
    """Validation severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """Result of a validation check"""
    check_name: str
    level: ValidationLevel
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    passed: bool = True
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            "check_name": self.check_name,
            "level": self.level.value,
            "message": self.message,
            "details": self.details,
            "passed": self.passed,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class ValidationSummary:
    """Summary of validation results"""
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    warnings: int = 0
    errors: int = 0
    critical_issues: int = 0
    results: List[ValidationResult] = field(default_factory=list)
    
    def add_result(self, result: ValidationResult):
        """Add a validation result"""
        self.results.append(result)
        self.total_checks += 1
        
        if result.passed:
            self.passed_checks += 1
        else:
            self.failed_checks += 1
            
            if result.level == ValidationLevel.WARNING:
                self.warnings += 1
            elif result.level == ValidationLevel.ERROR:
                self.errors += 1
            elif result.level == ValidationLevel.CRITICAL:
                self.critical_issues += 1
    
    def is_valid(self, threshold: float = 0.95) -> bool:
        """
        Determine if data is valid based on threshold
        
        Args:
            threshold: Minimum pass rate (0-1)
        
        Returns:
            True if validation passes
        """
        if self.total_checks == 0:
            return True
        
        pass_rate = self.passed_checks / self.total_checks
        has_critical = self.critical_issues > 0
        
        return pass_rate >= threshold and not has_critical
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "warnings": self.warnings,
            "errors": self.errors,
            "critical_issues": self.critical_issues,
            "pass_rate": self.passed_checks / self.total_checks if self.total_checks > 0 else 1.0,
            "is_valid": self.is_valid(),
            "results": [r.to_dict() for r in self.results]
        }


class DataValidator:
    """Validate data quality and integrity"""
    
    def __init__(self):
        self.config = settings.load_config("sources")
        self.missing_threshold = self.config["transformation"]["missing_value_threshold"]
        self.anomaly_threshold = self.config["transformation"]["anomaly_zscore_threshold"]
        
        # Define validation schemas for different data types
        self.schemas = {
            "stock": {
                "required_columns": ["timestamp", "symbol", "open", "high", "low", "close", "volume"],
                "numeric_columns": ["open", "high", "low", "close", "volume", "adj_close"],
                "positive_columns": ["open", "high", "low", "close", "volume"],
                "range_checks": {
                    "open": {"min": 0, "max": 1000000},
                    "high": {"min": 0, "max": 1000000},
                    "low": {"min": 0, "max": 1000000},
                    "close": {"min": 0, "max": 1000000},
                    "volume": {"min": 0, "max": 1e12},
                    "adj_close": {"min": 0, "max": 1000000}
                },
                "consistency_checks": [
                    ("high >= low", "High price should be >= low price"),
                    ("high >= open", "High price should be >= open price"),
                    ("high >= close", "High price should be >= close price"),
                    ("low <= open", "Low price should be <= open price"),
                    ("low <= close", "Low price should be <= close price")
                ]
            },
            "crypto": {
                "required_columns": ["timestamp", "symbol", "open", "high", "low", "close", "volume", "exchange"],
                "numeric_columns": ["open", "high", "low", "close", "volume"],
                "positive_columns": ["open", "high", "low", "close", "volume"],
                "range_checks": {
                    "open": {"min": 0, "max": 1000000},
                    "high": {"min": 0, "max": 1000000},
                    "low": {"min": 0, "max": 1000000},
                    "close": {"min": 0, "max": 1000000},
                    "volume": {"min": 0, "max": 1e15}
                }
            },
            "forex": {
                "required_columns": ["timestamp", "symbol", "open", "high", "low", "close"],
                "numeric_columns": ["open", "high", "low", "close"],
                "positive_columns": ["open", "high", "low", "close"],
                "range_checks": {
                    "open": {"min": 0.0001, "max": 1000},
                    "high": {"min": 0.0001, "max": 1000},
                    "low": {"min": 0.0001, "max": 1000},
                    "close": {"min": 0.0001, "max": 1000}
                }
            },
            "economic": {
                "required_columns": ["timestamp", "series_id", "value"],
                "numeric_columns": ["value"],
                "range_checks": {
                    "value": {"min": -1e12, "max": 1e12}
                }
            },
            "weather": {
                "required_columns": ["timestamp", "location", "temperature", "humidity", "pressure", "wind_speed"],
                "numeric_columns": ["temperature", "humidity", "pressure", "wind_speed"],
                "range_checks": {
                    "temperature": {"min": -100, "max": 100},  # Celsius
                    "humidity": {"min": 0, "max": 100},  # Percentage
                    "pressure": {"min": 800, "max": 1100},  # hPa
                    "wind_speed": {"min": 0, "max": 150}  # m/s
                }
            },
            "sentiment": {
                "required_columns": ["timestamp", "entity", "sentiment_score", "confidence"],
                "numeric_columns": ["sentiment_score", "confidence"],
                "range_checks": {
                    "sentiment_score": {"min": -1, "max": 1},
                    "confidence": {"min": 0, "max": 1}
                }
            }
        }
    
    def validate_dataframe(
        self,
        df: pd.DataFrame,
        data_type: str,
        source: Optional[str] = None,
        validation_level: ValidationLevel = ValidationLevel.ERROR
    ) -> Tuple[pd.DataFrame, ValidationSummary]:
        """
        Validate DataFrame for quality and integrity
        
        Args:
            df: DataFrame to validate
            data_type: Type of data (stock, crypto, etc.)
            source: Data source name (optional)
            validation_level: Minimum level to consider as failure
        
        Returns:
            Tuple of (validated DataFrame, validation summary)
        """
        if df.empty:
            result = ValidationResult(
                check_name="empty_dataframe",
                level=ValidationLevel.ERROR,
                message="DataFrame is empty",
                passed=False,
                details={"data_type": data_type}
            )
            summary = ValidationSummary()
            summary.add_result(result)
            return df, summary
        
        logger.info(
            f"Starting validation for {data_type} data",
            data_type=data_type,
            source=source,
            rows=len(df),
            columns=list(df.columns)
        )
        
        summary = ValidationSummary()
        df_validated = df.copy()
        
        # Get schema for data type
        schema = self.schemas.get(data_type, {})
        
        # Run validation checks
        self._validate_schema(df_validated, schema, data_type, summary)
        self._validate_missing_values(df_validated, schema, data_type, summary)
        self._validate_data_types(df_validated, schema, data_type, summary)
        self._validate_ranges(df_validated, schema, data_type, summary)
        self._validate_consistency(df_validated, schema, data_type, summary)
        self._validate_timestamps(df_validated, data_type, summary)
        self._validate_anomalies(df_validated, schema, data_type, summary)
        self._validate_uniqueness(df_validated, data_type, summary)
        self._validate_completeness(df_validated, data_type, summary)
        
        # Determine if validation passed
        is_valid = summary.is_valid()
        
        logger.info(
            f"Validation completed for {data_type} data",
            data_type=data_type,
            total_checks=summary.total_checks,
            passed_checks=summary.passed_checks,
            failed_checks=summary.failed_checks,
            is_valid=is_valid
        )
        
        # Filter out invalid rows if validation failed
        if not is_valid and validation_level != ValidationLevel.INFO:
            df_validated = self._filter_invalid_rows(df_validated, summary)
        
        return df_validated, summary
    
    def _validate_schema(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        data_type: str,
        summary: ValidationSummary
    ):
        """Validate required columns exist"""
        required_columns = schema.get("required_columns", [])
        
        missing_columns = []
        for col in required_columns:
            if col not in df.columns:
                missing_columns.append(col)
        
        if missing_columns:
            result = ValidationResult(
                check_name="required_columns",
                level=ValidationLevel.ERROR,
                message=f"Missing required columns: {missing_columns}",
                passed=False,
                details={
                    "data_type": data_type,
                    "missing_columns": missing_columns,
                    "required_columns": required_columns
                }
            )
            summary.add_result(result)
        else:
            result = ValidationResult(
                check_name="required_columns",
                level=ValidationLevel.INFO,
                message="All required columns present",
                passed=True,
                details={
                    "data_type": data_type,
                    "required_columns": required_columns
                }
            )
            summary.add_result(result)
    
    def _validate_missing_values(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        data_type: str,
        summary: ValidationSummary
    ):
        """Validate missing values are within threshold"""
        numeric_columns = schema.get("numeric_columns", [])
        
        for col in numeric_columns:
            if col in df.columns:
                missing_count = df[col].isnull().sum()
                missing_pct = missing_count / len(df) if len(df) > 0 else 0
                
                if missing_count > 0:
                    level = ValidationLevel.WARNING if missing_pct <= self.missing_threshold else ValidationLevel.ERROR
                    
                    result = ValidationResult(
                        check_name=f"missing_values_{col}",
                        level=level,
                        message=f"Column {col} has {missing_count} missing values ({missing_pct:.1%})",
                        passed=missing_pct <= self.missing_threshold,
                        details={
                            "column": col,
                            "missing_count": missing_count,
                            "missing_percentage": missing_pct,
                            "threshold": self.missing_threshold
                        }
                    )
                    summary.add_result(result)
    
    def _validate_data_types(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        data_type: str,
        summary: ValidationSummary
    ):
        """Validate data types are correct"""
        numeric_columns = schema.get("numeric_columns", [])
        
        for col in numeric_columns:
            if col in df.columns:
                # Check if column is numeric
                if not pd.api.types.is_numeric_dtype(df[col]):
                    non_numeric_count = pd.to_numeric(df[col], errors='coerce').isnull().sum()
                    
                    result = ValidationResult(
                        check_name=f"data_type_{col}",
                        level=ValidationLevel.ERROR,
                        message=f"Column {col} has {non_numeric_count} non-numeric values",
                        passed=False,
                        details={
                            "column": col,
                            "non_numeric_count": non_numeric_count,
                            "expected_type": "numeric"
                        }
                    )
                    summary.add_result(result)
    
    def _validate_ranges(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        data_type: str,
        summary: ValidationSummary
    ):
        """Validate values are within expected ranges"""
        range_checks = schema.get("range_checks", {})
        
        for col, ranges in range_checks.items():
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                min_val = ranges.get("min")
                max_val = ranges.get("max")
                
                if min_val is not None:
                    below_min = (df[col] < min_val).sum()
                    if below_min > 0:
                        result = ValidationResult(
                            check_name=f"range_min_{col}",
                            level=ValidationLevel.ERROR,
                            message=f"Column {col} has {below_min} values below minimum {min_val}",
                            passed=False,
                            details={
                                "column": col,
                                "below_min_count": below_min,
                                "minimum": min_val
                            }
                        )
                        summary.add_result(result)
                
                if max_val is not None:
                    above_max = (df[col] > max_val).sum()
                    if above_max > 0:
                        result = ValidationResult(
                            check_name=f"range_max_{col}",
                            level=ValidationLevel.ERROR,
                            message=f"Column {col} has {above_max} values above maximum {max_val}",
                            passed=False,
                            details={
                                "column": col,
                                "above_max_count": above_max,
                                "maximum": max_val
                            }
                        )
                        summary.add_result(result)
        
        # Special checks for positive columns
        positive_columns = schema.get("positive_columns", [])
        for col in positive_columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    result = ValidationResult(
                        check_name=f"positive_values_{col}",
                        level=ValidationLevel.ERROR,
                        message=f"Column {col} has {negative_count} negative values",
                        passed=False,
                        details={
                            "column": col,
                            "negative_count": negative_count
                        }
                    )
                    summary.add_result(result)
    
    def _validate_consistency(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        data_type: str,
        summary: ValidationSummary
    ):
        """Validate consistency between related columns"""
        consistency_checks = schema.get("consistency_checks", [])
        
        for condition, description in consistency_checks:
            try:
                # Parse condition
                if ">=" in condition:
                    col1, col2 = condition.split(">=")
                    col1 = col1.strip()
                    col2 = col2.strip()
                    
                    if col1 in df.columns and col2 in df.columns:
                        violations = (df[col1] < df[col2]).sum()
                        if violations > 0:
                            result = ValidationResult(
                                check_name=f"consistency_{col1}_gte_{col2}",
                                level=ValidationLevel.ERROR,
                                message=f"{description}: {violations} violations found",
                                passed=False,
                                details={
                                    "condition": condition,
                                    "description": description,
                                    "violation_count": violations,
                                    "columns": [col1, col2]
                                }
                            )
                            summary.add_result(result)
                
                elif "<=" in condition:
                    col1, col2 = condition.split("<=")
                    col1 = col1.strip()
                    col2 = col2.strip()
                    
                    if col1 in df.columns and col2 in df.columns:
                        violations = (df[col1] > df[col2]).sum()
                        if violations > 0:
                            result = ValidationResult(
                                check_name=f"consistency_{col1}_lte_{col2}",
                                level=ValidationLevel.ERROR,
                                message=f"{description}: {violations} violations found",
                                passed=False,
                                details={
                                    "condition": condition,
                                    "description": description,
                                    "violation_count": violations,
                                    "columns": [col1, col2]
                                }
                            )
                            summary.add_result(result)
            
            except Exception as e:
                logger.warning(
                    f"Failed to evaluate consistency condition: {condition}",
                    error=str(e),
                    data_type=data_type
                )
    
    def _validate_timestamps(
        self,
        df: pd.DataFrame,
        data_type: str,
        summary: ValidationSummary
    ):
        """Validate timestamp data"""
        if 'timestamp' not in df.columns:
            return
        
        # Check for null timestamps
        null_timestamps = df['timestamp'].isnull().sum()
        if null_timestamps > 0:
            result = ValidationResult(
                check_name="null_timestamps",
                level=ValidationLevel.ERROR,
                message=f"Found {null_timestamps} null timestamps",
                passed=False,
                details={
                    "null_count": null_timestamps
                }
            )
            summary.add_result(result)
        
        # Check for future timestamps
        now = datetime.utcnow().replace(tzinfo=None)
        future_timestamps = (df['timestamp'].dt.tz_localize(None) > now + timedelta(days=1)).sum()
        if future_timestamps > 0:
            result = ValidationResult(
                check_name="future_timestamps",
                level=ValidationLevel.WARNING,
                message=f"Found {future_timestamps} timestamps in the future",
                passed=True,  # Warning, not error
                details={
                    "future_count": future_timestamps
                }
            )
            summary.add_result(result)
        
        # Check for chronological order
        if len(df) > 1:
            time_diffs = df['timestamp'].diff().dropna()
            non_chronological = (time_diffs <= pd.Timedelta(0)).sum()
            if non_chronological > 0:
                result = ValidationResult(
                    check_name="chronological_order",
                    level=ValidationLevel.WARNING,
                    message=f"Found {non_chronological} non-chronological timestamps",
                    passed=True,  # Warning, not error
                    details={
                        "non_chronological_count": non_chronological
                    }
                )
                summary.add_result(result)
    
    def _validate_anomalies(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        data_type: str,
        summary: ValidationSummary
    ):
        """Detect statistical anomalies using Z-score"""
        numeric_columns = schema.get("numeric_columns", [])
        
        for col in numeric_columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                # Remove nulls for calculation
                values = df[col].dropna()
                
                if len(values) > 2:  # Need at least 3 points for meaningful std
                    mean = values.mean()
                    std = values.std()
                    
                    if std > 0:  # Avoid division by zero
                        z_scores = np.abs((values - mean) / std)
                        anomalies = (z_scores > self.anomaly_threshold).sum()
                        
                        if anomalies > 0:
                            result = ValidationResult(
                                check_name=f"anomalies_{col}",
                                level=ValidationLevel.WARNING,
                                message=f"Found {anomalies} anomalies in column {col} (Z-score > {self.anomaly_threshold})",
                                passed=True,  # Warning, not error
                                details={
                                    "column": col,
                                    "anomaly_count": anomalies,
                                    "z_score_threshold": self.anomaly_threshold,
                                    "mean": mean,
                                    "std": std
                                }
                            )
                            summary.add_result(result)
    
    def _validate_uniqueness(
        self,
        df: pd.DataFrame,
        data_type: str,
        summary: ValidationSummary
    ):
        """Validate uniqueness of key columns"""
        # Define unique constraint columns based on data type
        unique_constraints = {
            "stock": ["timestamp", "symbol"],
            "crypto": ["timestamp", "symbol", "exchange"],
            "forex": ["timestamp", "symbol"],
            "economic": ["timestamp", "series_id"],
            "weather": ["timestamp", "location"],
            "sentiment": ["timestamp", "entity", "source"]
        }
        
        constraint_cols = unique_constraints.get(data_type, [])
        existing_cols = [col for col in constraint_cols if col in df.columns]
        
        if existing_cols and len(df) > 0:
            duplicates = df.duplicated(subset=existing_cols, keep=False).sum()
            if duplicates > 0:
                result = ValidationResult(
                    check_name="duplicate_records",
                    level=ValidationLevel.ERROR,
                    message=f"Found {duplicates} duplicate records based on {existing_cols}",
                    passed=False,
                    details={
                        "duplicate_count": duplicates,
                        "unique_constraint_columns": existing_cols
                    }
                )
                summary.add_result(result)
    
    def _validate_completeness(
        self,
        df: pd.DataFrame,
        data_type: str,
        summary: ValidationSummary
    ):
        """Validate data completeness (e.g., no gaps in time series)"""
        if 'timestamp' not in df.columns or len(df) < 2:
            return
        
        # Sort by timestamp
        df_sorted = df.sort_values('timestamp')
        
        # Calculate expected frequency
        if len(df_sorted) > 1:
            time_diffs = df_sorted['timestamp'].diff().dropna()
            if not time_diffs.empty:
                median_diff = time_diffs.median()
                
                # Identify gaps larger than 2x median
                gaps = time_diffs[time_diffs > 2 * median_diff]
                if len(gaps) > 0:
                    gap_count = len(gaps)
                    total_gap_duration = gaps.sum()
                    
                    result = ValidationResult(
                        check_name="time_gaps",
                        level=ValidationLevel.WARNING,
                        message=f"Found {gap_count} gaps in time series data",
                        passed=True,  # Warning, not error
                        details={
                            "gap_count": gap_count,
                            "total_gap_duration": str(total_gap_duration),
                            "median_interval": str(median_diff),
                            "largest_gap": str(gaps.max())
                        }
                    )
                    summary.add_result(result)
    
    def _filter_invalid_rows(
        self,
        df: pd.DataFrame,
        summary: ValidationSummary
    ) -> pd.DataFrame:
        """Filter out rows that failed critical validations"""
        if df.empty:
            return df
        
        df_filtered = df.copy()
        rows_before = len(df_filtered)
        
        # Identify critical validation failures
        critical_checks = [
            result for result in summary.results 
            if result.level in [ValidationLevel.ERROR, ValidationLevel.CRITICAL] 
            and not result.passed
        ]
        
        if not critical_checks:
            return df_filtered
        
        # Apply filters based on critical checks
        mask = pd.Series(True, index=df_filtered.index)
        
        for result in critical_checks:
            check_name = result.check_name
            
            if check_name.startswith("range_min_"):
                col = check_name.replace("range_min_", "")
                if col in df_filtered.columns:
                    min_val = result.details.get("minimum")
                    if min_val is not None:
                        mask &= (df_filtered[col] >= min_val)
            
            elif check_name.startswith("range_max_"):
                col = check_name.replace("range_max_", "")
                if col in df_filtered.columns:
                    max_val = result.details.get("maximum")
                    if max_val is not None:
                        mask &= (df_filtered[col] <= max_val)
            
            elif check_name.startswith("positive_values_"):
                col = check_name.replace("positive_values_", "")
                if col in df_filtered.columns:
                    mask &= (df_filtered[col] >= 0)
            
            elif check_name == "null_timestamps":
                mask &= df_filtered['timestamp'].notnull()
        
        # Apply mask
        df_filtered = df_filtered[mask].reset_index(drop=True)
        rows_removed = rows_before - len(df_filtered)
        
        if rows_removed > 0:
            logger.warning(
                f"Removed {rows_removed} rows that failed validation",
                rows_before=rows_before,
                rows_after=len(df_filtered),
                rows_removed=rows_removed
            )
        
        return df_filtered
    
    def generate_validation_report(
        self,
        summary: ValidationSummary,
        output_format: str = "json"
    ) -> Union[str, Dict[str, Any]]:
        """
        Generate a validation report
        
        Args:
            summary: Validation summary
            output_format: Output format (json, html, text)
        
        Returns:
            Validation report in specified format
        """
        report_data = summary.to_dict()
        
        if output_format == "json":
            return json.dumps(report_data, indent=2, default=str)
        
        elif output_format == "text":
            report_lines = [
                "=" * 60,
                "DATA VALIDATION REPORT",
                "=" * 60,
                f"Total Checks: {summary.total_checks}",
                f"Passed: {summary.passed_checks} ({summary.passed_checks/summary.total_checks:.1%})",
                f"Failed: {summary.failed_checks}",
                f"  Warnings: {summary.warnings}",
                f"  Errors: {summary.errors}",
                f"  Critical: {summary.critical_issues}",
                f"Overall Status: {'PASS' if summary.is_valid() else 'FAIL'}",
                "",
                "FAILED CHECKS:"
            ]
            
            for result in summary.results:
                if not result.passed:
                    report_lines.append(f"\n{result.level.value.upper()}: {result.check_name}")
                    report_lines.append(f"  Message: {result.message}")
                    if result.details:
                        for key, value in result.details.items():
                            report_lines.append(f"  {key}: {value}")
            
            return "\n".join(report_lines)
        
        else:
            return report_data
    
    def validate_multiple_dataframes(
        self,
        dataframes: Dict[str, pd.DataFrame],
        data_types: Dict[str, str]
    ) -> Tuple[Dict[str, pd.DataFrame], Dict[str, ValidationSummary]]:
        """
        Validate multiple DataFrames at once
        
        Args:
            dataframes: Dictionary of {name: DataFrame}
            data_types: Dictionary of {name: data_type}
        
        Returns:
            Tuple of (validated DataFrames, validation summaries)
        """
        validated = {}
        summaries = {}
        
        for name, df in dataframes.items():
            data_type = data_types.get(name, "unknown")
            
            try:
                validated_df, summary = self.validate_dataframe(df, data_type, source=name)
                validated[name] = validated_df
                summaries[name] = summary
                
                logger.info(
                    f"Validation completed for {name}",
                    name=name,
                    data_type=data_type,
                    original_rows=len(df),
                    validated_rows=len(validated_df),
                    is_valid=summary.is_valid()
                )
                
            except Exception as e:
                logger.error(
                    f"Failed to validate {name}",
                    exc_info=e,
                    name=name,
                    data_type=data_type
                )
                validated[name] = pd.DataFrame()
                summaries[name] = ValidationSummary()
        
        return validated, summaries