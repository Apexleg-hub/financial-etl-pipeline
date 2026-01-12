# src/load/supabase_loader.py
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential
from ..utils.logger import logger
from config.settings import settings
from .data_models import BaseModel, PipelineMetadata


class SupabaseLoader:
    """Loader for Supabase database"""
    
    def __init__(self):
        self.client: Optional[Client] = None
        self._connect()
    
    def _connect(self):
        """Connect to Supabase"""
        try:
            self.client = create_client(settings.supabase_url, settings.supabase_key)
            logger.info("Connected to Supabase", url=settings.supabase_url)
        except Exception as e:
            logger.error("Failed to connect to Supabase", exc_info=e)
            raise
    
    def _serialize_data(self, data: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Convert timestamps and datetime objects to ISO format strings for JSON serialization
        
        Args:
            data: Dictionary or list of dictionaries to serialize
        
        Returns:
            Serialized data with timestamps converted to ISO format
        """
        def serialize_value(value):
            """Convert a single value to JSON-serializable format"""
            # Handle NaN and None values
            if pd.isna(value):
                return None
            elif isinstance(value, pd.Timestamp):
                return value.isoformat()
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, dict):
                return {k: serialize_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [serialize_value(v) for v in value]
            else:
                return value
        
        if isinstance(data, list):
            return [serialize_value(item) for item in data]
        else:
            return serialize_value(data)
    
    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    def upsert_data(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        conflict_columns: List[str],
        batch_size: int = None
    ) -> Dict[str, Any]:
        """
        Upsert data into Supabase table
        
        Args:
            table_name: Target table name
            data: List of records to upsert
            conflict_columns: Columns for conflict resolution
            batch_size: Batch size for insertion
        
        Returns:
            Dictionary with insertion results
        """
        if batch_size is None:
            batch_size = settings.batch_size
        
        logger.info(
            f"Upserting data to {table_name}",
            table=table_name,
            total_records=len(data),
            batch_size=batch_size,
            conflict_columns=conflict_columns
        )
        
        results = {
            "total": len(data),
            "inserted": 0,
            "updated": 0,
            "failed": 0,
            "errors": []
        }
        
        # Process in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            try:
                # Convert to DataFrame for validation
                df_batch = pd.DataFrame(batch)
                
                # Prepare data for upsert and serialize timestamps
                upsert_data = df_batch.to_dict(orient='records')
                upsert_data = self._serialize_data(upsert_data)
                
                # Execute upsert
                response = self.client.table(table_name).upsert(
                    upsert_data,
                    on_conflict=','.join(conflict_columns)
                ).execute()
                
                batch_results = len(response.data)
                results["inserted"] += batch_results
                
                logger.info(
                    f"Batch {i//batch_size + 1} upserted successfully",
                    table=table_name,
                    batch_size=len(batch),
                    results=batch_results
                )
                
            except Exception as e:
                logger.error(
                    f"Failed to upsert batch {i//batch_size + 1}",
                    exc_info=e,
                    table=table_name,
                    batch_start=i,
                    batch_end=i+batch_size
                )
                results["failed"] += len(batch)
                results["errors"].append(str(e))
        
        logger.info(
            f"Upsert completed for {table_name}",
            table=table_name,
            **results
        )
        
        return results
    
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        data_model_class: BaseModel,
        pipeline_id: str,
        run_id: str
    ) -> Dict[str, Any]:
        """
        Load DataFrame using data model
        
        Args:
            df: DataFrame to load
            data_model_class: Data model class
            pipeline_id: Pipeline identifier
            run_id: Run identifier
        
        Returns:
            Load results
        """
        logger.info(
            f"Loading {len(df)} records using {data_model_class.__name__}",
            data_model=data_model_class.__name__,
            pipeline_id=pipeline_id,
            run_id=run_id
        )
        
        # Start pipeline metadata record
        metadata = PipelineMetadata(
            pipeline_id=pipeline_id,
            run_id=run_id,
            status="loading",
            start_time=datetime.utcnow(),
            records_processed=len(df)
        )
        
        try:
            # Convert DataFrame to data model instances
            records = []
            for _, row in df.iterrows():
                try:
                    # Map DataFrame row to data model
                    record_data = {}
                    for field in data_model_class.__dataclass_fields__.keys():
                        if field in df.columns:
                            record_data[field] = row[field]
                    
                    # Add metadata
                    record_data['created_at'] = datetime.utcnow()
                    record_data['updated_at'] = datetime.utcnow()
                    record_data['source'] = pipeline_id
                    
                    # Create data model instance
                    record = data_model_class(**record_data)
                    records.append(record.to_dict())
                    
                except Exception as e:
                    logger.warning(
                        f"Failed to convert row to data model",
                        exc_info=True,
                        extra={
                            "row_index": _,
                            "data_model": data_model_class.__name__
                        }
                    )
            
            # Get table name and conflict columns from Meta class
            table_name = data_model_class.Meta.table_name
            conflict_columns = data_model_class.Meta.unique_constraint
            
            # Upsert data
            load_results = self.upsert_data(
                table_name=table_name,
                data=records,
                conflict_columns=conflict_columns
            )
            
            # Update pipeline metadata
            metadata.status = "completed"
            metadata.ended_at = datetime.utcnow()
            metadata.records_processed = load_results["inserted"]
            metadata.records_failed = load_results["failed"]
            
            if load_results["failed"] > 0:
                metadata.error_message = f"Failed to load {load_results['failed']} records"
                metadata.status = "partial_failure"
            
            # Save metadata
            self._save_pipeline_metadata(metadata)
            
            return load_results
            
        except Exception as e:
            logger.error(
                f"Failed to load data using {data_model_class.__name__}",
                exc_info=e,
                pipeline_id=pipeline_id,
                run_id=run_id
            )
            
            # Update metadata with error
            metadata.status = "failed"
            metadata.ended_at = datetime.utcnow()
            metadata.error_message = str(e)
            self._save_pipeline_metadata(metadata)
            
            raise
    
    def _save_pipeline_metadata(self, metadata: PipelineMetadata):
        """Save pipeline metadata to database"""
        try:
            metadata_dict = metadata.to_dict()
            # Serialize timestamps to ISO format
            metadata_dict = self._serialize_data(metadata_dict)
            self.client.table("pipeline_metadata").insert(metadata_dict).execute()
            logger.info(
                "Pipeline metadata saved",
                pipeline_id=metadata.pipeline_id,
                run_id=metadata.run_id,
                status=metadata.status
            )
        except Exception as e:
            logger.error(
                "Failed to save pipeline metadata",
                exc_info=e,
                pipeline_id=metadata.pipeline_id
            )
    
    def get_last_loaded_date(
        self,
        table_name: str,
        date_column: str = "date",
        filter_conditions: Optional[Dict[str, Any]] = None
    ) -> Optional[datetime]:
        """
        Get the last loaded date from a table
        
        Args:
            table_name: Table name
            date_column: Date column name
            filter_conditions: Optional filter conditions
        
        Returns:
            Last loaded date or None
        """
        try:
            query = self.client.table(table_name).select(date_column)
            
            if filter_conditions:
                for key, value in filter_conditions.items():
                    query = query.eq(key, value)
            
            query = query.order(date_column, desc=True).limit(1)
            response = query.execute()
            
            if response.data and len(response.data) > 0:
                last_date_str = response.data[0][date_column]
                return datetime.fromisoformat(last_date_str.replace('Z', '+00:00'))
            
            return None
            
        except Exception as e:
            logger.warning(
                f"Failed to get last loaded date from {table_name}",
                exc_info=True,
                extra={"table": table_name}
            )
            return None