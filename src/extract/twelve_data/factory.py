

# src/extract/twelve_data/factory.py
from typing import Dict, Any, Optional
from enum import Enum

from .forex import TwelveDataForexExtractor
from .stocks import TwelveDataStockExtractor
from .crypto import TwelveDataCryptoExtractor
from .etfs import TwelveDataETFExtractor
from .etfs_indices import TwelveDataIndexExtractor
from .time_series import TwelveDataTimeSeriesExtractor
from ...utils.logger import logger


class AssetType(Enum):
    """Enum for asset types"""
    FOREX = "forex"
    STOCK = "stock"
    CRYPTO = "crypto"
    ETF = "etf"
    INDEX = "index"
    ALL = "all"


class TwelveDataExtractorFactory:
    """
    Factory for creating Twelve Data extractors
    """
    
    @staticmethod
    def create_extractor(asset_type: AssetType):
        """
        Create extractor for specific asset type
        
        Args:
            asset_type: Type of asset to extract
            
        Returns:
            Appropriate extractor instance
        """
        extractor_map = {
            AssetType.FOREX: TwelveDataForexExtractor,
            AssetType.STOCK: TwelveDataStockExtractor,
            AssetType.CRYPTO: TwelveDataCryptoExtractor,
            AssetType.ETF: TwelveDataETFExtractor,
            AssetType.INDEX: TwelveDataIndexExtractor,
            AssetType.ALL: TwelveDataTimeSeriesExtractor
        }
        
        if asset_type not in extractor_map:
            raise ValueError(f"Unsupported asset type: {asset_type}")
        
        extractor_class = extractor_map[asset_type]
        logger.info(
            f"Creating {asset_type.value} extractor",
            asset_type=asset_type.value
        )
        
        return extractor_class()
    
    @staticmethod
    def create_all_extractors() -> Dict[AssetType, Any]:
        """
        Create all extractors
        
        Returns:
            Dictionary mapping asset type to extractor
        """
        extractors = {}
        
        for asset_type in AssetType:
            if asset_type != AssetType.ALL:
                extractors[asset_type] = TwelveDataExtractorFactory.create_extractor(asset_type)
        
        logger.info(
            f"Created {len(extractors)} extractors",
            extractor_count=len(extractors),
            asset_types=list(extractors.keys())
        )
        
        return extractors