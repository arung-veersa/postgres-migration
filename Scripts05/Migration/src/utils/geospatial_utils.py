"""
Geospatial utility functions for distance calculations.

Provides methods for calculating distances between coordinates
and ETA travel times based on distance and speed.
"""

import pandas as pd
import numpy as np
from typing import Optional


class GeospatialUtils:
    """Utilities for geospatial calculations."""
    
    METERS_TO_MILES = 1609.34  # 1 mile = 1609.34 meters
    
    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great circle distance between two points on Earth.
        
        Uses the Haversine formula to calculate distance in miles.
        
        Args:
            lat1: Latitude of point 1
            lon1: Longitude of point 1
            lat2: Latitude of point 2
            lon2: Longitude of point 2
            
        Returns:
            Distance in miles
        """
        if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
            return None
        
        # Convert to radians
        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        lon1_rad = np.radians(lon1)
        lon2_rad = np.radians(lon2)
        
        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        # Earth's radius in miles
        earth_radius_miles = 3958.8
        
        distance = earth_radius_miles * c
        return distance
    
    @staticmethod
    def calculate_distance_vectorized(df: pd.DataFrame, 
                                     lat1_col: str, lon1_col: str,
                                     lat2_col: str, lon2_col: str,
                                     extra_distance_pct: float = 1.0) -> pd.Series:
        """
        Calculate distances for all rows in a DataFrame (vectorized).
        
        Args:
            df: DataFrame with coordinate columns
            lat1_col: Column name for latitude 1
            lon1_col: Column name for longitude 1
            lat2_col: Column name for latitude 2
            lon2_col: Column name for longitude 2
            extra_distance_pct: Extra distance percentage multiplier (default 1.0)
            
        Returns:
            Series with distances in miles
        """
        # Convert to radians
        lat1 = np.radians(df[lat1_col])
        lat2 = np.radians(df[lat2_col])
        lon1 = np.radians(df[lon1_col])
        lon2 = np.radians(df[lon2_col])
        
        # Haversine formula (vectorized)
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        # Earth's radius in miles
        earth_radius_miles = 3958.8
        
        distance = earth_radius_miles * c * extra_distance_pct
        
        return distance.round(2)
    
    @staticmethod
    def calculate_eta_minutes(distance_miles: float, mph: float) -> Optional[float]:
        """
        Calculate ETA in minutes based on distance and speed.
        
        Args:
            distance_miles: Distance in miles
            mph: Average miles per hour
            
        Returns:
            ETA in minutes, or None if invalid inputs
        """
        if pd.isna(distance_miles) or pd.isna(mph) or mph == 0:
            return None
        
        hours = distance_miles / mph
        minutes = hours * 60
        return round(minutes, 2)
    
    @staticmethod
    def calculate_eta_vectorized(distance_series: pd.Series, mph_series: pd.Series) -> pd.Series:
        """
        Calculate ETA in minutes for all rows (vectorized).
        
        Args:
            distance_series: Series with distances in miles
            mph_series: Series with average miles per hour
            
        Returns:
            Series with ETA in minutes
        """
        # Avoid division by zero
        valid_mask = (mph_series.notna()) & (mph_series != 0)
        
        eta = pd.Series(index=distance_series.index, dtype=float)
        eta[valid_mask] = ((distance_series[valid_mask] / mph_series[valid_mask]) * 60).round(2)
        
        return eta
    
    @staticmethod
    def lookup_mph(distance: float, mph_df: pd.DataFrame) -> Optional[float]:
        """
        Look up average MPH based on distance from MPH lookup table.
        
        Args:
            distance: Distance in miles
            mph_df: DataFrame with columns "From", "To", "AverageMilesPerHour"
            
        Returns:
            Average MPH, or None if not found
        """
        if pd.isna(distance) or mph_df.empty:
            return None
        
        # Find matching row where distance is between From and To
        match = mph_df[(mph_df['From'] <= distance) & (mph_df['To'] >= distance)]
        
        if not match.empty:
            return match.iloc[0]['AverageMilesPerHour']
        
        return None

