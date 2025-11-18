"""
Conflict detection rules for visit conflicts.

Implements the 7 conflict detection rules used to identify
scheduling conflicts between visits.
"""

import pandas as pd
import numpy as np
from typing import Optional


class ConflictRules:
    """
    Conflict detection rules.
    
    Implements 7 rules for detecting conflicts between visits:
    1. Same Scheduled Time
    2. Same Visit Time
    3. Schedule Time Same as Visit Time
    4. Schedule Overlaps Another Schedule
    5. Visit Time Overlaps Another Visit Time
    6. Schedule Time Overlaps Visit Time
    7. Distance Flag (impossible travel distance)
    """
    
    @staticmethod
    def rule_1_same_sch_time(v1_df: pd.DataFrame, v2_df: pd.DataFrame) -> pd.Series:
        """
        Rule 1: Same Scheduled Time for different providers.
        
        Conditions:
        - Both visits have NO visit times (scheduled only)
        - Scheduled start and end times are identical
        - Different providers
        
        Returns:
            Series with 'Y' or 'N' values
        """
        same_time = (
            v1_df['VisitStartTime'].isna() &
            v1_df['VisitEndTime'].isna() &
            v2_df['VisitStartTime'].isna() &
            v2_df['VisitEndTime'].isna() &
            (v1_df['SchStartTime'] == v2_df['SchStartTime']) &
            (v1_df['SchEndTime'] == v2_df['SchEndTime'])
        )
        
        return np.where(same_time, 'Y', 'N')
    
    @staticmethod
    def rule_2_same_visit_time(v1_df: pd.DataFrame, v2_df: pd.DataFrame) -> pd.Series:
        """
        Rule 2: Same Visit Time (actual visit times match).
        
        Conditions:
        - Both visits have visit times (actuals recorded)
        - Visit start and end times are identical
        - Different providers
        
        Returns:
            Series with 'Y' or 'N' values
        """
        same_time = (
            v1_df['VisitStartTime'].notna() &
            v1_df['VisitEndTime'].notna() &
            v2_df['VisitStartTime'].notna() &
            v2_df['VisitEndTime'].notna() &
            (v1_df['VisitStartTime'] == v2_df['VisitStartTime']) &
            (v1_df['VisitEndTime'] == v2_df['VisitEndTime'])
        )
        
        return np.where(same_time, 'Y', 'N')
    
    @staticmethod
    def rule_3_sch_visit_time_same(v1_df: pd.DataFrame, v2_df: pd.DataFrame) -> pd.Series:
        """
        Rule 3: Schedule time of one visit matches visit time of another.
        
        Conditions:
        - One visit has only schedule, other has visit times
        - Schedule times match the visit times
        - Different providers
        
        Returns:
            Series with 'Y' or 'N' values
        """
        # V2 has visit times, V1 has only schedule, and they match
        v2_visit_v1_sch = (
            v2_df['VisitStartTime'].notna() &
            v2_df['VisitEndTime'].notna() &
            v1_df['VisitStartTime'].isna() &
            v1_df['VisitEndTime'].isna() &
            (v1_df['SchStartTime'] == v2_df['VisitStartTime']) &
            (v1_df['SchEndTime'] == v2_df['VisitEndTime'])
        )
        
        # V1 has visit times, V2 has only schedule, and they match
        v1_visit_v2_sch = (
            v1_df['VisitStartTime'].notna() &
            v1_df['VisitEndTime'].notna() &
            v2_df['VisitStartTime'].isna() &
            v2_df['VisitEndTime'].isna() &
            (v2_df['SchStartTime'] == v1_df['VisitStartTime']) &
            (v2_df['SchEndTime'] == v1_df['VisitEndTime'])
        )
        
        return np.where(v2_visit_v1_sch | v1_visit_v2_sch, 'Y', 'N')
    
    @staticmethod
    def rule_4_sch_overlap_sch(v1_df: pd.DataFrame, v2_df: pd.DataFrame) -> pd.Series:
        """
        Rule 4: Schedule overlaps another schedule (but not exact match).
        
        Conditions:
        - Both visits have only schedules (no visit times)
        - Schedule times overlap but are not identical
        - Different providers
        
        Returns:
            Series with 'Y' or 'N' values
        """
        overlap = (
            v1_df['VisitStartTime'].isna() &
            v1_df['VisitEndTime'].isna() &
            v2_df['VisitStartTime'].isna() &
            v2_df['VisitEndTime'].isna() &
            (v1_df['SchStartTime'] < v2_df['SchEndTime']) &
            (v1_df['SchEndTime'] > v2_df['SchStartTime']) &
            ~((v1_df['SchStartTime'] == v2_df['SchStartTime']) & 
              (v1_df['SchEndTime'] == v2_df['SchEndTime']))
        )
        
        return np.where(overlap, 'Y', 'N')
    
    @staticmethod
    def rule_5_visit_overlap_visit(v1_df: pd.DataFrame, v2_df: pd.DataFrame) -> pd.Series:
        """
        Rule 5: Visit time overlaps another visit time (but not exact match).
        
        Conditions:
        - Both visits have visit times
        - Visit times overlap but are not identical
        - Different providers
        
        Returns:
            Series with 'Y' or 'N' values
        """
        overlap = (
            v1_df['VisitStartTime'].notna() &
            v1_df['VisitEndTime'].notna() &
            v2_df['VisitStartTime'].notna() &
            v2_df['VisitEndTime'].notna() &
            (v1_df['VisitStartTime'] < v2_df['VisitEndTime']) &
            (v1_df['VisitEndTime'] > v2_df['VisitStartTime']) &
            ~((v1_df['VisitStartTime'] == v2_df['VisitStartTime']) & 
              (v1_df['VisitEndTime'] == v2_df['VisitEndTime']))
        )
        
        return np.where(overlap, 'Y', 'N')
    
    @staticmethod
    def rule_6_sch_overlap_visit(v1_df: pd.DataFrame, v2_df: pd.DataFrame) -> pd.Series:
        """
        Rule 6: Schedule time overlaps visit time (but not exact match).
        
        Conditions:
        - One visit has schedule only, other has visit times
        - Times overlap but are not identical
        - Different providers
        
        Returns:
            Series with 'Y' or 'N' values
        """
        # V1 schedule overlaps V2 visit time
        v1_sch_v2_visit = (
            v1_df['VisitStartTime'].isna() &
            v1_df['VisitEndTime'].isna() &
            v2_df['VisitStartTime'].notna() &
            v2_df['VisitEndTime'].notna() &
            (v1_df['SchStartTime'] < v2_df['VisitEndTime']) &
            (v1_df['SchEndTime'] > v2_df['VisitStartTime']) &
            ~((v1_df['SchStartTime'] == v2_df['VisitStartTime']) & 
              (v1_df['SchEndTime'] == v2_df['VisitEndTime']))
        )
        
        # V2 schedule overlaps V1 visit time
        v2_sch_v1_visit = (
            v2_df['VisitStartTime'].isna() &
            v2_df['VisitEndTime'].isna() &
            v1_df['VisitStartTime'].notna() &
            v1_df['VisitEndTime'].notna() &
            (v2_df['SchStartTime'] < v1_df['VisitEndTime']) &
            (v2_df['SchEndTime'] > v1_df['VisitStartTime']) &
            ~((v2_df['SchStartTime'] == v1_df['VisitStartTime']) & 
              (v2_df['SchEndTime'] == v1_df['VisitEndTime']))
        )
        
        return np.where(v1_sch_v2_visit | v2_sch_v1_visit, 'Y', 'N')
    
    @staticmethod
    def rule_7_distance_flag(merged_df: pd.DataFrame, 
                            extra_distance_pct: float,
                            mph_df: pd.DataFrame) -> pd.Series:
        """
        Rule 7: Distance flag - impossible to travel between locations in time.
        
        Conditions:
        - Both visits have coordinates and visit times
        - Different zip codes (or at least one is null)
        - Time available to travel < time required to travel
        - Different providers
        
        Returns:
            Series with 'Y' or 'N' values
        """
        from src.utils.geospatial_utils import GeospatialUtils
        
        # Base conditions
        has_coordinates = (
            merged_df['PLongitude'].notna() &
            merged_df['PLatitude'].notna() &
            merged_df['ConPLongitude'].notna() &
            merged_df['ConPLatitude'].notna()
        )
        
        has_visit_times = (
            merged_df['VisitStartTime'].notna() &
            merged_df['VisitEndTime'].notna() &
            merged_df['ConVisitStartTime'].notna() &
            merged_df['ConVisitEndTime'].notna()
        )
        
        different_zip = (
            (merged_df['PZipCode'].isna() | merged_df['ConPZipCode'].isna()) |
            (merged_df['PZipCode'] != merged_df['ConPZipCode'])
        )
        
        # Calculate distances
        distance = GeospatialUtils.calculate_distance_vectorized(
            merged_df,
            'PLatitude', 'PLongitude',
            'ConPLatitude', 'ConPLongitude',
            extra_distance_pct
        )
        
        # Lookup MPH for each distance (convert Decimal to float)
        def lookup_mph_for_distance(dist):
            if pd.isna(dist):
                return None
            match = mph_df[(mph_df['From'] <= dist) & (mph_df['To'] >= dist)]
            if not match.empty:
                return float(match.iloc[0]['AverageMilesPerHour'])
            return None
        
        mph_values = distance.apply(lookup_mph_for_distance)
        
        # Calculate ETA in minutes
        eta = GeospatialUtils.calculate_eta_vectorized(distance, mph_values)
        
        # Calculate time available between visits (in minutes)
        time_v1_to_v2 = (merged_df['ConVisitStartTime'] - merged_df['VisitEndTime']).dt.total_seconds() / 60
        time_v2_to_v1 = (merged_df['VisitStartTime'] - merged_df['ConVisitEndTime']).dt.total_seconds() / 60
        
        # Check if either direction is impossible
        impossible_v1_to_v2 = (time_v1_to_v2 > 0) & (eta > time_v1_to_v2)
        impossible_v2_to_v1 = (time_v2_to_v1 > 0) & (eta > time_v2_to_v1)
        
        distance_flag = (
            has_coordinates &
            has_visit_times &
            different_zip &
            mph_values.notna() &
            (impossible_v1_to_v2 | impossible_v2_to_v1)
        )
        
        return np.where(distance_flag, 'Y', 'N')

