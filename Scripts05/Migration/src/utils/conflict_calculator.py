"""
Conflict Calculator - Main logic for calculating conflicts between visits.

This module orchestrates the conflict detection process by:
1. Merging V1 and V2 visit data
2. Applying all 7 conflict rules
3. Calculating derived fields
4. Filtering to only conflicting pairs
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
from datetime import datetime

from src.utils.conflict_rules import ConflictRules
from src.utils.geospatial_utils import GeospatialUtils
from src.utils.logger import get_logger


logger = get_logger(__name__)


class ConflictCalculator:
    """
    Calculator for visit conflicts.
    
    Merges V1 (visits to update) with V2 (all visits) and applies
    conflict detection rules to identify conflicts.
    """
    
    def __init__(self):
        self.rules = ConflictRules()
        self.geo_utils = GeospatialUtils()
    
    def calculate_conflicts(self, 
                           v1_df: pd.DataFrame, 
                           v2_df: pd.DataFrame,
                           settings: pd.Series,
                           mph_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate conflicts between V1 and V2 visits.
        
        Args:
            v1_df: Visits with CONFLICTID (to be updated)
            v2_df: All visits for the same SSNs
            settings: Settings row with ExtraDistancePer, etc.
            mph_df: MPH lookup table
            
        Returns:
            DataFrame with conflict pairs and calculated fields
        """
        logger.info(f"Calculating conflicts: V1={len(v1_df)} visits, V2={len(v2_df)} visits")
        
        if v1_df.empty or v2_df.empty:
            logger.warning("V1 or V2 is empty, no conflicts to calculate")
            return pd.DataFrame()
        
        # Step 1: Merge V1 and V2 on conflict conditions
        conflicts = self._merge_v1_v2(v1_df, v2_df)
        
        if conflicts.empty:
            logger.info("No potential conflict pairs after merge")
            return pd.DataFrame()
        
        logger.info(f"Found {len(conflicts)} potential conflict pairs")
        
        # Step 2: Apply all 7 conflict rules
        conflicts = self._apply_conflict_rules(conflicts, settings, mph_df)
        
        # Step 3: Filter to only rows matching at least one rule
        conflicts = self._filter_conflicting_pairs(conflicts)
        
        if conflicts.empty:
            logger.info("No conflicts detected after applying rules")
            return pd.DataFrame()
        
        logger.info(f"Detected {len(conflicts)} actual conflicts")
        
        # Step 4: Calculate derived fields
        conflicts = self._calculate_derived_fields(conflicts, settings, mph_df)
        
        # Step 5: Prepare update data
        conflicts = self._prepare_update_data(conflicts)
        
        return conflicts
    
    def _merge_v1_v2(self, v1_df: pd.DataFrame, v2_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge V1 and V2 on conflict conditions.
        
        Join conditions:
        - Same VisitDate
        - Same SSN
        - Different VisitID
        - Different ProviderID
        """
        # Merge on VisitDate and SSN
        merged = v1_df.merge(
            v2_df,
            on=['VisitDate', 'SSN'],
            how='inner',
            suffixes=('', '_Con')
        )
        
        # Filter: different visits, different providers
        merged = merged[
            (merged['VisitID'] != merged['VisitID_Con']) &
            (merged['ProviderID'] != merged['ProviderID_Con'])
        ]
        
        # Rename Con columns properly
        con_columns = [col for col in merged.columns if col.endswith('_Con')]
        rename_map = {col: 'Con' + col[:-4] for col in con_columns}
        merged = merged.rename(columns=rename_map)
        
        return merged
    
    def _apply_conflict_rules(self, 
                             conflicts: pd.DataFrame,
                             settings: pd.Series,
                             mph_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all 7 conflict detection rules.
        
        Adds flag columns for each rule:
        - SameSchTimeFlag
        - SameVisitTimeFlag
        - SchAndVisitTimeSameFlag
        - SchOverAnotherSchTimeFlag
        - VisitTimeOverAnotherVisitTimeFlag
        - SchTimeOverVisitTimeFlag
        - DistanceFlag
        """
        logger.info("Applying conflict rules")
        
        # Create temporary DataFrames for V1 and V2 perspectives
        # (rules expect separate V1 and V2 DataFrames)
        v1_cols = [col for col in conflicts.columns if not col.startswith('Con')]
        v2_cols_map = {f'Con{col}': col for col in conflicts.columns 
                       if col.startswith('Con') and col != 'CONFLICTID'}
        
        # Apply rules
        conflicts['SameSchTimeFlag'] = self.rules.rule_1_same_sch_time(conflicts, conflicts)
        conflicts['SameVisitTimeFlag'] = self.rules.rule_2_same_visit_time(conflicts, conflicts)
        conflicts['SchAndVisitTimeSameFlag'] = self.rules.rule_3_sch_visit_time_same(conflicts, conflicts)
        conflicts['SchOverAnotherSchTimeFlag'] = self.rules.rule_4_sch_overlap_sch(conflicts, conflicts)
        conflicts['VisitTimeOverAnotherVisitTimeFlag'] = self.rules.rule_5_visit_overlap_visit(conflicts, conflicts)
        conflicts['SchTimeOverVisitTimeFlag'] = self.rules.rule_6_sch_overlap_visit(conflicts, conflicts)
        
        # Rule 7 requires additional parameters
        # Convert Decimal to float to avoid type errors
        extra_distance_pct = float(settings.get('ExtraDistancePer', 1.0)) if settings is not None else 1.0
        conflicts['DistanceFlag'] = self.rules.rule_7_distance_flag(
            conflicts, 
            extra_distance_pct, 
            mph_df
        )
        
        return conflicts
    
    def _filter_conflicting_pairs(self, conflicts: pd.DataFrame) -> pd.DataFrame:
        """
        Filter to only pairs that match at least one conflict rule.
        """
        rule_columns = [
            'SameSchTimeFlag',
            'SameVisitTimeFlag',
            'SchAndVisitTimeSameFlag',
            'SchOverAnotherSchTimeFlag',
            'VisitTimeOverAnotherVisitTimeFlag',
            'SchTimeOverVisitTimeFlag',
            'DistanceFlag'
        ]
        
        # Keep rows where at least one rule is 'Y'
        has_conflict = (conflicts[rule_columns] == 'Y').any(axis=1)
        filtered = conflicts[has_conflict].copy()
        
        logger.info(f"Filtered to {len(filtered)} conflicting pairs")
        
        return filtered
    
    def _calculate_derived_fields(self,
                                  conflicts: pd.DataFrame,
                                  settings: pd.Series,
                                  mph_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived fields like distance, time differences, etc.
        """
        logger.info("Calculating derived fields")
        
        # Calculate minute difference between scheduled times
        conflicts['MinuteDiffBetweenSch'] = self._calculate_minute_diff(conflicts)
        
        # Calculate distance and ETA
        if settings is not None:
            # Convert Decimal to float to avoid type errors
            extra_distance_pct = float(settings.get('ExtraDistancePer', 1.0))
            
            conflicts['DistanceMilesFromLatLng'] = self.geo_utils.calculate_distance_vectorized(
                conflicts,
                'PLatitude', 'PLongitude',
                'ConPLatitude', 'ConPLongitude',
                extra_distance_pct
            )
            
            # Lookup MPH for each distance (convert Decimal to float)
            conflicts['AverageMilesPerHour'] = conflicts['DistanceMilesFromLatLng'].apply(
                lambda d: float(mph) if (mph := self.geo_utils.lookup_mph(d, mph_df)) is not None else None
            )
            
            # Calculate ETA
            conflicts['ETATravleMinutes'] = self.geo_utils.calculate_eta_vectorized(
                conflicts['DistanceMilesFromLatLng'],
                conflicts['AverageMilesPerHour']
            )
        
        return conflicts
    
    def _calculate_minute_diff(self, conflicts: pd.DataFrame) -> pd.Series:
        """
        Calculate minute difference between scheduled visits.
        
        Logic from SQL lines 98-108:
        - If both positive, take minimum
        - Otherwise take the positive one
        - Otherwise 0
        """
        diff_v1_to_v2 = (conflicts['ConVisitStartTime'] - conflicts['VisitEndTime']).dt.total_seconds() / 60
        diff_v2_to_v1 = (conflicts['VisitStartTime'] - conflicts['ConVisitEndTime']).dt.total_seconds() / 60
        
        # Both positive: take minimum
        both_positive = (diff_v1_to_v2 > 0) & (diff_v2_to_v1 > 0)
        
        # Initialize as float64 to avoid dtype incompatibility warning
        result = pd.Series(0.0, index=conflicts.index, dtype='float64')
        result[both_positive] = np.minimum(diff_v1_to_v2[both_positive], diff_v2_to_v1[both_positive])
        result[(~both_positive) & (diff_v1_to_v2 > 0)] = diff_v1_to_v2[(~both_positive) & (diff_v1_to_v2 > 0)]
        result[(~both_positive) & (diff_v2_to_v1 > 0)] = diff_v2_to_v1[(~both_positive) & (diff_v2_to_v1 > 0)]
        
        return result
    
    def _prepare_update_data(self, conflicts: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data for bulk update with proper flag logic.
        
        Implements CASE logic for conditional flag updates from SQL lines 12-13.
        """
        # Update flags only if current value is 'N'
        flag_columns = [
            'SameSchTimeFlag',
            'SameVisitTimeFlag',
            'SchAndVisitTimeSameFlag',
            'SchOverAnotherSchTimeFlag',
            'VisitTimeOverAnotherVisitTimeFlag',
            'SchTimeOverVisitTimeFlag',
            'DistanceFlag'
        ]
        
        # These flags are updated conditionally - only if currently 'N'
        # For new records, they'll all be from calculated values
        # Note: This conditional logic will be applied in the UPDATE statement
        
        # Add audit fields
        conflicts['UpdatedDate'] = datetime.now()
        conflicts['UpdateFlag'] = None  # Will be set to NULL
        conflicts['ResolveDate'] = None
        
        return conflicts

