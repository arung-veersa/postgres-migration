"""
Local Step Functions Simulator.
Simulates AWS Step Functions state machine execution locally with real database.

This allows you to:
1. Test the complete pipeline workflow before AWS deployment
2. Measure actual execution times
3. Validate database operations
4. Identify potential Lambda timeout issues

Usage:
    python scripts/simulate_step_functions.py
    python scripts/simulate_step_functions.py --mock  # Fast test with mock DB
"""

import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import (
    POSTGRES_CONFIG, 
    CONFLICT_SCHEMA, 
    ANALYTICS_SCHEMA,
    validate_config
)
from src.connectors.postgres_connector import PostgresConnector
from src.tasks.task_01_copy_to_temp import Task01CopyToTemp
from src.tasks.task_02_update_conflicts import Task02UpdateConflictVisitMaps
from src.utils.logger import get_logger

logger = get_logger(__name__)


class StepFunctionsSimulator:
    """Simulates AWS Step Functions execution locally."""
    
    def __init__(self, use_mock: bool = False):
        """
        Initialize simulator.
        
        Args:
            use_mock: If True, uses mock connector instead of real database
        """
        self.use_mock = use_mock
        self.results: List[Dict[str, Any]] = []
        self.start_time = None
        self.end_time = None
    
    def _print_header(self):
        """Print simulation header."""
        print()
        print("=" * 70)
        print("AWS STEP FUNCTIONS - LOCAL SIMULATION")
        print("=" * 70)
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Database: {POSTGRES_CONFIG.get('database', 'N/A')}")
        print(f"Conflict Schema: {CONFLICT_SCHEMA}")
        print(f"Analytics Schema: {ANALYTICS_SCHEMA}")
        print(f"Mock Mode: {self.use_mock}")
        print("=" * 70)
        print()
    
    def _print_state_header(self, state_name: str, state_number: int, total_states: int):
        """Print state execution header."""
        print(f"\n[State {state_number}/{total_states}] {state_name}")
        print("-" * 70)
    
    def _print_state_result(self, state_name: str, success: bool, duration: float, 
                           details: Dict[str, Any] = None):
        """Print state execution result."""
        status_icon = "✅" if success else "❌"
        status_text = "SUCCEEDED" if success else "FAILED"
        
        print(f"{status_icon} {status_text} in {duration:.2f}s")
        
        if details:
            for key, value in details.items():
                print(f"   {key}: {value}")
    
    def _execute_state(self, state_name: str, state_func, state_number: int, 
                      total_states: int) -> Dict[str, Any]:
        """
        Execute a single state and track results.
        
        Args:
            state_name: Name of the state
            state_func: Function to execute
            state_number: Current state number
            total_states: Total number of states
        
        Returns:
            State execution result
        """
        self._print_state_header(state_name, state_number, total_states)
        
        start = time.time()
        
        try:
            result = state_func()
            duration = time.time() - start
            
            state_result = {
                'state': state_name,
                'status': 'SUCCEEDED',
                'duration': duration,
                'result': result
            }
            
            # Extract details for display
            details = {}
            if isinstance(result, dict):
                if 'affected_rows' in result:
                    details['Rows Affected'] = f"{result['affected_rows']:,}"
                if 'updated_rows' in result:
                    details['Rows Updated'] = f"{result['updated_rows']:,}"
                if 'duration_seconds' in result:
                    details['Task Duration'] = f"{result['duration_seconds']:.2f}s"
            
            self._print_state_result(state_name, True, duration, details)
            
            self.results.append(state_result)
            return state_result
        
        except Exception as e:
            duration = time.time() - start
            
            state_result = {
                'state': state_name,
                'status': 'FAILED',
                'duration': duration,
                'error': str(e)
            }
            
            details = {'Error': str(e)}
            self._print_state_result(state_name, False, duration, details)
            
            self.results.append(state_result)
            raise
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete Step Functions simulation.
        
        Returns:
            Summary of execution results
        """
        self.start_time = time.time()
        self._print_header()
        
        try:
            # State 1: Validate Configuration
            self._execute_state(
                state_name="ValidateConfig",
                state_func=self._state_validate_config,
                state_number=1,
                total_states=3
            )
            
            # State 2: Execute Task 01
            self._execute_state(
                state_name="ExecuteTask01",
                state_func=self._state_execute_task_01,
                state_number=2,
                total_states=3
            )
            
            # State 3: Execute Task 02
            self._execute_state(
                state_name="ExecuteTask02",
                state_func=self._state_execute_task_02,
                state_number=3,
                total_states=3
            )
            
            self.end_time = time.time()
            self._print_summary()
            
            return {
                'status': 'SUCCEEDED',
                'states': self.results,
                'total_duration': self.end_time - self.start_time
            }
        
        except Exception as e:
            self.end_time = time.time()
            self._print_summary(failed=True)
            
            return {
                'status': 'FAILED',
                'states': self.results,
                'total_duration': self.end_time - self.start_time,
                'error': str(e)
            }
    
    def _state_validate_config(self) -> Dict[str, Any]:
        """State 1: Validate configuration."""
        validate_config()
        return {'message': 'Configuration validated successfully'}
    
    def _state_execute_task_01(self) -> Dict[str, Any]:
        """State 2: Execute Task 01."""
        # Get connector
        if self.use_mock:
            from scripts.mock_postgres_connector import MockPostgresConnector
            connector = MockPostgresConnector(**POSTGRES_CONFIG)
        else:
            connector = PostgresConnector(**POSTGRES_CONFIG)
        
        # Execute task
        task = Task01CopyToTemp(connector)
        result = task.run()
        
        if result['status'] == 'success':
            return result.get('result', {})
        else:
            raise Exception(result.get('error', 'Task 01 failed'))
    
    def _state_execute_task_02(self) -> Dict[str, Any]:
        """State 3: Execute Task 02."""
        # Get connector
        if self.use_mock:
            from scripts.mock_postgres_connector import MockPostgresConnector
            connector = MockPostgresConnector(**POSTGRES_CONFIG)
        else:
            connector = PostgresConnector(**POSTGRES_CONFIG)
        
        # Execute task
        task = Task02UpdateConflictVisitMaps(connector)
        result = task.run()
        
        if result['status'] == 'success':
            return result.get('result', {})
        else:
            raise Exception(result.get('error', 'Task 02 failed'))
    
    def _print_summary(self, failed: bool = False):
        """Print execution summary."""
        total_duration = self.end_time - self.start_time
        
        print()
        print("=" * 70)
        if failed:
            print("❌ PIPELINE EXECUTION FAILED")
        else:
            print("✅ PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
        # State-by-state summary
        print("\nState Summary:")
        print("-" * 70)
        for i, state in enumerate(self.results, 1):
            status_icon = "✅" if state['status'] == 'SUCCEEDED' else "❌"
            duration_min = state['duration'] / 60
            print(f"{i}. {state['state']:<20} {status_icon} {state['status']:<12} "
                  f"{state['duration']:>7.2f}s ({duration_min:>5.2f} min)")
        
        # Total duration
        print("-" * 70)
        total_min = total_duration / 60
        print(f"{'Total Duration':<20}              {total_duration:>7.2f}s ({total_min:>5.2f} min)")
        print("=" * 70)
        
        # Lambda timeout analysis
        if not failed:
            print("\n🔍 Lambda Timeout Analysis:")
            print("-" * 70)
            
            lambda_timeout = 900  # 15 minutes
            lambda_safe_zone = 720  # 12 minutes
            
            if total_duration > lambda_timeout:
                print("⚠️  CRITICAL: Duration EXCEEDS Lambda 15-minute timeout!")
                print("   Action Required: Implement chunking for Task 02")
                print(f"   Exceeded by: {(total_duration - lambda_timeout):.0f}s "
                      f"({(total_duration - lambda_timeout)/60:.1f} min)")
            elif total_duration > lambda_safe_zone:
                print("⚠️  WARNING: Duration is close to Lambda timeout")
                print("   Recommendation: Monitor production executions closely")
                print(f"   Buffer remaining: {(lambda_timeout - total_duration):.0f}s "
                      f"({(lambda_timeout - total_duration)/60:.1f} min)")
            else:
                print("✅ Duration is safely within Lambda 15-minute timeout")
                print(f"   Buffer remaining: {(lambda_timeout - total_duration):.0f}s "
                      f"({(lambda_timeout - total_duration)/60:.1f} min)")
            
            print("=" * 70)


def main():
    """Main entry point for simulation."""
    # Check for mock flag
    use_mock = '--mock' in sys.argv
    
    # Create and run simulator
    simulator = StepFunctionsSimulator(use_mock=use_mock)
    result = simulator.run()
    
    # Exit with appropriate code
    sys.exit(0 if result['status'] == 'SUCCEEDED' else 1)


if __name__ == '__main__':
    main()

