"""
Test script for Phase 2 chunking logic.
Run this locally before deploying to AWS.
"""

import sys
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import POSTGRES_CONFIG
from src.connectors.postgres_connector import PostgresConnector
from src.tasks.task_02_get_chunks import Task02GetChunks
from src.tasks.task_02_process_chunk import Task02ProcessChunk


def test_chunk_generation():
    """Test chunk generation logic."""
    print("=" * 80)
    print("TEST 1: Chunk Generation")
    print("=" * 80)
    
    connector = PostgresConnector(**POSTGRES_CONFIG)
    task = Task02GetChunks(connector)
    
    try:
        result = task.run()
        
        if result['status'] != 'success':
            print(f"\n❌ Task failed: {result.get('error', 'Unknown error')}")
            return None
        
        # Extract actual chunk data from result wrapper
        chunk_data = result['result']
        
        print(f"\n✅ Chunk generation successful!")
        print(f"\nSummary:")
        print(f"  Total rows: {chunk_data['total_rows']:,}")
        print(f"  Total (date, ssn) keys: {chunk_data['total_keys']:,}")
        print(f"  Number of chunks: {chunk_data['num_chunks']}")
        print(f"  Chunks file: {chunk_data['chunks_file']}")
        print(f"  Duration: {result['duration_seconds']:.2f}s")
        
        print(f"\nChunk IDs: {chunk_data['chunk_ids'][:10]}")
        if len(chunk_data['chunk_ids']) > 10:
            print(f"  ... and {len(chunk_data['chunk_ids']) - 10} more")
        
        # Load actual chunks from file for inspection
        chunks_file = chunk_data['chunks_file']
        with open(chunks_file, 'r') as f:
            all_chunks = json.load(f)
        
        print(f"\nFirst 5 chunks (from {chunks_file}):")
        for chunk in all_chunks[:5]:
            print(f"  Chunk {chunk['chunk_id']}: "
                  f"{chunk['estimated_rows']:,} rows, "
                  f"{chunk['num_keys']} keys, "
                  f"dates {chunk['date_range']['start']} to {chunk['date_range']['end']}")
        
        if len(all_chunks) > 5:
            print(f"  ... and {len(all_chunks) - 5} more chunks")
        
        print(f"\n📄 Full chunks stored in: {chunks_file}")
        
        return chunk_data
        
    except Exception as e:
        print(f"\n❌ Chunk generation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def test_single_chunk_processing(chunks_result):
    """Test processing a single chunk."""
    if not chunks_result or chunks_result['num_chunks'] == 0:
        print("\n⚠️  No chunks available to test")
        return
    
    print("\n" + "=" * 80)
    print("TEST 2: Single Chunk Processing")
    print("=" * 80)
    
    # Load chunks from file
    chunks_file = chunks_result['chunks_file']
    with open(chunks_file, 'r') as f:
        all_chunks = json.load(f)
    
    # Get first chunk with reasonable size
    test_chunk = None
    for chunk in all_chunks:
        if chunk['estimated_rows'] < 1000:  # Use small chunk for testing
            test_chunk = chunk
            break
    
    if not test_chunk:
        test_chunk = all_chunks[0]
        print(f"\n⚠️  Using chunk {test_chunk['chunk_id']} with {test_chunk['estimated_rows']} rows")
        print("   This might take a while...")
        
        response = input("\nContinue with this chunk? (y/n): ")
        if response.lower() != 'y':
            print("Skipping chunk processing test")
            return
    
    connector = PostgresConnector(**POSTGRES_CONFIG)
    task = Task02ProcessChunk(connector)
    
    try:
        print(f"\nProcessing chunk {test_chunk['chunk_id']}...")
        print(f"  Estimated rows: {test_chunk['estimated_rows']}")
        print(f"  Number of keys: {test_chunk['num_keys']}")
        
        # Call execute() with just chunk_id (it will load keys from file)
        result = task.execute(chunk_id=test_chunk['chunk_id'])
        
        print(f"\n✅ Chunk processing successful!")
        print(f"\nResults:")
        print(f"  Rows marked: {result['rows_marked']}")
        print(f"  Rows updated: {result['rows_updated']}")
        print(f"  Duration: {result['duration_seconds']:.2f}s")
        print(f"  Throughput: {result['throughput_rows_per_sec']:.2f} rows/sec")
        
        if result['rows_updated'] > 0:
            print(f"\n✅ Test successful - chunk processing works correctly")
        else:
            print(f"\n⚠️  No rows updated - chunk may have been already processed (idempotent)")
        
        return result
        
    except Exception as e:
        print(f"\n❌ Chunk processing failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def test_idempotency(chunks_result):
    """Test that re-running a chunk is safe (idempotent)."""
    if not chunks_result or chunks_result['num_chunks'] == 0:
        return
    
    print("\n" + "=" * 80)
    print("TEST 3: Idempotency Check")
    print("=" * 80)
    
    # Load chunks from file
    chunks_file = chunks_result['chunks_file']
    with open(chunks_file, 'r') as f:
        all_chunks = json.load(f)
    
    # Find a small chunk
    test_chunk = None
    for chunk in all_chunks:
        if chunk['estimated_rows'] < 500:
            test_chunk = chunk
            break
    
    if not test_chunk:
        print("⚠️  No small chunks found for idempotency test (skipping)")
        return
    
    print(f"\nTesting idempotency with chunk {test_chunk['chunk_id']}...")
    print(f"  Estimated rows: {test_chunk['estimated_rows']}")
    
    response = input("This will run the same chunk twice. Continue? (y/n): ")
    if response.lower() != 'y':
        print("Skipping idempotency test")
        return
    
    connector = PostgresConnector(**POSTGRES_CONFIG)
    task = Task02ProcessChunk(connector)
    
    try:
        # First run
        print("\nRun 1:")
        result1 = task.execute(chunk_id=test_chunk['chunk_id'])
        print(f"  Rows updated: {result1['rows_updated']}")
        print(f"  Duration: {result1['duration_seconds']:.2f}s")
        
        # Second run (should be idempotent)
        print("\nRun 2 (same chunk):")
        result2 = task.execute(chunk_id=test_chunk['chunk_id'])
        print(f"  Rows updated: {result2['rows_updated']}")
        print(f"  Duration: {result2['duration_seconds']:.2f}s")
        
        # Verify idempotency
        if result2['rows_updated'] == 0:
            print(f"\n✅ IDEMPOTENCY VERIFIED!")
            print("   Second run updated 0 rows (already processed)")
            print("   Safe to re-run failed chunks!")
        elif result2['rows_updated'] == result1['rows_updated']:
            print(f"\n✅ IDEMPOTENCY VERIFIED!")
            print("   Both runs updated same number of rows")
            print("   Results are consistent!")
        else:
            print(f"\n⚠️  WARNING: Different results!")
            print(f"   Run 1: {result1['rows_updated']} rows")
            print(f"   Run 2: {result2['rows_updated']} rows")
            print("   This might indicate non-idempotent behavior")
        
    except Exception as e:
        print(f"\n❌ Idempotency test failed: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Main test runner."""
    print("\n" + "=" * 80)
    print("PHASE 2 CHUNKING - LOCAL TESTS")
    print("=" * 80)
    print("\nThis script tests the Phase 2 chunking implementation locally.")
    print("Make sure your .env file is configured correctly.\n")
    
    response = input("Continue with tests? (y/n): ")
    if response.lower() != 'y':
        print("Tests cancelled")
        return
    
    # Test 1: Chunk generation
    chunks_result = test_chunk_generation()
    
    if not chunks_result:
        print("\n❌ Cannot proceed - chunk generation failed")
        return
    
    # Test 2: Single chunk processing
    print("\n" + "-" * 80)
    response = input("\nTest single chunk processing? (y/n): ")
    if response.lower() == 'y':
        test_single_chunk_processing(chunks_result)
    
    # Test 3: Idempotency
    print("\n" + "-" * 80)
    response = input("\nTest idempotency (run same chunk twice)? (y/n): ")
    if response.lower() == 'y':
        test_idempotency(chunks_result)
    
    print("\n" + "=" * 80)
    print("TESTS COMPLETE")
    print("=" * 80)
    print("\nNext steps:")
    print("  1. Review chunks in temp/task02_chunks.json")
    print("  2. Deploy updated Lambda: cd deploy && .\\build_lambda.ps1")
    print("  3. Update Step Functions state machine (see aws/README.md)")
    print("=" * 80)


if __name__ == '__main__':
    main()

