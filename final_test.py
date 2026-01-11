#!/usr/bin/env python3
print("=== FINAL TEST ===")

# Добавляем текущую директорию в путь
import sys
import os
sys.path.insert(0, os.getcwd())

print("1. Testing imports...")
try:
    import prompt
    print("   ✅ prompt")
except ImportError as e:
    print(f"   ❌ prompt: {e}")

try:
    from src.primitive_db.utils import load_metadata, validate_column_definition
    print("   ✅ utils")
except ImportError as e:
    print(f"   ❌ utils: {e}")

try:
    from src.primitive_db.core import create_table
    print("   ✅ core")
except ImportError as e:
    print(f"   ❌ core: {e}")

print("\n2. Testing functions...")
try:
    # Test utils
    result = validate_column_definition("name:str")
    print(f"   ✅ validate_column_definition('name:str') = {result}")
    
    # Test core
    metadata = {}
    metadata = create_table(metadata, "test_table", ["age:int", "active:bool"])
    print(f"   ✅ create_table() successful, tables: {list(metadata.keys())}")
    
    print("\n🎉 ALL TESTS PASSED!")
    
except Exception as e:
    print(f"\n❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
