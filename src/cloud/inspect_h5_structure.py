# Create: inspect_h5_structure.py

import h5py
from pathlib import Path
import os
from dotenv import load_dotenv
root_dir = Path(__file__).resolve().parent.parent.parent
env_path = root_dir / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

def inspect_h5_file(filepath):
    """Inspect HDF5 file structure"""
    print(f"\n{'='*60}")
    print(f"File: {Path(filepath).name}")
    print(f"{'='*60}")
    
    try:
        with h5py.File(filepath, 'r') as f:
            def print_structure(name, obj):
                indent = '  ' * name.count('/')
                if isinstance(obj, h5py.Dataset):
                    print(f"{indent}📊 {name}: shape={obj.shape}, dtype={obj.dtype}")
                else:
                    print(f"{indent}📁 {name}/")
            
            f.visititems(print_structure)
    except Exception as e:
        print(f"Error: {e}")

# Inspect first file
data_dir = Path(os.getenv('BOSCH_DATA_DIR'))
h5_files = list(data_dir.rglob('*.h5'))

if h5_files:
    inspect_h5_file(h5_files[0])
else:
    print("No HDF5 files found")