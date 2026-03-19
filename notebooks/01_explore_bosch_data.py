"""
Explore Bosch CNC dataset structure
"""
import h5py
import pandas as pd
import numpy as np
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

def explore_h5_file(filepath):
    """Examine a single HDF5 file with a flatter structure"""
    with h5py.File(filepath, 'r') as f:
        print(f"\n{'='*60}")
        print(f"File: {os.path.basename(filepath)}")
        print(f"{'='*60}")
        
        # In this dataset, the keys are actually the datasets themselves
        items = list(f.keys())
        print(f"Items found in root: {items}")
        
        for item_name in items:
            item = f[item_name]
            if isinstance(item, h5py.Dataset):
                print(f"  Dataset: {item_name} | Shape: {item.shape} | Dtype: {item.dtype}")
                if len(item.shape) > 0:
                    # Show a small slice of data
                    print(f"    Sample: {item[0:10]}")
            elif isinstance(item, h5py.Group):
                print(f"  Group: {item_name} | Keys: {list(item.keys())}")

def load_bosch_file(filepath, max_samples=None):
    """Load Bosch HDF5 file and split 3D vibration data into X, Y, Z"""
    with h5py.File(filepath, 'r') as f:
        # data has shape (N, 3)
        data = f['vibration_data'][:] 
        
        if max_samples:
            data = data[:max_samples]

        # Splitting the 2D array into three 1D arrays
        # data[:, 0] gets all rows for the first column (X)
        accel_x = data[:, 0]
        accel_y = data[:, 1]
        accel_z = data[:, 2]

        # Label based on path (Good=0, Bad=1)
        label = 1 if 'bad' in str(filepath).lower() else 0
        
        # Parse metadata from filename
        filename = os.path.basename(filepath)
        parts = filename.replace('.h5', '').split('_')
        machine_id = parts[0]
        run_id = parts[-1] # Usually the last part
        
        # Create timestamps
        timestamps = pd.date_range(
            start='2020-01-01',
            periods=len(accel_x),
            freq='500us'
        )
        
        df = pd.DataFrame({
            'timestamp': timestamps,
            'machine_id': machine_id,
            'run_id': run_id,
            'accel_x': accel_x,
            'accel_y': accel_y,
            'accel_z': accel_z,
            'label': label
        })
        
        return df

if __name__ == '__main__':
    # Find some sample Bosch files
    bosch_path = Path(os.getenv('BOSCH_DATA_DIR'))
    
    # Get first operation directory
    op_dirs = sorted([d for d in bosch_path.iterdir() if d.is_dir() and d.name.startswith('OP')])
    
    if not op_dirs:
        print("Bosch dataset not found. Run: git clone https://github.com/boschresearch/CNC_Machining.git data/bosch/raw")
        exit(1)
    
    # Explore first operation's first file
    # 2. Let's look for .h5 files inside 'good' or 'bad' subfolders
    # We use rglob('**/*.h5') to search recursively
    all_h5_files = []
    for op_dir in op_dirs:
        # This finds files in OP00/good/*.h5, OP00/bad/*.h5, etc.
        files = list(op_dir.rglob('*.h5'))
        all_h5_files.extend(files)

    if all_h5_files:
        # Pick the first one found to explore
        sample_file = all_h5_files[0]
        
        # Determine label based on folder name
        label_category = "Good" if "good" in str(sample_file).lower() else "Bad"
        
        print(f"Found {len(all_h5_files)} total .h5 files.")
        print(f"Exploring: {sample_file} (Category: {label_category})")
        
        # --- Rest of your original exploration code ---
        explore_h5_file(str(sample_file))
        df = load_bosch_file(str(sample_file))
        # ... (rest of your print statements)
    else:
        print(f"No .h5 files found in subfolders of {bosch_path}")