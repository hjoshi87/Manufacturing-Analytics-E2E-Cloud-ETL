import h5py
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
from tqdm import tqdm
load_dotenv()

def summarize_bosch_dataset(base_path=os.getenv('BOSCH_DATA_DIR')):
    """Walk through all Machines and Operations to create a full summary"""
    if not base_path:
        print(" Error: BOSCH_DATA_DIR not found in .env file.")
        return None
    
    summary_data = []
    base_obj = Path(base_path)
    
    # 1. Get all Machine directories (M01, M02, M03)
    machine_dirs = sorted([d for d in base_obj.iterdir() if d.is_dir() and d.name.startswith('M')])
    
    if not machine_dirs:
        print(f"No machine folders (Mxx) found in {base_path}")
        return None

    for m_dir in machine_dirs:
        print(f"\nScanning Machine: {m_dir.name}")
        
        # 2. Get all Operation directories inside this machine
        op_dirs = sorted([d for d in m_dir.iterdir() if d.is_dir() and d.name.startswith('OP')])
        
        for op_dir in op_dirs:
            op_name = op_dir.name
            # 3. Find all .h5 files recursively (includes good/bad subfolders)
            h5_files = list(op_dir.rglob('*.h5'))
            
            if not h5_files:
                continue

            total_samples = 0
            anomaly_count = 0
            
            for h5_file in tqdm(h5_files, desc=f"  {m_dir.name} - {op_name}", leave=False):
                # Labeling based on the folder name in the path
                if "bad" in str(h5_file).lower():
                    anomaly_count += 1
                
                try:
                    with h5py.File(str(h5_file), 'r') as f:
                        if 'vibration_data' in f:
                            total_samples += len(f['vibration_data'])
                except Exception as e:
                    print(f"Error reading {h5_file.name}: {e}")
                    continue
            
            summary_data.append({
                'Machine': m_dir.name,
                'Operation': op_name,
                'Total_Files': len(h5_files),
                'Anomalies': anomaly_count,
                'Normal': len(h5_files) - anomaly_count,
                'Total_Samples': total_samples
            })
    
    # Create DataFrame
    df_summary = pd.DataFrame(summary_data)
    
    # Print formatted output
    print("\n" + "="*80)
    print("BOSCH CNC GLOBAL DATASET SUMMARY")
    print("="*80)
    # Pivot for a cleaner view: Machines as columns, Operations as rows
    pivot_view = df_summary.pivot_table(
        index='Operation', 
        columns='Machine', 
        values='Total_Files', 
        aggfunc='sum', 
        fill_value=0
    )
    print("Files per Machine/Operation:")
    print(pivot_view)
    print("-" * 80)
    
    print(f"TOTAL STATS:")
    print(f"  Total Files:   {df_summary['Total_Files'].sum()}")
    print(f"  Total Samples: {df_summary['Total_Samples'].sum():,}")
    print(f"  Total Anomalies: {df_summary['Anomalies'].sum()}")
    
    return df_summary

if __name__ == '__main__':
    summarize_bosch_dataset()