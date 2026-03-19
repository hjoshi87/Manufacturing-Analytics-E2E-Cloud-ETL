# Bosch CNC Machining Dataset Guide

## Overview
- **Source:** https://github.com/boschresearch/CNC_Machining
- **License:** CC-BY-4.0 (free for commercial use)
- **Size:** ~20-30 GB (2,700+ files)
- **Format:** HDF5 (.h5)
- **Time Period:** October 2018 - August 2021 (2+ years)
- **Machines:** 3 (M01, M02, M03)
- **Tool Operations:** 15 (OP00 - OP14)

## File Naming Convention
- Format: `M{machine_id}_{Month}{Year}_{run_id}.h5`
- Example: `M01_Aug2019_000.h5`
  - M01 = Machine 01
  - Aug2019 = August 2019
  - 000 = Run 000 (first run that day)

## Directory Structure

data/bosch/raw/CNC_Machining/
├── data/
│   ├── OP00/  (Step Drill, 250 Hz, ~132 sec)
│   │   ├── M01_Aug2019_000.h5
│   │   ├── M01_Aug2019_001.h5
│   │   ├── M02_Feb2019_000.h5
│   │   └── ... (142 files)
│   ├── OP01/  (Step Drill, 250 Hz, ~29 sec)
│   │   └── ... (156 files)
│   └── ... (through OP14)
└── README.md

## Tool Operations (15 total)
| OP | Description | Speed (Hz) | Feed (mm/s) | Duration (s) |
|----|-------------|-----------|------------|-------------|
| 00 | Step Drill | 250 | ~100 | ~132 |
| 01 | Step Drill | 250 | ~100 | ~29 |
| 02 | Drill | 200 | ~50 | ~42 |
| 03 | Step Drill | 250 | ~330 | ~77 |
| ... | ... | ... | ... | ... |

See full table in project documentation.

## Data Format (Inside HDF5)
Each .h5 file contains:
- **accel_x, accel_y, accel_z** (3 axes, 2000 samples/sec)
- **label** (0 = OK, 1 = NOK/faulty)
- **metadata** (timestamp info in filename)

## Key Challenges (from paper)
1. **Cross-machine drift:** M01 patterns differ from M02/M03
2. **Temporal drift:** Equipment degrades over 2+ years
3. **Class imbalance:** OK:NOK = 816:35
4. **Multiple anomaly types:** Tool wear, tool breakage, misalignment
5. **Manual annotation:** Some labels may be ambiguous

## Citation

Tnani, M., Feil, M., & Diepold, K. (2022).
Smart Data Collection System for Brownfield CNC Milling Machines:
A New Benchmark Dataset for Data-Driven Machine Monitoring.
Procedia CIRP, 107, 131-136.