manufacturing_analytics/
├── dbt_project.yml           # Project config
├── profiles.yml              # Redshift credentials
├── models/
│   ├── staging/              # Clean, rename columns
│   │   └── stg_silver_harmonized.sql
│   ├── intermediate/         # Business logic
│   │   ├── int_anomaly_metrics.sql
│   │   ├── int_energy_consumption.sql
│   │   └── int_industrial_correlation.sql
│   └── mart/                 # Final analytics tables
│       ├── fct_machine_performance.sql
│       ├── fct_energy_analysis.sql
│       └── dim_machines.sql
├── tests/
│   └── schema_tests.yml
├── macros/
└── seeds/