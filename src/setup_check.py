"""Sanity check script to verify JoDi dependencies are installed correctly."""

import dbt.version
import duckdb

if __name__ == "__main__":
    print(f"dbt version: {dbt.version.get_installed_version()}")
    print(f"duckdb version: {duckdb.__version__}")
    print("JoDi setup complete.")
