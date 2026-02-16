"""Sanity check script to verify JoDi dependencies are installed correctly."""

import sqlalchemy
import sshtunnel

if __name__ == "__main__":
    print(f"SQLAlchemy version: {sqlalchemy.__version__}")
    print(f"sshtunnel version: {sshtunnel.__version__}")
    print("JoDi setup complete.")
