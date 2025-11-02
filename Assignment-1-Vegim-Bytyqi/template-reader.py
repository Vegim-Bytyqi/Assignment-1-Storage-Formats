import argparse
import utils
import os

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description='Measure file skipping in a Delta Lake scan using DuckDB profiling.'
  )
  parser.add_argument(
    '--delta-path',
    default='./lineitem-1-delta',
    help='Path to the Delta Lake directory.'
  )

  args = parser.parse_args()
  assert os.path.exists(args.delta_path)
  utils.measure_skipping(f"""
    SELECT count(*) FROM delta_scan('{args.delta_path}') WHERE l_shipdate BETWEEN DATE '1993-01-01' AND DATE '1993-02-01'
  """)