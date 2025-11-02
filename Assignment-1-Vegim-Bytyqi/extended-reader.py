import argparse
import os
from textwrap import dedent

import utils

DEFAULT_SCALE_FACTOR = 1

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description='Measure Delta Lake file skipping for TPC-H Q10 using DuckDB profiling.'
  )
  parser.add_argument(
    '--delta-path',
    default='./lineitem-delta-part',
    help='Path to the Delta Lake directory containing the optimized lineitem table.'
  )
  parser.add_argument(
    '--scale-factor',
    type=int,
    default=DEFAULT_SCALE_FACTOR,
    help='TPC-H scale factor to load into DuckDB to supply dimension tables.'
  )
  parser.add_argument(
    '--profile-output',
    default='profile_q10.json',
    help='Where to write the DuckDB JSON profile output.'
  )

  args = parser.parse_args()
  if not os.path.exists(args.delta_path):
    raise FileNotFoundError(f'Delta path not found: {args.delta_path}')

  print(f'Using delta path: {args.delta_path}')

  setup = [
    "INSTALL tpch; LOAD tpch;",
    f"CALL dbgen(sf={args.scale_factor});"
  ]

  query = dedent(f"""
    WITH q10_lineitem AS (
      SELECT *
      FROM delta_scan('{args.delta_path}')
      WHERE
        o_orderdate >= DATE '1993-10-01'
        AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
        AND l_returnflag = 'R'
    )
    SELECT
      c.c_custkey,
      c.c_name,
      SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
      c.c_acctbal,
      n.n_name,
      c.c_address,
      c.c_phone,
      c.c_comment
    FROM
      q10_lineitem l
      JOIN customer c ON c.c_custkey = l.o_custkey
      JOIN nation n ON c.c_nationkey = n.n_nationkey
    GROUP BY
      c.c_custkey,
      c.c_name,
      c.c_acctbal,
      c.c_phone,
      n.n_name,
      c.c_address,
      c.c_comment
    ORDER BY
      revenue DESC
    LIMIT 20;
  """)

  stats = utils.measure_skipping(
    query,
    setup_statements=setup,
    profile_output=args.profile_output
  )

  if stats:
    print(f"Skipped {stats['skipped']} of {stats['total']} files (read {stats['read']}).")
    if stats['latency'] is not None:
      print(f"Query latency: {stats['latency']}s")
