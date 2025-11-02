import duckdb
import pandas as pd
from deltalake import write_deltalake

SCALE_FACTOR = 1

# Connect.
con = duckdb.connect()

# And generate TPC-H (sf = `SCALE_FACTOR`).
print(f'Creating TPC-H (sf={SCALE_FACTOR})..')
con.execute('INSTALL tpch; LOAD tpch;')
con.execute(f'CALL dbgen(sf={SCALE_FACTOR});')
print(f'Finished creating TPC-H (sf={SCALE_FACTOR}).')

# Fetch full `lineitem` table.
lineitem = con.execute('SELECT * FROM lineitem').df()

# Make sure it's a datetime.
lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])

# And write.
print(f'Writing to Delta Lake..')
write_deltalake(
  f'./lineitem-{SCALE_FACTOR}-delta',
  lineitem,
  mode='overwrite',
)
print(f'✅ Delta table written to ./lineitem-{SCALE_FACTOR}-delta.')