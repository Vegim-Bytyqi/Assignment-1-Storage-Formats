import duckdb
import pandas as pd
from deltalake import write_deltalake

SCALE_FACTOR = 1

# Connect.
con = duckdb.connect()

# And generate TPC-H (sf = 1).
print(f'Creating TPC-H (sf={SCALE_FACTOR})..')
con.execute('INSTALL tpch; LOAD tpch;')
con.execute(f'CALL dbgen(sf={SCALE_FACTOR});')
print(f'Finished creating TPC-H (sf={SCALE_FACTOR}).')

# Fetch full `lineitem` table and enrich it with order dates so Q10 filters can be applied directly.
lineitem = con.execute('SELECT * FROM lineitem').df()
orders = con.execute('SELECT o_orderkey, o_orderdate, o_custkey FROM orders').df()

# Normalize temporal columns.
lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])

# Denormalize order dates into the lineitem rows.
lineitem = lineitem.merge(
  orders,
  how='left',
  left_on='l_orderkey',
  right_on='o_orderkey',
  validate='many_to_one'
)
lineitem = lineitem.drop(columns=['o_orderkey'])

# Derive partition columns aligned with TPC-H Q10 predicates.
lineitem['order_year'] = lineitem['o_orderdate'].dt.year.astype('int32')
lineitem['order_month'] = lineitem['o_orderdate'].dt.month.astype('int32')

# Sort to keep return flag/date locality inside each file for better stats.
lineitem = lineitem.sort_values(
  ['l_returnflag', 'order_year', 'order_month', 'l_orderkey']
).reset_index(drop=True)

# Write to Delta Lake partitioned on return flag + order month for efficient skipping.
print(f'Writing to Delta Lake..')
write_deltalake(
  'lineitem-delta-part',
  lineitem,
  mode='overwrite',
  partition_by=['l_returnflag', 'order_year', 'order_month']
)
print(f'✅ Delta table written to lineitem-delta-part.')
