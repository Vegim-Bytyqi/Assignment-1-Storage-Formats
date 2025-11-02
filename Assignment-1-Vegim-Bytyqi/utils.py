import duckdb
import json

def find_scanning_info(node):
  if "extra_info" in node and "Scanning Files" in node["extra_info"]:
    return node["extra_info"]["Scanning Files"]
  for child in node.get("children", []):
    result = find_scanning_info(child)
    if result:
      return result
  return None

def measure_skipping(query, setup_statements=None, profile_output='profile.json'):
  con = duckdb.connect()

  if setup_statements:
    for stmt in setup_statements:
      con.execute(stmt)

  con.execute("PRAGMA enable_profiling='json';")
  if profile_output:
    con.execute(f"PRAGMA profiling_output='{profile_output}';")

  con.execute(query).fetchall()
  con.execute('PRAGMA disable_profiling;')

  profile_path = profile_output or 'profile.json'

  # Read the profile.
  with open(profile_path) as f:
    prof = json.load(f)
    scan_info = find_scanning_info(prof)
    latency = prof.get("latency", None)

    if scan_info:
      read, total_in_query = map(int, scan_info.split("/"))
      skipped = total_in_query - read
      print(f'Files read: {read}, Total files: {total_in_query}, Skipped: {skipped} | Latency: {latency}')
      return {'read': read, 'total': total_in_query, 'skipped': skipped, 'latency' : latency}
    else:
      print('No scanning info found in profile.')
      return None
