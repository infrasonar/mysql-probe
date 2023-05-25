from libprobe.asset import Asset
from lib.query import get_conn, query


QUERY = """\
SELECT table_schema, table_name, rows_read, rows_changed
FROM information_schema.table_statistics
"""


async def check_mysql(
        asset: Asset,
        asset_config: dict,
        config: dict) -> dict:

    conn = await get_conn(asset, asset_config, config)
    try:
        res = await query(conn, QUERY)
    finally:
        conn.close()

    return {
        'tables': res
    }
