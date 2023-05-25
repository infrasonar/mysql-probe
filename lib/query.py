import aiomysql
import datetime
import decimal
import logging
from libprobe.asset import Asset
from libprobe.exceptions import CheckException


DEFAULT_MYSQL_PORT = 3306


async def get_conn(
        asset: Asset,
        asset_config: dict,
        config: dict) -> aiomysql.Connection:
    address = config.get('address')
    if not address:
        address = asset.name
    assert asset_config, 'missing credentials'
    port = config.get('port', DEFAULT_MYSQL_PORT)

    try:
        conn = await aiomysql.connect(
            host=address,
            port=port,
            user=asset_config['username'],
            password=asset_config['password'],
        )
    except Exception as e:
        error_msg = str(e) or type(e).__name__
        raise CheckException(f'unable to connect: {error_msg}')
    return conn


async def query(conn: aiomysql.Connection, query: str) -> list:
    items = []
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(query)
            rows = await cursor.fetchall()
            for row in rows:
                item = {}
                for (name, *_), value in zip(cursor.description, row):
                    if isinstance(value, decimal.Decimal):
                        item[name] = float(value)
                    elif isinstance(value, datetime.datetime):
                        row[name] = value.timestamp()
                    elif isinstance(value, datetime.timedelta):
                        row[name] = value.seconds
                    else:
                        item[name] = value
                items.append(item)

    except Exception as e:
        error_msg = str(e) or type(e).__name__
        logging.exception(f'query error: {error_msg};')
        raise CheckException(error_msg)

    return items


async def query_flat(conn: aiomysql.Connection, query: str) -> list:
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(query)
            item = dict(await cursor.fetchall())

    except Exception as e:
        error_msg = str(e) or type(e).__name__
        logging.exception(f'query error: {error_msg};')
        raise CheckException(error_msg)
    else:
        return item
