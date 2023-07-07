from libprobe.asset import Asset
from lib.query import get_conn, query_flat


QUERY_STATUS = "SHOW /*!50002 GLOBAL */ STATUS"
QUERY_VARIABLES = "SHOW GLOBAL VARIABLES"

STATUS_VARS = {
    # Command Metrics
    'Prepared_stmt_count': int,
    'Slow_queries': int,
    'Questions': int,
    'Queries': int,
    'Com_select': int,
    'Com_insert': int,
    'Com_update': int,
    'Com_delete': int,
    'Com_replace': int,
    'Com_load': int,
    'Com_insert_select': int,
    'Com_update_multi': int,
    'Com_delete_multi': int,
    'Com_replace_select': int,
    # Connection Metrics
    'Connections': int,
    'Max_used_connections': int,
    'Aborted_clients': int,
    'Aborted_connects': int,
    # Table Cache Metrics
    'Open_files': int,
    'Open_tables': int,
    # Network Metrics
    'Bytes_sent': int,
    'Bytes_received': int,
    # Query Cache Metrics
    'Qcache_hits': int,
    'Qcache_inserts': int,
    'Qcache_lowmem_prunes': int,
    # Table Lock Metrics
    'Table_locks_waited': int,
    'Table_locks_waited_rate': int,
    # Temporary Table Metrics
    'Created_tmp_tables': int,
    'Created_tmp_disk_tables': int,
    'Created_tmp_files': int,
    # Thread Metrics
    'Threads_connected': int,
    'Threads_running': int,
    # MyISAM Metrics
    'Key_buffer_bytes_unflushed': int,
    'Key_buffer_bytes_used': int,
    'Key_read_requests': int,
    'Key_reads': int,
    'Key_write_requests': int,
    'Key_writes': int,

    'Binlog_cache_disk_use': int,
    'Binlog_cache_use': int,
    'Handler_commit': int,
    'Handler_delete': int,
    'Handler_prepare': int,
    'Handler_read_first': int,
    'Handler_read_key': int,
    'Handler_read_next': int,
    'Handler_read_prev': int,
    'Handler_read_rnd': int,
    'Handler_read_rnd_next': int,
    'Handler_rollback': int,
    'Handler_update': int,
    'Handler_write': int,
    'Opened_tables': int,
    'Qcache_total_blocks': int,
    'Qcache_free_blocks': int,
    'Qcache_free_memory': int,
    'Qcache_not_cached': int,
    'Qcache_queries_in_cache': int,
    'Select_full_join': int,
    'Select_full_range_join': int,
    'Select_range': int,
    'Select_range_check': int,
    'Select_scan': int,
    'Sort_merge_passes': int,
    'Sort_range': int,
    'Sort_rows': int,
    'Sort_scan': int,
    'Table_locks_immediate': int,
    'Table_locks_immediate_rate': int,
    'Threads_cached': int,
    'Threads_created': int,

    # TODO ook deze?
    # 'Caching_sha2_password_rsa_public_key': str,
    # 'Current_tls_ca': str,
    # 'Current_tls_cert': str,
    # 'Current_tls_key': str,
    # 'Current_tls_version': str,
    # 'Innodb_buffer_pool_dump_status': str,
    # 'Innodb_buffer_pool_load_status': str,
    # 'Innodb_redo_log_enabled': lambda v: v == 'ON',
    # 'Innodb_redo_log_read_only': lambda v: v == 'ON',
    # 'Innodb_redo_log_resize_status': lambda v: v == 'OK',
    # 'Max_used_connections_time': str,  # to timestamp?
    # 'Mysqlx_address': str,
    # 'Mysqlx_socket': str,
    # 'Mysqlx_ssl_server_not_after': str,  # to timestamp?
    # 'Mysqlx_ssl_server_not_before': str,  # to timestamp?
    # 'Resource_group_supported': lambda v: v == 'ON',
    # 'Rsa_public_key': str,
    # 'Ssl_server_not_after': str,  # to timestamp?
    # 'Ssl_server_not_before': str,  # to timestamp?
    # 'Ssl_session_cache_mode': str,
    # 'Telemetry_traces_supported': lambda v: v == 'ON',
    # 'Tls_library_version': str,
}

VARIABLES_VARS = {
    'key_buffer_size': int,
    'max_connections': int,
    'max_prepared_stmt_count': int,
    'query_cache_size': int,
    'table_open_cache': int,
    'thread_cache_size': int,
    'long_query_time': float,
}


async def check_mysql(
        asset: Asset,
        asset_config: dict,
        config: dict) -> dict:

    conn = await get_conn(asset, asset_config, config)
    try:
        status = await query_flat(conn, QUERY_STATUS)
        variables = await query_flat(conn, QUERY_VARIABLES)
    finally:
        conn.close()

    item = {
        'name': 'status',
    }
    for var_name, var_type in STATUS_VARS.items():
        if var_name in status:
            name = var_name.lower()  # lowercase metricnames
            item[name] = var_type(status[var_name])

    item_variables = {
        'name': 'variables',
    }
    for var_name, var_type in VARIABLES_VARS.items():
        if var_name in variables:
            item_variables[var_name] = var_type(variables[var_name])

    return {
        'status': [item],
        'variables': [item_variables]
    }
