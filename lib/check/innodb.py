import logging
import re
from collections import defaultdict
from libprobe.asset import Asset
from libprobe.exceptions import IgnoreCheckException
from lib.query import get_conn, query
from typing import Dict, Any


QUERY_HAS_INNODB = """\
SELECT engine
FROM information_schema.ENGINES
WHERE engine='InnoDB' and support != 'no' and support != 'disabled'
"""
QUERY = "SHOW /*!50000 ENGINE*/ INNODB STATUS"


def get_stats_from_innodb_status(innodb_status_text) -> Dict[str, Any]:
    results: Dict[str, Any] = defaultdict(int)

    # Here we now parse InnoDB STATUS one line at a time
    # This is heavily inspired by the Percona monitoring plugins work
    txn_seen = False
    prev_line = ''
    # Only return aggregated buffer pool metrics
    buffer_id = -1
    for line in innodb_status_text.splitlines():
        line = line.strip()
        row = re.split(" +", line)
        row = [item.strip(',') for item in row]
        row = [item.strip(';') for item in row]
        row = [item.strip('[') for item in row]
        row = [item.strip(']') for item in row]

        if line.startswith('---BUFFER POOL'):
            buffer_id = int(row[2])

        # SEMAPHORES
        if line.find('Mutex spin waits') == 0:
            # Mutex spin waits 79626940, rounds 157459864, OS waits 698719
            # Mutex spin waits 0, rounds 247280272495, OS waits 316513438
            results['mutex_spin_waits'] = int(row[3])
            results['mutex_spin_rounds'] = int(row[5])
            results['mutex_os_waits'] = int(row[8])
        elif line.find('RW-shared spins') == 0 and line.find(';') > 0:
            # RW-shared spins 3859028, OS waits 2100750; RW-excl spins
            # 4641946, OS waits 1530310
            results['s_lock_spin_waits'] = int(row[2])
            results['x_lock_spin_waits'] = int(row[8])
            results['s_lock_os_waits'] = int(row[5])
            results['x_lock_os_waits'] = int(row[11])
        elif line.find('RW-shared spins') == 0 and \
                line.find('; RW-excl spins') == -1:
            # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
            # RW-shared spins 604733, rounds 8107431, OS waits 241268
            results['s_lock_spin_waits'] = int(row[2])
            results['s_lock_spin_rounds'] = int(row[4])
            results['s_lock_os_waits'] = int(row[7])
        elif line.find('RW-excl spins') == 0:
            # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
            # RW-excl spins 604733, rounds 8107431, OS waits 241268
            results['x_lock_spin_waits'] = int(row[2])
            results['x_lock_spin_rounds'] = int(row[4])
            results['x_lock_os_waits'] = int(row[7])
        elif line.find('seconds the semaphore:') > 0:
            # --Thread 907205 has waited at handler/ha_innodb.cc line 7156 for
            # 1.00 seconds the semaphore:
            results['semaphore_waits'] += 1
            results['semaphore_wait_time'] += int(float(row[9])) * 1000

        # TRANSACTIONS
        elif line.find('Trx id counter') == 0:
            # The beginning of the TRANSACTIONS section: start counting
            # transactions
            # Trx id counter 0 1170664159
            # Trx id counter 861B144C
            txn_seen = True
        elif line.find('History list length') == 0:
            # History list length 132
            results['history_list_length'] = int(row[3])
        elif txn_seen and line.find('---TRANSACTION') == 0:
            # ---TRANSACTION 0, not started, process no 13510, OS thread id
            # 1170446656
            results['current_transactions'] += 1
            if line.find('ACTIVE') > 0:
                results['active_transactions'] += 1
        elif line.find('read views open inside InnoDB') > 0:
            # 1 read views open inside InnoDB
            results['read_views'] = int(row[0])
        elif line.find('mysql tables in use') == 0:
            # mysql tables in use 2, locked 2
            results['tables_in_use'] += int(row[4])
            results['locked_tables'] += int(row[6])
        elif txn_seen and line.find('lock struct(s)') > 0:
            # 23 lock struct(s), heap size 3024, undo log entries 27
            # LOCK WAIT 12 lock struct(s), heap size 3024, undo log entries 5
            # LOCK WAIT 2 lock struct(s), heap size 368
            if line.find('LOCK WAIT') == 0:
                results['lock_structs'] += int(row[2])
                results['locked_transactions'] += 1
            elif line.find('ROLLING BACK') == 0:
                # ROLLING BACK 127539 lock struct(s), heap size 15201832,
                # 4411492 row lock(s), undo log entries 1042488
                results['lock_structs'] += int(row[2])
            else:
                results['lock_structs'] += int(row[0])

        # FILE I/O
        elif line.find(' OS file reads, ') > 0:
            # 8782182 OS file reads, 15635445 OS file writes, 947800 OS
            # fsyncs
            results['os_file_reads'] = int(row[0])
            results['os_file_writes'] = int(row[4])
            results['os_file_fsyncs'] = int(row[8])
        elif line.find('Pending normal aio reads:') == 0:
            try:
                if len(row) == 8:
                    # (len(row) == 8)  Pending normal aio reads: 0, aio
                    # writes: 0,
                    results['pending_normal_aio_reads'] = int(row[4])
                    results['pending_normal_aio_writes'] = int(row[7])
                elif len(row) == 14:
                    # (len(row) == 14) Pending normal aio reads: 0 [0, 0] ,
                    # aio writes: 0 [0, 0] ,
                    results['pending_normal_aio_reads'] = int(row[4])
                    results['pending_normal_aio_writes'] = int(row[10])
                elif len(row) == 16:
                    # (len(row) == 16) Pending normal aio reads:
                    # [0, 0, 0, 0] , aio writes: [0, 0, 0, 0] ,
                    if all(v.isdigit() for v in row[4:8]) and \
                            all(v.isdigit() for v in row[11:15]):
                        results['pending_normal_aio_reads'] = sum(
                            map(int, (row[4], row[5], row[6], row[7]))
                        )
                        results['pending_normal_aio_writes'] = sum(
                            map(int, (row[11], row[12], row[13], row[14]))
                        )

                    # (len(row) == 16) Pending normal aio reads: 0
                    # [0, 0, 0, 0] , aio writes: 0 [0, 0] ,
                    elif all(v.isdigit() for v in row[4:9]) and \
                            all(v.isdigit() for v in row[12:15]):
                        results['pending_normal_aio_reads'] = int(row[4])
                        results['pending_normal_aio_writes'] = int(row[12])
                    else:
                        logging.warning(f"Can't parse result line {line}")
                elif len(row) == 18:
                    # (len(row) == 18) Pending normal aio reads: 0
                    # [0, 0, 0, 0] , aio writes: 0 [0, 0, 0, 0] ,
                    results['pending_normal_aio_reads'] = int(row[4])
                    results['pending_normal_aio_writes'] = int(row[12])
                elif len(row) == 22:
                    # (len(row) == 22)
                    # Pending normal aio reads: 0 [0, 0, 0, 0, 0, 0, 0, 0] ,
                    # aio writes: 0 [0, 0, 0, 0] ,
                    results['pending_normal_aio_reads'] = int(row[4])
                    results['pending_normal_aio_writes'] = int(row[16])
            except ValueError as e:
                logging.warning(f"Can't parse result line {line}: {e}")
        elif line.find('ibuf aio reads') == 0:
            #  ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
            #  or ibuf aio reads:, log i/o's:, sync i/o's:
            if len(row) == 10:
                results['pending_ibuf_aio_reads'] = int(row[3])
                results['pending_aio_log_ios'] = int(row[6])
                results['pending_aio_sync_ios'] = int(row[9])
            elif len(row) == 7:
                results['pending_ibuf_aio_reads'] = 0
                results['pending_aio_log_ios'] = 0
                results['pending_aio_sync_ios'] = 0
        elif line.find('Pending flushes (fsync)') == 0:
            if len(row) == 4:
                # Pending flushes (fsync): 0
                results['pending_buffer_pool_flushes'] = int(row[3])
            else:
                # Pending flushes (fsync) log: 0; buffer pool: 0
                results['pending_log_flushes'] = int(row[4])
                results['pending_buffer_pool_flushes'] = int(row[7])

        # INSERT BUFFER AND ADAPTIVE HASH INDEX
        elif line.find('Ibuf for space 0: size ') == 0:
            # Older InnoDB code seemed to be ready for an ibuf per tablespace.
            # It had two lines in the output.  Newer has just one line, see
            # below.
            # Ibuf for space 0: size 1, free list len 887, seg size 889, is
            # not empty
            # Ibuf for space 0: size 1, free list len 887, seg size 889,
            results['ibuf_size'] = int(row[5])
            results['ibuf_free_list'] = int(row[9])
            results['ibuf_segment_size'] = int(row[12])
        elif line.find('Ibuf: size ') == 0:
            # Ibuf: size 1, free list len 4634, seg size 4636,
            results['ibuf_size'] = int(row[2])
            results['ibuf_free_list'] = int(row[6])
            results['ibuf_segment_size'] = int(row[9])

            if line.find('merges') > -1:
                results['ibuf_merges'] = int(row[10])
        elif line.find(', delete mark ') > 0 and \
                prev_line.find('merged operations:') == 0:
            # Output of show engine innodb status has changed in 5.5
            # merged operations:
            # insert 593983, delete mark 387006, delete 73092
            results['ibuf_merged_inserts'] = int(row[1])
            results['ibuf_merged_delete_marks'] = int(row[4])
            results['ibuf_merged_deletes'] = int(row[6])
            results['ibuf_merged'] = sum(
                map(int, [row[1], row[4], row[6]]))
        elif line.find(' merged recs, ') > 0:
            # 19817685 inserts, 19817684 merged recs, 3552620 merges
            results['ibuf_merged_inserts'] = int(row[0])
            results['ibuf_merged'] = int(row[2])
            results['ibuf_merges'] = int(row[5])
        elif line.find('Hash table size ') == 0:
            # In some versions of InnoDB, the used cells is omitted.
            # Hash table size 4425293, used cells 4229064, ....
            # Hash table size 57374437, node heap has 72964 buffer(s) <--
            # no used cells
            results['hash_index_cells_total'] = int(row[3])
            results['hash_index_cells_used'] = int(row[6]) \
                if line.find('used cells') > 0 else 0

        # LOG
        elif line.find(" log i/o's done, ") > 0:
            # 3430041 log i/o's done, 17.44 log i/o's/second
            # 520835887 log i/o's done, 17.28 log i/o's/second, 518724686
            # syncs, 2980893 checkpoints
            results['log_writes'] = int(row[0])
        elif line.find(" pending log writes, ") > 0:
            # 0 pending log writes, 0 pending chkp writes
            results['pending_log_writes'] = int(row[0])
            results['pending_checkpoint_writes'] = int(row[4])
        elif line.find("Log sequence number") == 0:
            # This number is NOT printed in hex in InnoDB plugin.
            # Log sequence number 272588624
            results['lsn_current'] = int(row[3])
        elif line.find("Log flushed up to") == 0:
            # This number is NOT printed in hex in InnoDB plugin.
            # Log flushed up to   272588624
            results['lsn_flushed'] = int(row[4])
        elif line.find("Last checkpoint at") == 0:
            # Last checkpoint at  272588624
            results['lsn_last_checkpoint'] = int(row[3])

        # BUFFER POOL AND MEMORY
        elif line.find("Total memory allocated") == 0 and \
                line.find("in additional pool allocated") > 0:
            # Total memory allocated 29642194944; in additional pool
            # allocated 0
            # Total memory allocated by read views 96
            results['mem_total'] = int(row[3])
            results['mem_additional_pool'] = int(row[8])
        elif line.find('Adaptive hash index ') == 0:
            #   Adaptive hash index 1538240664     (186998824 + 1351241840)
            results['mem_adaptive_hash'] = int(row[3])
        elif line.find('Page hash           ') == 0:
            #   Page hash           11688584
            results['mem_page_hash'] = int(row[2])
        elif line.find('Dictionary cache    ') == 0:
            #   Dictionary cache    145525560      (140250984 + 5274576)
            results['mem_dictionary'] = int(row[2])
        elif line.find('File system         ') == 0:
            #   File system         313848         (82672 + 231176)
            results['mem_file_system'] = int(row[2])
        elif line.find('Lock system         ') == 0:
            #   Lock system         29232616       (29219368 + 13248)
            results['mem_lock_system'] = int(row[2])
        elif line.find('Recovery system     ') == 0:
            #   Recovery system     0      (0 + 0)
            results['mem_recovery_system'] = int(row[2])
        elif line.find('Threads             ') == 0:
            #   Threads             409336         (406936 + 2400)
            results['mem_thread_hash'] = int(row[1])
        elif line.find("Buffer pool size ") == 0:
            # The " " after size is necessary to avoid matching the wrong line:
            # Buffer pool size        1769471
            # Buffer pool size, bytes 28991012864
            if buffer_id == -1:
                results['buffer_pool_pages_total'] = int(row[3])
        elif line.find("Free buffers") == 0:
            # Free buffers            0
            if buffer_id == -1:
                results['buffer_pool_pages_free'] = int(row[2])
        elif line.find("Database pages") == 0:
            # Database pages          1696503
            if buffer_id == -1:
                results['buffer_pool_pages_data'] = int(row[2])

        elif line.find("Modified db pages") == 0:
            # Modified db pages       160602
            if buffer_id == -1:
                results['buffer_pool_pages_dirty'] = int(row[3])
        elif line.find("Pages read ahead") == 0:
            # Must do this BEFORE the next test, otherwise it'll get fooled by
            # this line from the new plugin:
            # Pages read ahead 0.00/s, evicted without access 0.06/s
            pass
        elif line.find("Pages read") == 0:
            # Pages read 15240822, created 1770238, written 21705836
            if buffer_id == -1:
                results['pages_read'] = int(row[2])
                results['pages_created'] = int(row[4])
                results['pages_written'] = int(row[6])

        # ROW OPERATIONS
        elif line.find('Number of rows inserted') == 0:
            # Number of rows inserted 50678311, updated 66425915, deleted
            # 20605903, read 454561562
            results['rows_inserted'] = int(row[4])
            results['rows_updated'] = int(row[6])
            results['rows_deleted'] = int(row[8])
            results['rows_read'] = int(row[10])
        elif line.find(" queries inside InnoDB, ") > 0:
            # 0 queries inside InnoDB, 0 queries in queue
            results['queries_inside'] = int(row[0])
            results['queries_queued'] = int(row[4])

        prev_line = line

    # We need to calculate this metric separately
    try:
        results['checkpoint_age'] = results['lsn_current'] - \
            results['lsn_last_checkpoint']
    except KeyError as e:
        logging.error(
            f"Not all InnoDB LSN metrics available, unable to compute: {e}")

    try:
        innodb_page_size = results['page_size']
        innodb_buffer_pool_pages_used = (
            results['buffer_pool_pages_total'] -
            results['buffer_pool_pages_free']
        )

        if 'buffer_pool_bytes_data' not in results:
            results['buffer_pool_bytes_data'] = \
                results['buffer_pool_pages_data'] * innodb_page_size

        if 'buffer_pool_bytes_dirty' not in results:
            results['buffer_pool_bytes_dirty'] = \
                results['buffer_pool_pages_dirty'] * innodb_page_size

        if 'buffer_pool_bytes_free' not in results:
            results['buffer_pool_bytes_free'] = \
                results['buffer_pool_pages_free'] * innodb_page_size

        if 'buffer_pool_bytes_total' not in results:
            results['buffer_pool_bytes_total'] = \
                results['buffer_pool_pages_total'] * innodb_page_size

        if 'buffer_pool_pages_utilization' not in results:
            results['buffer_pool_pages_utilization'] = (
                innodb_buffer_pool_pages_used /
                results['buffer_pool_pages_total']
            )
        else:
            # Ensure float
            results['buffer_pool_pages_total'] = \
                float(results['buffer_pool_pages_total'])

        if 'buffer_pool_bytes_used' not in results:
            results['buffer_pool_bytes_used'] = \
                innodb_buffer_pool_pages_used * innodb_page_size
    except (KeyError, TypeError) as e:
        logging.error("Not all InnoDB buffer pool metrics are available, "
                      f"unable to compute: {e}")

    return results


async def check_innodb(
        asset: Asset,
        asset_config: dict,
        config: dict) -> dict:

    conn = await get_conn(asset, asset_config, config)
    try:
        res = await query(conn, QUERY_HAS_INNODB)
        if len(res) == 0:
            raise IgnoreCheckException
        res = await query(conn, "SHOW /*!50000 ENGINE*/ INNODB STATUS")
        assert len(res), 'no INNODB STATUS metrics found'
        stats = get_stats_from_innodb_status(res[0]['Status'])
        stats['name'] = 'innodb'
    finally:
        conn.close()

    return {
        'innodb': [stats]
    }
