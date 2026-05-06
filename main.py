from libprobe.probe import Probe
from lib.check.innodb import CheckInnoDb
from lib.check.mysql import CheckMySql
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = (
        CheckInnoDb,
        CheckMySql,
    )

    probe = Probe("mysql", version, checks, loggers=('aiomysql',))

    probe.start()
