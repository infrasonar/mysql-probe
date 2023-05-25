from libprobe.probe import Probe
from lib.check.innodb import check_innodb
from lib.check.mysql import check_mysql
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = {
        'innodb': check_innodb,
        'mysql': check_mysql,
    }

    probe = Probe("mysql", version, checks)

    probe.start()
