from libprobe.probe import Probe
from lib.check.innodb import check_innodb
from lib.check.mysql import check_mysql
from lib.check.system import check_system
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = {
        'innodb': check_innodb,
        'mysql': check_mysql,
        'system': check_system,
    }

    probe = Probe("mysql", version, checks)

    probe.start()
