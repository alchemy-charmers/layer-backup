from charms.reactive import when_not, when_any, when, set_state
from charmhelpers.core import hookenv
from charms import layer
from crontab import CronTab

import tarfile
import datetime
import os
import dirsync
import logging


class Backup:
    def __init__(self):
        self.layer_options = layer.options('backup')
        self.charm_config = hookenv.config()

    def backup(self):
        if self.charm_config['backup-location'] == '':
            hookenv.action_fail("No backup-location set")
            hookenv.log('No backup location set, can\'t run backup', 'ERROR')
            return
        hookenv.log('Creating backup', 'INFO')
        if self.layer_options['backup-method'] == 'tgz':
            self._tgz_backup()
        elif self.layer_options['backup-method'] == 'sync':
            self._sync_backup()
        else:
            hookenv.action_fail("backup-method invalid")
            hookenv.log('Layer option for backup-method is not valid.', 'ERROR')
            return

    def _sync_backup(self):
        options = {'purge': True, 'create': True}
        logger = logging.getLogger('syncLogger')
        logger.setLevel(logging.ERROR)
        result = dirsync.sync(self.layer_options['backup-files'].format(**self.charm_config).strip(),
                              self.charm_config['backup-location'].strip(),
                              'sync',
                              logger=logger,
                              **options)
        hookenv.log('Files synced: {}'.format(result), 'INFO')

    def _tgz_backup(self):
        backup_file = (self.charm_config['backup-location'] + '/' +
                       self.layer_options['backup-name'] + '-{}'.format(datetime.datetime.now()))
        backup_file = backup_file.replace(':', '-') + '.tgz'
        try:
            os.mkdir(self.charm_config['backup-location'])
        except FileExistsError:
            pass

        with tarfile.open(backup_file, 'x:gz') as outFile:
            hookenv.log('Processing files: {}'.format(self.layer_options['backup-files'], 'DEBUG'))
            for addfile in self.layer_options['backup-files'].split('\n'):
                addfile = addfile.format(**self.charm_config).strip()
                outFile.add(addfile, arcname=addfile.split('/')[-1])

        # Clean up backups
        if self.charm_config['backup-count'] > 0:
            hookenv.log('Pruning files in {}'.format(self.charm_config['backup-location']), 'INFO')

            def mtime(x):
                return os.stat(os.path.join(self.charm_config['backup-location'], x)).st_mtime
            sortedFiles = sorted(os.listdir(self.charm_config['backup-location']), key=mtime)
            deleteCount = max(len(sortedFiles) - self.charm_config['backup-count'], 0)
            for file in sortedFiles[0:deleteCount]:
                os.remove(os.path.join(self.charm_config['backup-location'], file))
        else:
            hookenv.log('Skipping backup pruning', 'INFO')

    def create_backup_cron(self):
        self.remove_backup_cron(log=False)
        system_cron = CronTab(user='root')
        unit = hookenv.local_unit()
        directory = hookenv.charm_dir()
        action = directory + '/actions/backup'
        command = "juju-run {unit} {action}".format(unit=unit, action=action)
        job = system_cron.new(command=command, comment="backup cron")
        job.setall(self.charm_config['backup-cron'])
        system_cron.write()
        hookenv.log("Backup created for: {}".format(self.charm_config['backup-cron']))

    def remove_backup_cron(self, log=True):
        system_cron = CronTab(user='root')
        try:
            job = next(system_cron.find_comment("backup cron"))
            system_cron.remove(job)
            system_cron.write()
            if log:
                hookenv.log("Removed backup cron.", 'INFO')
        except StopIteration:
            if log:
                hookenv.log("Backup removal called, but cron not present.", 'WARNING')


backup = Backup()


@when_not('layer-backup.installed')
def install_layer_backup():
    if backup.charm_config['backup-location'] != '':
        backup.create_backup_cron()
    set_state('layer-backup.installed')


@when_any('config.changed.backup-location', 'config.changed.backup-cron')
@when('layer-backup.installed')
def update_backup_cron():
    if backup.charm_config['backup-location'] == '':
        backup.remove_backup_cron()
    else:
        backup.create_backup_cron()

