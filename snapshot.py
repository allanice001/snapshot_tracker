#!/usr/bin/python
import time
from boto.ec2 import connect_to_region
from daemon import Daemon
from boto.dynamodb2.table import Table
from datetime import datetime


class Snapshot(Daemon):
    def main(self):
        ec2 = connect_to_region("us-east-1", validate_certs=False)
        ddb = Table('ti_backups')
        reservations = ec2.get_all_instances()

        """
        Apply a "Backup" Tag, and set value to "on" - look at acpnat01 where this tag has been created
        Then replace the above reservations line to only filter instances where this tag is present

        reservations = ec2.get_all_instances(filters={"tag-key":"Backup","tag-value":"on"})
        """

        instances = []
        for r in reservations:
            instances.extend(r.instances)

        for i in instances:
            volumes = [v for v in ec2.get_all_volumes() if v.attach_data.instance_id == i.id]
            for v in volumes:
                snap = ec2.create_snapshot(v.attach_data.id, v.attach_data.instance_id)

                """
                Lets add tags to the new snapshots
                snap.add_tags({'foo': 'bar', 'fie': 'bas'})
                """

                snap_prog = ec2.get_all_snapshots(snapshot_ids=snap.id)

                for s in snap_prog:
                    while s.status == 'pending':
                        time.sleep(2)
                        snap_progress = ec2.get_all_snapshots(snapshot_ids=snap.id)
                        for snap in snap_progress:
                            if snap.status != 'pending':
                                s.status = snap.status
                    date = datetime.utcnow()
                    date = date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    ddb.put_item(data={'instance_id': i.id, 'snap_id': snap.id,'create_date': date})

    def run(self):
        self.main()

if __name__ == '__main__':
    snapshot = Snapshot('snapshot.pid')
    snapshot.main()