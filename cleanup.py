#!/usr/bin/python


"""
NOTE
Running this will delete ALL EC2 snapshots older than $retention_days

"""
import sys
from boto.ec2 import connect_to_region
from datetime import datetime, timedelta

# Specify retention days as argument, or default to 7
try:
    days = int(sys.argv[1])
except IndexError:
    days = 7

delete_time = datetime.utcnow() - timedelta(days=days)

"""
Implement filter tags

filters = {
    'tag-key': 'Backup'
}

"""


print 'Deleting any snapshots older than {days} days'.format(days=days)

ec2 = connect_to_region("us-east-1")
snapshots = ec2.get_all_snapshots()

"""
With the filter, the above line should become::
snapshots = ec2.get_all_snapshots(filters=filters)
"""

deletion_counter = 0
size_counter = 0

for snapshot in snapshots:
    start_time = datetime.strptime(
        snapshot.start_time,
        '%Y-%m-%dT%H:%M:%S.000Z'
    )

    if start_time < delete_time:
        print 'Deleting {id}'.format(id=snapshot.id)
        deletion_counter = deletion_counter + 1
        size_counter = size_counter + snapshot.volume_size
        # Just to make sure you're reading!
        #snapshot.delete(dry_run=True)


print 'Deleted {number} snapshots totalling {size} GB'.format(
    number=deletion_counter,
    size=size_counter
)