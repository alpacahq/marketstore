/**
Package commitlog implements the append-only on-disk persistency layey.

CommitLog is the on-disk persistency layer for Slait.  It is a simple append-only style format aiming
the best performance for the use case.  The idea is borrowed from Kafka (and Jocko), but we further
simpliy it to remove the index file assuming the data is cached in the main memory.

Physical layout

We assume every entry has timestamp and enforce entries to be ordered by the time in ascending
order.  Each record is encoded as follows.

- byte 0-7: timestamp of the record in Unix epoch nano seconds
- byte 8-11: the size of the payload
- byte 12-: payload byte array

The byte 0-7 and 8-11 are encoded in little endian.  There is no padding in between records.
Since the timestamp is encoded by Unix epoch nanoseconds, the maximum value for the timestamp is
somewhere around the year 2262.  The timezone info will not be considered in the physical layout,
and the restored time is always in UTC timezone.

Segment files

A partition is split into segment files.  When the newest segment is full, new record is written
into a new segment file.  A segment file is named after the base nanosecond from the first
record in the file.  When a file is trimmed, the deletion happens only at the segment level.
The maximum file size of the segment files are configured by the caller.

The module does not have any concurrency protection.  The caller should take the appropriate
action on use of this.
*/
package commitlog
