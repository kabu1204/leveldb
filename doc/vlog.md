# Value Log

## TODO

1. AsyncIterator to speedup range scan for BlobDB.
2. `class BlobDB` is handy for developing, apply changes
   to `DBImpl` after I finish everything.
3. mmap limiter for AppendableMmapFile
4. use tmp_batch_ when BuildWriterGroup to avoid modifying user's writebatch

## Overview

I've thought of 2 options to store ValueHandle(value pointer) in
the LSMTree(the original DBImpl).

The difficulty is how we judge that a value is out-of-date during GC.

My goal is to keep the original code untouched as much as possible and use
current interface.

We need to query LSMTree with the corresponding key and its SeqNumber during GC.
However, `class DB` do not provide such an interface for us to specify the
sequence number of the snapshot.

### Use key+SeqNumber as the LSMTree's key

This is quite tricky.

We just need to create another wrapper class like `class BlobDB` inherited from
`class DBImpl`. The `BlobDB::Write()` appends our own SeqNumber(say `vlog_entry_seq_number`)
to the user key, and call `ValueLogImpl::Put()` and `DBImpl::Write()`.
We can simply maintain our own SeqNumber in LSMTree by `DBImpl::Put("~VLOG:SEQNUMBER~", "123456")`.

The troublesome point is that how we deal with the deleted records?

----

Say the user called `BlobDB::Put("UKEY", "VALUExxx")` 3 times, we got 3 records in the db:

| UKEY100 | VALUE0 |
|---------|--------|
| UKEY101 | VALUE1 |
| UKEY102 | VALUE2 |

Then the user called `BlobDB::Delete("UKEY")`, how to deal with that?
How can we correctly reclaim the space of the `UKEY001`-`UKEY003` when doing compaction?

---

1. First, when `Delete`, we append the SeqNumber as usually, inserting a new record
   `<"UKEY103", DEL>`.

2. When we `Get`, we use the `UKEY104` as the key to query LSMTree, we return the
   upper bound of the key, which will be `UKEY103`.

But it's still hard to deal with the snapshot.

### Modify `class DBImpl`

1. we need a method like ``DBImpl::GetSmallesSnapshot()``,
2. we need to modify ``DBImpl::Write()`` to set seqence number before inserting to vlog,

## Garbage collection

When we scan a record <K, V, Seq>, below are cases in which we need to keep the record:

1. `Seq > db->smallest_snapshot`;
2. `ptr = db->Get(smallest_snapshot, K)`, and ptr points to the record;

### WriterGroup

#### WriteCallback