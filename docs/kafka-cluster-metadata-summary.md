# Kafka Cluster Metadata Format

## Overview
This document summarizes the structure of Kafka's cluster metadata stored in the `__cluster_metadata` topic log file.

## Log File Structure

| Component | Description | Format Details |
|-----------|-------------|----------------|
| **Log File** | Contains a sequence of batches | Located at `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log` |
| **Batch** | Group of records with common attributes | Includes header with batch metadata |

## Batch Structure

| Field | Type | Description |
|-------|------|-------------|
| Batch Length | INT32 | Size of the batch in bytes |
| Partition Leader Epoch | INT32 | Leader epoch for the partition |
| Magic | INT8 | Message format version |
| CRC | UINT32 | Checksum for the batch |
| Attributes | INT16 | Batch attributes flags |
| Last Offset Delta | INT32 | Delta from first to last offset in batch |
| First Timestamp | INT64 | Timestamp of first record |
| Max Timestamp | INT64 | Timestamp of last record |
| Producer ID | INT64 | ID of producer that created the batch |
| Producer Epoch | INT16 | Epoch of producer that created the batch |
| Base Sequence | INT32 | First sequence number in batch |
| Records Count | INT32 | Number of records in the batch |

## Record Structure

| Field | Type | Description |
|-------|------|-------------|
| Length | VARINT | Size of the record in bytes |
| Attributes | INT8 | Record attributes |
| Timestamp Delta | VARINT | Delta from batch timestamp |
| Offset Delta | VARINT | Delta from base offset |
| Key Length | VARINT | Length of key (-1 if null) |
| Key | BYTES | Record key data |
| Value Length | VARINT | Length of value (-1 if null) |
| Value | BYTES | Record value data |
| Headers | ARRAY | Record headers |

## Metadata Record Types

| Type ID | Name | Description |
|---------|------|-------------|
| 0 | REGISTER_BROKER_RECORD | Broker registration information |
| 1 | UNREGISTER_BROKER_RECORD | Broker unregistration information |
| 2 | TOPIC_RECORD | Topic metadata |
| 3 | PARTITION_RECORD | Partition metadata |
| 4 | CONFIG_RECORD | Configuration settings |
| 5 | PARTITION_CHANGE_RECORD | Partition change information |
| 6 | FENCE_BROKER_RECORD | Broker fencing information |
| 7 | UNFENCE_BROKER_RECORD | Broker unfencing information |
| 8 | REMOVE_TOPIC_RECORD | Topic removal information |
| 9 | FEATURE_LEVEL_RECORD | Feature level information |
| 10 | CLIENT_QUOTA_RECORD | Client quota information |
| 11 | PRODUCER_IDS_RECORD | Producer ID information |
| 12 | REMOVE_PRODUCER_IDS_RECORD | Producer ID removal information |
| 13 | BROKER_REGISTRATION_CHANGE_RECORD | Broker registration change |
| 14 | ACCESS_CONTROL_RECORD | Access control information |
| 15 | REMOVE_ACCESS_CONTROL_RECORD | Access control removal |

## Topic Record (Type 2) Structure

| Field | Type | Description |
|-------|------|-------------|
| Name | STRING | Topic name |
| Topic ID | UUID (16 bytes) | Unique identifier for the topic |
| Deleted | BOOLEAN | Whether the topic is marked for deletion |
| Properties | ARRAY | Topic properties |

## Partition Record (Type 3) Structure

| Field | Type | Description |
|-------|------|-------------|
| Partition ID | INT32 | Partition identifier |
| Topic ID | UUID (16 bytes) | ID of the topic this partition belongs to |
| Replicas | ARRAY of INT32 | Broker IDs hosting replicas |
| ISR | ARRAY of INT32 | In-sync replica broker IDs |
| Removing Replicas | ARRAY of INT32 | Replicas being removed |
| Adding Replicas | ARRAY of INT32 | Replicas being added |
| Leader | INT32 | Leader broker ID |
| Leader Epoch | INT32 | Current leader epoch |
| Partition Epoch | INT32 | Current partition epoch |
