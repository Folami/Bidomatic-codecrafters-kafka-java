# Kafka DescribeTopicPartitions API Summary

## Overview
This document summarizes the DescribeTopicPartitions API (key 75) request and response formats.

## Request Format (v0)

| Component | Type | Description |
|-----------|------|-------------|
| **Request Header** | - | Standard Kafka request header |
| Correlation ID | INT32 | Client-generated identifier |
| Client ID | STRING | Client identifier |
| Tagged Fields | COMPACT_BYTES | Flexible protocol fields |
| **Request Body** | - | - |
| Topics | COMPACT_ARRAY | Array of topics to describe |
| Topic Name | COMPACT_STRING | Name of the topic |
| Partitions | COMPACT_ARRAY | Array of partition IDs (optional) |
| Partition ID | INT32 | ID of the partition |
| Cursor | NULLABLE_STRING | Pagination cursor |
| Tagged Fields | COMPACT_BYTES | Flexible protocol fields |

## Response Format (v0)

| Component | Type | Description |
|-----------|------|-------------|
| **Response Header** | - | Standard Kafka response header |
| Correlation ID | INT32 | Matches request correlation ID |
| Tagged Fields | COMPACT_BYTES | Flexible protocol fields |
| **Response Body** | - | - |
| Throttle Time (ms) | INT32 | Time client should throttle requests (typically 0) |
| Topics | COMPACT_ARRAY | Array of topic information |
| Error Code | INT16 | 0: Success, 3: UNKNOWN_TOPIC_OR_PARTITION |
| Topic Name | COMPACT_STRING | Name of the topic |
| Topic ID | UUID | 16-byte unique identifier for the topic |
| Is Internal | BOOLEAN | Whether topic is internal |
| Partitions | COMPACT_ARRAY | Array of partition information |
| Error Code | INT16 | 0: Success, other: Error |
| Partition Index | INT32 | Partition ID |
| Leader ID | INT32 | Broker ID of the leader |
| Leader Epoch | INT32 | Current leader epoch |
| Replica Nodes | COMPACT_ARRAY of INT32 | Broker IDs of all replicas |
| ISR Nodes | COMPACT_ARRAY of INT32 | Broker IDs of in-sync replicas |
| Offline Replicas | COMPACT_ARRAY of INT32 | Broker IDs of offline replicas |
| Tagged Fields | COMPACT_BYTES | Flexible protocol fields |
| Topic Authorized Operations | INT32 | Bitmap of allowed operations (typically 0x00000DF8) |
| Tagged Fields | COMPACT_BYTES | Flexible protocol fields |
| Next Cursor | NULLABLE_STRING | Pagination cursor for next request |
| Tagged Fields | COMPACT_BYTES | Flexible protocol fields |

## Error Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | NONE | Success |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Topic or partition does not exist |
| 4 | LEADER_NOT_AVAILABLE | Leader not available |
| 5 | NOT_LEADER_OR_FOLLOWER | Broker not leader for partition |
| 6 | REQUEST_TIMED_OUT | Request timed out |
| 8 | REPLICA_NOT_AVAILABLE | Replica not available |
| 9 | NOT_ENOUGH_REPLICAS | Not enough replicas |
| 10 | NOT_ENOUGH_REPLICAS_AFTER_APPEND | Not enough replicas after append |
| 37 | TOPIC_AUTHORIZATION_FAILED | Topic authorization failed |

## Implementation Notes

1. When reading from the cluster metadata log file:
   - Parse the log file to extract topic and partition records
   - Match topic names to their UUIDs and partition information
   - Store this information for quick lookup when handling API requests

2. When handling DescribeTopicPartitions requests:
   - Extract the topic name from the request
   - Look up the topic in the metadata
   - If found, return success (0) with topic and partition details
   - If not found, return UNKNOWN_TOPIC_OR_PARTITION (3)

3. Response format details:
   - Use compact array format (length byte + 1)
   - For cursor, use 0xFF to indicate null cursor
   - Include all required fields in the correct order
   - Set appropriate error codes based on topic existence
