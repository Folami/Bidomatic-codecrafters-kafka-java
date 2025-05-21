# DescribeTopicPartitions API (Key: 75)

## Overview
The DescribeTopicPartitions API allows clients to retrieve detailed information about topic partitions in a Kafka cluster. This API is particularly useful for clients that need to discover the structure of topics, including their partitions, leaders, and replicas.

## Request Structure

### Version 0
```
DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor _tagged_fields 
  topics => name _tagged_fields 
    name => COMPACT_STRING
  response_partition_limit => INT32
  cursor => topic_name partition_index _tagged_fields 
    topic_name => COMPACT_STRING
    partition_index => INT32
```

| Field | Type | Description |
|-------|------|-------------|
| topics | COMPACT_ARRAY | List of topics to fetch details for |
| → name | COMPACT_STRING | The topic name |
| response_partition_limit | INT32 | Maximum number of partitions to include in the response |
| cursor | OBJECT | First topic and partition to fetch details for (for pagination) |
| → topic_name | COMPACT_STRING | Name of the first topic to process |
| → partition_index | INT32 | Partition index to start with |
| _tagged_fields | COMPACT_ARRAY | Tagged fields for future protocol extensions |

## Response Structure

### Version 0
```
DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] cursor _tagged_fields 
  throttle_time_ms => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations _tagged_fields 
    error_code => INT16
    name => COMPACT_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] _tagged_fields 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
  cursor => topic_name partition_index _tagged_fields 
    topic_name => COMPACT_STRING
    partition_index => INT32
```

| Field | Type | Description |
|-------|------|-------------|
| throttle_time_ms | INT32 | Duration in milliseconds for throttling due to quota violation |
| topics | COMPACT_ARRAY | List of topics with their details |
| → error_code | INT16 | Error code for this topic, or 0 if no error |
| → name | COMPACT_STRING | The topic name |
| → topic_id | UUID | The unique topic ID |
| → is_internal | BOOLEAN | Whether the topic is internal |
| → partitions | COMPACT_ARRAY | List of partitions in this topic |
| → → error_code | INT16 | Error code for this partition, or 0 if no error |
| → → partition_index | INT32 | The partition index |
| → → leader_id | INT32 | The leader broker ID |
| → → leader_epoch | INT32 | The leader epoch |
| → → replica_nodes | COMPACT_ARRAY[INT32] | List of replica node IDs |
| → → isr_nodes | COMPACT_ARRAY[INT32] | List of in-sync replica node IDs |
| → → offline_replicas | COMPACT_ARRAY[INT32] | List of offline replica node IDs |
| → topic_authorized_operations | INT32 | 32-bit bitfield of authorized operations for this topic |
| cursor | OBJECT | Cursor for pagination |
| → topic_name | COMPACT_STRING | Name of the next topic to process |
| → partition_index | INT32 | Next partition index to process |
| _tagged_fields | COMPACT_ARRAY | Tagged fields for future protocol extensions |

## Common Error Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | NONE | Success |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | This server does not host this topic-partition |
| 100 | UNKNOWN_TOPIC_ID | This server does not host this topic ID |
| 103 | INCONSISTENT_TOPIC_ID | The log's topic ID did not match the topic ID in the request |

## Usage Examples

### Requesting Information for a Single Topic
```
Request:
  topics: ["my-topic"]
  response_partition_limit: 100
  cursor: null

Response:
  topics: [
    {
      error_code: 0,
      name: "my-topic",
      topic_id: <UUID>,
      is_internal: false,
      partitions: [
        {
          error_code: 0,
          partition_index: 0,
          leader_id: 1,
          leader_epoch: 5,
          replica_nodes: [1, 2, 3],
          isr_nodes: [1, 2, 3],
          offline_replicas: []
        },
        ...
      ]
    }
  ]
```

### Requesting Information for Multiple Topics
```
Request:
  topics: ["topic1", "topic2", "topic3"]
  response_partition_limit: 100
  cursor: null

Response:
  topics: [
    {
      error_code: 0,
      name: "topic1",
      topic_id: <UUID>,
      is_internal: false,
      partitions: [...]
    },
    {
      error_code: 0,
      name: "topic2",
      topic_id: <UUID>,
      is_internal: false,
      partitions: [...]
    },
    {
      error_code: 0,
      name: "topic3",
      topic_id: <UUID>,
      is_internal: false,
      partitions: [...]
    }
  ]
```

## Implementation Notes

1. The API supports pagination through the cursor mechanism, allowing clients to retrieve information about large numbers of topics and partitions in manageable chunks.
2. The response includes detailed information about each partition, including the leader, replicas, and in-sync replicas.
3. The API was introduced in Kafka 3.0 with API key 75.
4. The request and response use the compact format for strings and arrays, which is more efficient than the older non-compact format.
5. The API supports tagged fields, which allows for future extensions without breaking backward compatibility.
