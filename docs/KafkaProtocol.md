# Kafka Protocol Overview

## Introduction

The Kafka protocol is a binary protocol built on top of TCP/IP that defines how clients communicate with the Kafka broker. This document provides an overview of the protocol structure and key concepts.

## Protocol Structure

### Message Format

All Kafka protocol messages follow this general structure:

```
RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32
```

Where:
- `Size`: A 4-byte integer that specifies the size of the subsequent request or response message in bytes.
- `RequestMessage` or `ResponseMessage`: The actual message content.

### Request Format

```
RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => Depends on the ApiKey
```

Where:
- `ApiKey`: Identifies the API being invoked (e.g., Produce, Fetch, etc.)
- `ApiVersion`: Specifies the version of the API being used
- `CorrelationId`: A client-generated identifier that correlates requests with responses
- `ClientId`: A user-supplied identifier for the client application
- `RequestMessage`: The specific request payload, which varies by API

### Response Format

```
ResponseMessage => CorrelationId ResponseMessage
  CorrelationId => int32
  ResponseMessage => Depends on the ApiKey
```

Where:
- `CorrelationId`: The same identifier that was provided in the request
- `ResponseMessage`: The specific response payload, which varies by API

## Primitive Types

The Kafka protocol uses the following primitive types:

| Type | Description |
|------|-------------|
| BOOLEAN | A single byte (0 = false, non-zero = true) |
| INT8 | 1-byte signed integer |
| INT16 | 2-byte signed integer in big-endian order |
| INT32 | 4-byte signed integer in big-endian order |
| INT64 | 8-byte signed integer in big-endian order |
| UINT16 | 2-byte unsigned integer in big-endian order |
| UINT32 | 4-byte unsigned integer in big-endian order |
| VARINT | Variable-length signed integer using zig-zag encoding |
| VARLONG | Variable-length signed long using zig-zag encoding |
| STRING | Length-prefixed string (INT16 length + UTF-8 bytes) |
| COMPACT_STRING | Length-prefixed string with compact encoding |
| BYTES | Length-prefixed byte array (INT32 length + bytes) |
| COMPACT_BYTES | Length-prefixed byte array with compact encoding |
| ARRAY | Length-prefixed array (INT32 length + elements) |
| COMPACT_ARRAY | Length-prefixed array with compact encoding |

## Key APIs

| API Key | Name | Description |
|---------|------|-------------|
| 0 | Produce | Send messages to the broker |
| 1 | Fetch | Fetch messages from the broker |
| 2 | ListOffsets | Get offsets for a topic/partition |
| 3 | Metadata | Get metadata about topics and partitions |
| 18 | ApiVersions | Get supported API versions |
| 75 | DescribeTopicPartitions | Get detailed information about topic partitions |

## Versioning and Compatibility

Kafka has a bidirectional client compatibility policy:
- New clients can talk to old servers
- Old clients can talk to new servers

This is achieved through API versioning. Before each request is sent, the client sends the API key and API version. The server will reject requests with unsupported versions and respond with the format expected by the client based on the version specified in the request.

## Error Handling

Responses typically include an error code field. A value of 0 indicates success, while non-zero values indicate specific errors. Common error codes include:

| Code | Name | Description |
|------|------|-------------|
| 0 | NONE | Success |
| 1 | OFFSET_OUT_OF_RANGE | Requested offset is not within range |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Server does not host this topic-partition |
| 5 | LEADER_NOT_AVAILABLE | No leader for this topic-partition |
| 6 | NOT_LEADER_OR_FOLLOWER | Broker is not the leader for this partition |
| 35 | UNSUPPORTED_VERSION | API version not supported |

## Implementation Considerations

When implementing a Kafka client or server:

1. **Versioning**: Always check API versions for compatibility
2. **Error Handling**: Properly handle error codes in responses
3. **Batching**: Batch requests when possible for better performance
4. **Connection Management**: Maintain persistent connections for efficiency
5. **Metadata Caching**: Cache metadata and refresh only when necessary

## References

- [Official Kafka Protocol Documentation](https://kafka.apache.org/protocol.html)
- [Kafka Protocol Guide](https://kafka.apache.org/documentation/#protocol)
- [KIP-482: Tagged Fields](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields)
