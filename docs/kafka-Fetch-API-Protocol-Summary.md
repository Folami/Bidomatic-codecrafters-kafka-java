# Kafka Fetch API Protocol Summary

This document summarizes the Kafka Fetch Request and Response messages for various API versions.

## Fetch Request

The FetchRequest is used by consumers to fetch messages from Kafka brokers.

### Common Fields (across many versions):

- **ReplicaId**: The replica ID of the consumer. For normal consumers, this is -1.
- **MaxWaitTime**: The maximum time in milliseconds to wait for the fetch to complete.
- **MinBytes**: The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait up to MaxWaitTime.
- **MaxBytes**: The maximum bytes to include in the message set for this fetch request. This helps bound the memory usage on both the server and the client.
- **IsolationLevel**: (For v4+) Controls the visibility of transactional records. 0 for READ_UNCOMMITTED, 1 for READ_COMMITTED.
- **SessionId**: (For v7+) The session ID for the fetch request, if using KIP-227 incremental fetch sessions.
- **SessionEpoch**: (For v7+) The epoch for the fetch request, if using KIP-227 incremental fetch sessions.
- **Topics**: An array of topics to fetch from.
  - **Topic**: The name of the topic.
  - **Partitions**: An array of partitions to fetch from.
    - **Partition**: The partition ID.
    - **FetchOffset**: The message offset to start fetching from.
    - **LogStartOffset**: (For v5+) The earliest available offset of the follower replica. Used by followers to truncate their log.
    - **PartitionMaxBytes**: The maximum bytes to fetch from this partition.
- **ForgottenTopicsData**: (For v7+) Topics to remove from the fetch session.
- **RackId**: (For v11+) The rack ID of the consumer.

### Version-Specific Fetch Request Notes:

- **V0**: Basic fetch functionality.
- **V1**: Adds MaxBytes.
- **V2**: Adds IsolationLevel.
- **V3**: No major structural changes from V2, often protocol refinements.
- **V4**: No major structural changes from V3.
- **V5**: Adds LogStartOffset in partition data.
- **V6**: No major structural changes from V5.
- **V7**: Introduces fetch sessions (SessionId, SessionEpoch, ForgottenTopicsData). MaxBytes at the top level becomes the default for topics if not overridden.
- **V8**: No major structural changes from V7.
- **V9**: Adds CurrentLeaderEpoch in partition data (used by followers).
- **V10**: No major structural changes from V9.
- **V11**: Adds RackId.
- **V12**: Flexible versions. Introduces `ClusterId` (string, nullable). `ReplicaId` becomes `ReplicaEpoch`.
- **V13**: Adds `ReplicaIsObsolete` boolean per partition.
- **V14**: No major structural changes from V13.
- **V15**: Adds `CurrentMetadataEpoch` (int32).
- **V16**: No major structural changes from V15.

## Fetch Response

The FetchResponse returns messages to the consumer.

### Common Fields (across many versions):

- **ThrottleTimeMs**: The duration in milliseconds for which the request was throttled due to quota violation.
- **ErrorCode**: (For v1+, at the top level) Response error code.
- **SessionId**: (For v7+) The session ID, if using fetch sessions.
- **Responses**: An array of topic responses.
  - **Topic**: The topic name.
  - **PartitionResponses**: An array of partition responses.
    - **PartitionHeader**:
      - **Partition**: The partition ID.
      - **ErrorCode**: Partition level error code.
      - **HighWatermark**: The offset of the last successfully replicated message.
      - **LastStableOffset**: (For v4+) The last stable offset (LSO) of the partition. This is the offset of the first message that has not been aborted.
      - **LogStartOffset**: (For v5+) The start offset of the log for this partition on the leader.
      - **AbortedTransactions**: (For v4+) List of aborted transactions included in the KIP-98 message set.
      - **PreferredReadReplica**: (For v11+) The preferred read replica for the consumer to use on its next fetch request.
      - **DivergingEpoch**: (For v12+)
        - **Epoch**
        - **EndOffset**
      - **CurrentLeader**: (For v12+)
        - **LeaderId**
        - **LeaderEpoch**
      - **SnapshotId**: (For v12+)
        - **EndOffset**
        - **Epoch**
    - **RecordSet**: The actual messages. The format of this depends on the message version negotiated.

### Version-Specific Fetch Response Notes:

- **V0**: Basic response. ErrorCode is per partition.
- **V1**: Adds top-level ErrorCode and ThrottleTimeMs.
- **V2**: No major structural changes from V1.
- **V3**: No major structural changes from V2.
- **V4**: Adds LastStableOffset and AbortedTransactions.
- **V5**: Adds LogStartOffset.
- **V6**: No major structural changes from V5.
- **V7**: Adds SessionId.
- **V8**: No major structural changes from V7.
- **V9**: No major structural changes from V8.
- **V10**: No major structural changes from V9.
- **V11**: Adds PreferredReadReplica.
- **V12**: Flexible versions. Adds `DivergingEpoch`, `CurrentLeader`, `SnapshotId` per partition.
- **V13**: No major structural changes from V12.
- **V14**: No major structural changes from V13.
- **V15**: No major structural changes from V14.
- **V16**: No major structural changes from V15.

---

**Note on RecordSet/Messages:**

The `RecordSet` (or `Messages` in older versions) field within each partition response contains the actual Kafka messages. The structure of these messages (e.g., message format version 0, 1, or 2) is a separate concern from the Fetch API version itself.

- **MessageSet (V0, V1, V2 etc. for older message formats):**
  - A sequence of messages, each with an offset, message size, and then the message itself.
  - Message: CRC, MagicByte, Attributes, Key, Value. (Timestamp added in later message versions).
- **RecordBatch (for KIP-98, Message Format V2):**
  - BaseOffset, BatchLength, PartitionLeaderEpoch, MagicByte (2), CRC, Attributes, LastOffsetDelta, FirstTimestamp, MaxTimestamp, ProducerId, ProducerEpoch, BaseSequence, Records (array of Record).
  - Record: Length (varint), Attributes (int8), TimestampDelta (varint), OffsetDelta (varint), KeyLength (varint), Key, ValueLength (varint), Value, Headers (array of Header).
  - Header: HeaderKeyLength (varint), HeaderKey, HeaderValueLength (varint), HeaderValue.

---

**Key Concepts Introduced in Later Versions:**

- **IsolationLevel (Request V2+, Response V4+):**

  - `READ_UNCOMMITTED` (0): Fetches all messages, including transactional messages that may have been aborted. This is the default.
  - `READ_COMMITTED` (1): Fetches only non-transactional messages or committed transactional messages. Consumers will need to buffer messages to provide transactional guarantees.
  - The response includes `LastStableOffset` (LSO) and `AbortedTransactions` to help `READ_COMMITTED` consumers filter messages.

- **Fetch Sessions (KIP-227) (Request V7+, Response V7+):**

  - Allows consumers to send incremental fetch requests, specifying only the changes from the previous request.
  - Reduces overhead by avoiding resending the full list of topics and partitions on every fetch.
  - Uses `SessionId` and `SessionEpoch`.
  - `ForgottenTopicsData` in the request allows removing topics/partitions from the session.
  - A `SessionId` of 0 indicates a new session. A non-zero `SessionId` with `SessionEpoch` -1 closes the session.

- **Rack Awareness (Request V11+):**

  - The `RackId` in the request allows brokers to try and serve fetches from the closest replica if follower fetching (KIP-392) is enabled.
  - The `PreferredReadReplica` in the response can guide the client to a better replica for future fetches.

- **Flexible Versions (V12+):**

  - Indicates the use of a more flexible message format that includes tagged fields, allowing for easier evolution of the protocol without breaking older clients for non-critical field additions.

- **Leader Epoch and Divergence (Response V12+):**
  - `DivergingEpoch`, `CurrentLeader`, `SnapshotId` fields in the response are primarily for replica synchronization and recovery, helping followers detect log divergence and truncate correctly.

This summary provides a high-level overview. For precise field definitions, data types, and detailed behavior, the official Kafka protocol documentation should always be consulted.
