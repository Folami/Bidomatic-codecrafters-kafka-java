# Kafka Protocol Documentation

This directory contains documentation about the Kafka protocol and implementation details for the Kafka clone project.

## Files

- **kafka-cluster-metadata-summary.md**: Detailed information about Kafka's cluster metadata format stored in the `__cluster_metadata` topic log file.

- **kafka-describe-topic-partitions-api-summary.md**: Summary of the DescribeTopicPartitions API (key 75) request and response formats.

- **kafka-implementation-guide.md**: Practical implementation guide with code examples for implementing the DescribeTopicPartitions API.

## Purpose

These documents serve as a reference for understanding and implementing Kafka's protocol in our clone project. They provide both theoretical information about the protocol formats and practical implementation details.

## References

- [Kafka Protocol Documentation](https://kafka.apache.org/protocol.html)
- [Kafka Metadata Definitions](https://github.com/apache/kafka/tree/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata)
- [Kafka Cluster Metadata Format](https://binspec.org/kafka-cluster-metadata)
- [DescribeTopicPartitions Response Format](https://binspec.org/kafka-describe-topic-partitions-response-v0)
