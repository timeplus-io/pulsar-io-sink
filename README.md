# Pulsar Sink Connector for Timeplus

[![Validation](https://github.com/timeplus-io/pulsar-io-sink/actions/workflows/validate.yml/badge.svg?branch=main)](https://github.com/timeplus-io/pulsar-io-sink/actions/workflows/validate.yml)

Note: this connector is currently on technical preview stage, it might not be optimized and is subject to change.

This connector helps you route your data in [Apache Pulsar](https://pulsar.apache.org) topics to [Timeplus](https://timeplus.com) to anaylize your data in real-time while you are streaming your data to your Pulsar topics.

To run this connector, either download the NAR file from the releases page or checkout the code and build the NAR file on your machine. And then upload the NAR file to your Pulsar worker nodes in the connectors folder, or use the `archive` parameter when you create a sink.

## Configurations

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| hostname | String | Y | | The hostname of your Timeplus workspace, currently there are two options: `beta.timeplus.cloud` or `cloud.timeplus.com.cn`. |
| workspaceID | String | Y | | The Timeplus workspace ID. |
| streamName | String | Y | | The name of the Timeplus stream you would like to send data to. |
| apiKey | String | Y | | The API key you create in your workspace to be used to send data. |
