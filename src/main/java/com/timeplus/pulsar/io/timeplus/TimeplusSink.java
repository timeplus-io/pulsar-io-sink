/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.timeplus.pulsar.io.timeplus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema.Type;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

public class TimeplusSink implements Sink<GenericObject> {

  private static final String TP_TIME_KEY = "_tp_time";
  private static final JsonNodeFactory jsonNodeFactory =
      JsonNodeFactory.withExactBigDecimals(false);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final TypeReference<HashMap<String, Object>> mapType =
      new TypeReference<HashMap<String, Object>>() {};

  private String protocol = "https://";
  private HttpClient httpClient;
  private ObjectMapper mapper;
  private URI uri;
  private Map<String, String> headers;

  static TimeplusSink forTest() {
    final var sink = new TimeplusSink();
    sink.protocol = "http://";
    return sink;
  }

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    TimeplusSinkConfig httpSinkConfig = TimeplusSinkConfig.load(config);
    uri = new URI(ingestURL(httpSinkConfig));
    headers = httpSinkConfig.headers();
    httpClient = HttpClient.newHttpClient();
    mapper = new ObjectMapper().registerModule(new JavaTimeModule());
  }

  @Override
  public void write(Record<GenericObject> record) throws Exception {
    Object json = toJsonSerializable(record);
    byte[] bytes = mapper.writeValueAsBytes(json);
    HttpRequest.Builder builder =
        HttpRequest.newBuilder().uri(uri).POST(HttpRequest.BodyPublishers.ofByteArray(bytes));
    headers.forEach(builder::header);

    HttpResponse<String> response =
        httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new IOException(
          String.format(
              "ingest API call failed code=%s, body=%s", response.statusCode(), response.body()));
    }
  }

  // a Record should be something that can be encoded as a JSON object
  private static Object toJsonSerializable(Record<GenericObject> record) {
    final var val = record.getValue().getNativeObject();
    final var schema = record.getSchema();

    if ((schema == null && (val instanceof String || val instanceof byte[]))
        || (schema.getSchemaInfo().getType().equals(SchemaType.STRING)
            || schema.getSchemaInfo().getType().equals(SchemaType.BYTES))) {
      final var v = val.toString();
      try {
        final var jsonMap = objectMapper.readValue(v, mapType);
        record
            .getEventTime()
            .ifPresent(
                eventTime ->
                    jsonMap.put(
                        TP_TIME_KEY,
                        JsonConverter.toJson(org.apache.avro.Schema.create(Type.LONG), eventTime)));
      } catch (JsonProcessingException e) {
        throw new UnsupportedOperationException(
            "expected in-coming string is a valid JSON object, but got err " + e);
      }
      return val;
    }

    switch (schema.getSchemaInfo().getType()) {
      case KEY_VALUE:
        final var keyValueSchema = (KeyValueSchema) schema;
        final var keyType = keyValueSchema.getKeySchema().getSchemaInfo().getType();
        // TODO allow using key to store the columns and values for data.
        if (keyType != SchemaType.STRING) {
          throw new UnsupportedOperationException(
              "key-value record must have string key, but got =" + keyType);
        }

        @SuppressWarnings("unchecked")
        final var keyValue = (org.apache.pulsar.common.schema.KeyValue<String, ?>) val;
        Map<String, Object> jsonKeyValue = new HashMap<>();
        String key = keyValue.getKey();
        Object value = keyValue.getValue();
        jsonKeyValue.put(
            key,
            toJsonSerializable(
                keyValueSchema.getValueSchema(),
                value instanceof GenericObject
                    ? ((GenericObject) value).getNativeObject()
                    : value));
        record
            .getEventTime()
            .ifPresent(
                eventTime ->
                    jsonKeyValue.put(
                        TP_TIME_KEY,
                        JsonConverter.toJson(org.apache.avro.Schema.create(Type.LONG), eventTime)));
        return jsonKeyValue;
      case AVRO:
        final var node = JsonConverter.toJson((org.apache.avro.generic.GenericRecord) val);
        record
            .getEventTime()
            .ifPresent(
                eventTime -> {
                  final var n = jsonNodeFactory.numberNode(eventTime);
                  node.set(TP_TIME_KEY, n);
                });
        return node;
      case JSON:
        final var jsonNode = (JsonNode) val;
        if (!(jsonNode instanceof ObjectNode)) {
          throw new UnsupportedOperationException(
              "JSON record must be an object, but got =" + jsonNode.getNodeType());
        }
        record
            .getEventTime()
            .ifPresent(
                eventTime -> {
                  final var n = jsonNodeFactory.numberNode(eventTime);
                  ((ObjectNode) jsonNode).set(TP_TIME_KEY, n);
                });
        return val;
      default:
        throw new UnsupportedOperationException(
            "Unsupported schema type =" + schema.getSchemaInfo().getType());
    }
    // TODO put other stuff into the json object, such as properties, messsage ID, publish time,
    // etc. (should be configurable)
  }

  private static Object toJsonSerializable(Schema<?> schema, Object val) {
    if (schema == null || schema.getSchemaInfo().getType().isPrimitive()) {
      return val;
    }
    switch (schema.getSchemaInfo().getType()) {
      case KEY_VALUE:
        KeyValueSchema<?, ?> keyValueSchema = (KeyValueSchema<?, ?>) schema;
        org.apache.pulsar.common.schema.KeyValue<?, ?> keyValue =
            (org.apache.pulsar.common.schema.KeyValue<?, ?>) val;
        Map<String, Object> jsonKeyValue = new HashMap<>();
        Object key = keyValue.getKey();
        Object value = keyValue.getValue();
        jsonKeyValue.put(
            "key",
            toJsonSerializable(
                keyValueSchema.getKeySchema(),
                key instanceof GenericObject ? ((GenericObject) key).getNativeObject() : key));
        jsonKeyValue.put(
            "value",
            toJsonSerializable(
                keyValueSchema.getValueSchema(),
                value instanceof GenericObject
                    ? ((GenericObject) value).getNativeObject()
                    : value));
        return jsonKeyValue;
      case AVRO:
        return JsonConverter.toJson((org.apache.avro.generic.GenericRecord) val);
      case JSON:
        return val;
      default:
        throw new UnsupportedOperationException(
            "Unsupported schema type =" + schema.getSchemaInfo().getType());
    }
  }

  @Override
  public void close() {}

  private String ingestURL(TimeplusSinkConfig config) {
    return protocol
        + config.getHostname()
        + "/"
        + config.getWorkspaceID()
        + "/api/v1beta1/streams/"
        + config.getStreamName()
        + "/ingest";
  }
}
