/*
 * This file was copied from the same file from the HTTP connector
 * on the apache Pulsar repo. The original file was licensed under
 * the Apache License, Version 2.0.
 *
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/**
 * Convert an AVRO GenericRecord to a JsonNode.
 */
public class JsonConverter {

    private static final Map<String, LogicalTypeConverter<?>> logicalTypeConverters = new HashMap<>();
    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);

    public static JsonNode topLevelMerge(final JsonNode n1, final JsonNode n2) {
        final var objectNode = jsonNodeFactory.objectNode();
        n1.fieldNames().forEachRemaining(f -> objectNode.set(f, n1.get(f)));
        n2.fieldNames().forEachRemaining(f -> objectNode.replace(f, n2.get(f)));
        return objectNode;
    }

    public static ObjectNode toJson(final GenericRecord genericRecord) {
        if (genericRecord == null) {
            return null;
        }
        final var objectNode = jsonNodeFactory.objectNode();
        for (final Schema.Field field : genericRecord.getSchema().getFields()) {
            objectNode.set(field.name(), toJson(field.schema(), genericRecord.get(field.name())));
        }
        return objectNode;
    }

    public static JsonNode toJson(final Schema schema, final Object value) {
        if (schema.getLogicalType() != null && logicalTypeConverters.containsKey(schema.getLogicalType().getName())) {
            return logicalTypeConverters.get(schema.getLogicalType().getName()).toJson(schema, value);
        }
        if (value == null) {
            return jsonNodeFactory.nullNode();
        }
        switch(schema.getType()) {
            case NULL: // this should not happen
                return jsonNodeFactory.nullNode();
            case INT:
                return jsonNodeFactory.numberNode((Integer) value);
            case LONG:
                return jsonNodeFactory.numberNode((Long) value);
            case DOUBLE:
                return jsonNodeFactory.numberNode((Double) value);
            case FLOAT:
                return jsonNodeFactory.numberNode((Float) value);
            case BOOLEAN:
                return jsonNodeFactory.booleanNode((Boolean) value);
            case BYTES:
                return jsonNodeFactory.binaryNode((byte[]) value);
            case FIXED:
                return jsonNodeFactory.binaryNode(((GenericFixed) value).bytes());
            case ENUM: // GenericEnumSymbol
            case STRING:
                return jsonNodeFactory.textNode(value.toString()); // can be a String or org.apache.avro.util.Utf8
            case ARRAY: {
                final var elementSchema = schema.getElementType();
                final var arrayNode = jsonNodeFactory.arrayNode();
                Object[] iterable;
                if (value instanceof GenericData.Array) {
                    iterable = ((GenericData.Array<?>) value).toArray();
                } else {
                    iterable = (Object[]) value;
                }
                for (final var elem : iterable) {
                    final var fieldValue = toJson(elementSchema, elem);
                    arrayNode.add(fieldValue);
                }
                return arrayNode;
            }
            case MAP: {
                final var map = (Map<?, ?>) value;
                final var objectNode = jsonNodeFactory.objectNode();
                for (final var entry : map.entrySet()) {
                    final var jsonNode = toJson(schema.getValueType(), entry.getValue());
                    // can be a String or org.apache.avro.util.Utf8
                    final var entryKey = entry.getKey() == null ? null : entry.getKey().toString();
                    objectNode.set(entryKey, jsonNode);
                }
                return objectNode;
            }
            case RECORD:
                return toJson((GenericRecord) value);
            case UNION:
                for (final var s : schema.getTypes()) {
                    if (s.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return toJson(s, value);
                }
                // this case should not happen
                return jsonNodeFactory.textNode(value.toString());
            default:
                throw new UnsupportedOperationException("Unknown AVRO schema type=" + schema.getType());
        }
    }

    abstract static class LogicalTypeConverter<T> {
        final Conversion<T> conversion;

        public LogicalTypeConverter(final Conversion<T> conversion) {
            this.conversion = conversion;
        }

        abstract JsonNode toJson(final Schema schema, final Object value);
    }

    static {
        logicalTypeConverters.put("decimal", new LogicalTypeConverter<BigDecimal>(
                new Conversions.DecimalConversion()) {
            @Override
            JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof BigDecimal)) {
                    throw new IllegalArgumentException("Invalid type for Decimal, expected BigDecimal but was "
                            + value.getClass());
                }
                final var decimal = (BigDecimal) value;
                return jsonNodeFactory.numberNode(decimal);
            }
        });
        logicalTypeConverters.put("date", new LogicalTypeConverter<LocalDate>(
                new TimeConversions.DateConversion()) {
            @Override
            JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("Invalid type for date, expected Integer but was "
                            + value.getClass());
                }
                final var daysFromEpoch = (Integer) value;
                return jsonNodeFactory.numberNode(daysFromEpoch);
            }
        });
        logicalTypeConverters.put("time-millis", new LogicalTypeConverter<LocalTime>(
                new TimeConversions.TimeMillisConversion()) {
            @Override
            JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("Invalid type for time-millis, expected Integer but was "
                            + value.getClass());
                }
                final var timeMillis = (Integer) value;
                return jsonNodeFactory.numberNode(timeMillis);
            }
        });
        logicalTypeConverters.put("time-micros", new LogicalTypeConverter<LocalTime>(
                new TimeConversions.TimeMicrosConversion()) {
            @Override
            JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException("Invalid type for time-micros, expected Long but was "
                            + value.getClass());
                }
                final var timeMicro = (Long) value;
                return jsonNodeFactory.numberNode(timeMicro);
            }
        });
        logicalTypeConverters.put("timestamp-millis", new LogicalTypeConverter<Instant>(
                new TimeConversions.TimestampMillisConversion()) {
            @Override
            JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException("Invalid type for timestamp-millis, expected Long but was "
                            + value.getClass());
                }
                return jsonNodeFactory.numberNode((Long) value);
            }
        });
        logicalTypeConverters.put("timestamp-micros", new LogicalTypeConverter<Instant>(
                new TimeConversions.TimestampMicrosConversion()) {
            @Override
            JsonNode toJson(final Schema schema, final Object value) {
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException("Invalid type for timestamp-micros, expected Long but was "
                            + value.getClass());
                }
                return jsonNodeFactory.numberNode((Long) value);
            }
        });
        logicalTypeConverters.put("uuid", new LogicalTypeConverter<UUID>(
                new Conversions.UUIDConversion()) {
            @Override
            JsonNode toJson(final Schema schema, final Object value) {
                return jsonNodeFactory.textNode(value == null ? null : value.toString());
            }
        });
    }

    public static ArrayNode toJsonArray(final JsonNode jsonNode, final List<String> fields) {
        final var arrayNode = jsonNodeFactory.arrayNode();
        final var it = jsonNode.fieldNames();
        while (it.hasNext()) {
            final var fieldName = it.next();
            if (fields.contains(fieldName)) {
                arrayNode.add(jsonNode.get(fieldName));
            }
        }
        return arrayNode;
    }

}
