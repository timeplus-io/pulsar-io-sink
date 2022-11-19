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

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.pulsar.io.core.annotations.FieldDoc;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TimeplusSinkConfig implements Serializable {
  @FieldDoc(
  defaultValue = "",
  help = "The Timeplus host name the workspace belongs to"
  )
  private String hostname;

  @FieldDoc(
  defaultValue = "",
  help = "The workspace ID"
  )
  private String workspaceID;

  @FieldDoc(
  defaultValue = "",
  help = "The API key"
  )
  private String apiKey;

  @FieldDoc(
  defaultValue = "",
  help = "The name of the stream"
  )
  private String streamName;

  public static TimeplusSinkConfig load(Map<String, Object> map) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new ObjectMapper().writeValueAsString(map), TimeplusSinkConfig.class);
  }

  public Map<String, String> headers() {
    return Map.of(
      "Content-Type", "application/x-ndjson",
      "X-Api-Key", apiKey
    );
  }
}
