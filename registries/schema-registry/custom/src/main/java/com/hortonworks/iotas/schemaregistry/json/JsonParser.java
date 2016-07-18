/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.hortonworks.iotas.schemaregistry.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.common.exception.ParserException;
import com.hortonworks.iotas.schemaregistry.BaseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * A simple json parser that uses {@link ObjectMapper} to parse
 * json to a Map&lt;String, Object&gt;
 */
public class JsonParser extends BaseParser {
    private static final String VERSION = "1.0";
    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);

    private Schema schema = null;
    private final ObjectMapper mapper = new ObjectMapper();

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    /**
     * For self describing data formats like Json we can construct the schema from
     * sample data.
     *
     * @param sampleJsonData
     * @throws ParserException
     */
    public void setSchema(String sampleJsonData) throws ParserException {
        this.schema = schemaFromSampleData(sampleJsonData);
    }

    public String version() {
        return VERSION;
    }

    public Schema schema() {
        return schema;
    }

    public Map<String, Object> parse(byte[] data) throws ParserException {
        try {
            return mapper.readValue(data, new TypeReference<Map<String, Object>>(){});
        } catch (IOException e) {
            throw new ParserException("Error trying to parse data.", e);
        }
    }
}
