/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Json serializer and deserializer for {@link DatabaseRegistration}. */
public class DatabaseRegistrationJsonSerde
        implements JsonSerializer<DatabaseRegistration>, JsonDeserializer<DatabaseRegistration> {

    public static final DatabaseRegistrationJsonSerde INSTANCE =
            new DatabaseRegistrationJsonSerde();

    static final String COMMENT_NAME = "comment";
    static final String PROPERTIES_NAME = "properties";

    static final String CREATE_TIME = "create_time";
    static final String MODIFY_TIME = "modify_time";
    private static final String VERSION_KEY = "version";
    private static final int VERSION = 1;

    @Override
    public void serialize(DatabaseRegistration tableReg, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, VERSION);

        // serialize comment.
        if (tableReg.comment != null) {
            generator.writeStringField(COMMENT_NAME, tableReg.comment);
        }

        // serialize properties.
        generator.writeObjectFieldStart(PROPERTIES_NAME);
        for (Map.Entry<String, String> entry : tableReg.properties.entrySet()) {
            generator.writeObjectField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();

        // serialize create time.
        generator.writeNumberField(CREATE_TIME, tableReg.createTime);
        // serialize modify time.
        generator.writeNumberField(MODIFY_TIME, tableReg.modifyTime);

        generator.writeEndObject();
    }

    @Override
    public DatabaseRegistration deserialize(JsonNode node) {
        JsonNode commentNode = node.get(COMMENT_NAME);
        String comment = null;
        if (commentNode != null) {
            comment = commentNode.asText();
        }

        Map<String, String> properties = deserializeProperties(node.get(PROPERTIES_NAME));
        long createTime = node.get(CREATE_TIME).asLong(-1);
        long modifyTime = node.get(MODIFY_TIME).asLong(-1);

        return new DatabaseRegistration(comment, properties, createTime, modifyTime);
    }

    private Map<String, String> deserializeProperties(JsonNode node) {
        HashMap<String, String> properties = new HashMap<>();
        Iterator<String> optionsKeys = node.fieldNames();
        while (optionsKeys.hasNext()) {
            String key = optionsKeys.next();
            properties.put(key, node.get(key).asText());
        }
        return properties;
    }
}