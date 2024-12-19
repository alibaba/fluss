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

package com.alibaba.fluss.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Updating properties for updating properties. */
public class UpdateProperties {
    private final List<String> resetProperties;
    private final Map<String, String> setProperties;
    private final List<String> resetCustomProperties;
    private final Map<String, String> setCustomProperties;

    private UpdateProperties(
            List<String> resetProperties,
            Map<String, String> setProperties,
            List<String> resetCustomProperties,
            Map<String, String> setCustomProperties) {
        this.resetProperties = resetProperties;
        this.setProperties = setProperties;
        this.resetCustomProperties = resetCustomProperties;
        this.setCustomProperties = setCustomProperties;
    }

    /** Creates a builder for building update property data. */
    public static Builder builder() {
        return new Builder();
    }

    public List<String> getResetProperties() {
        return resetProperties;
    }

    public Map<String, String> getSetProperties() {
        return setProperties;
    }

    public List<String> getResetCustomProperties() {
        return resetCustomProperties;
    }

    public Map<String, String> getSetCustomProperties() {
        return setCustomProperties;
    }

    /** Builder for {@link TableDescriptor}. */
    public static class Builder {
        private final List<String> resetProperties;
        private final Map<String, String> setProperties;
        private final List<String> resetCustomProperties;
        private final Map<String, String> setCustomProperties;

        protected Builder() {
            this.resetProperties = new ArrayList<>();
            this.setProperties = new HashMap<>();
            this.resetCustomProperties = new ArrayList<>();
            this.setCustomProperties = new HashMap<>();
        }

        public Builder resetProperty(String property) {
            this.resetProperties.add(property);
            return this;
        }

        public Builder setProperty(String property, String value) {
            this.setProperties.put(property, value);
            return this;
        }

        public Builder resetCustomProperty(String property) {
            this.resetCustomProperties.add(property);
            return this;
        }

        public Builder setCustomProperty(String property, String value) {
            this.setCustomProperties.put(property, value);
            return this;
        }

        public UpdateProperties build() {
            return new UpdateProperties(
                    resetProperties, setProperties, resetCustomProperties, setCustomProperties);
        }
    }
}
