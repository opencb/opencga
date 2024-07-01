/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.config;

import java.util.Objects;

/**
 * Created by imedina on 22/05/16.
 */
public class RestServerConfiguration extends AbstractServerConfiguration {

    public HttpConfiguration httpConfiguration = new HttpConfiguration();
    public RestServerConfiguration() {
    }

    public RestServerConfiguration(int port) {
        super(port);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RestServerConfiguration{");
        sb.append("port=").append(port);
        sb.append(", httpConfiguration=").append(httpConfiguration);
        sb.append('}');
        return sb.toString();
    }

    @Deprecated
    protected void setDefaultLimit(Object o) {
        Configuration.reportUnusedField("configuration.yml#server.rest.defaultLimit", o);
    }

    @Deprecated
    protected void setMaxLimit(Object o) {
        Configuration.reportUnusedField("configuration.yml#server.rest.maxLimit", o);
    }

    public HttpConfiguration getHttpConfiguration() {
        return httpConfiguration;
    }

    public RestServerConfiguration setHttpConfiguration(HttpConfiguration httpConfiguration) {
        this.httpConfiguration = httpConfiguration;
        return this;
    }

    public static class HttpConfiguration {
        private int outputBufferSize = -1;
        private int outputAggregationSize = -1;
        private int requestHeaderSize = -1;
        private int responseHeaderSize = -1;
        private int headerCacheSize = -1;

        public int getOutputBufferSize() {
            return outputBufferSize;
        }

        public HttpConfiguration setOutputBufferSize(int outputBufferSize) {
            this.outputBufferSize = outputBufferSize;
            return this;
        }

        public int getOutputAggregationSize() {
            return outputAggregationSize;
        }

        public HttpConfiguration setOutputAggregationSize(int outputAggregationSize) {
            this.outputAggregationSize = outputAggregationSize;
            return this;
        }

        public int getRequestHeaderSize() {
            return requestHeaderSize;
        }

        public HttpConfiguration setRequestHeaderSize(int requestHeaderSize) {
            this.requestHeaderSize = requestHeaderSize;
            return this;
        }

        public int getResponseHeaderSize() {
            return responseHeaderSize;
        }

        public HttpConfiguration setResponseHeaderSize(int responseHeaderSize) {
            this.responseHeaderSize = responseHeaderSize;
            return this;
        }

        public int getHeaderCacheSize() {
            return headerCacheSize;
        }

        public HttpConfiguration setHeaderCacheSize(int headerCacheSize) {
            this.headerCacheSize = headerCacheSize;
            return this;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("HttpConfiguration{");
            sb.append("outputBufferSize=").append(outputBufferSize);
            sb.append(", outputAggregationSize=").append(outputAggregationSize);
            sb.append(", requestHeaderSize=").append(requestHeaderSize);
            sb.append(", responseHeaderSize=").append(responseHeaderSize);
            sb.append(", headerCacheSize=").append(headerCacheSize);
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HttpConfiguration that = (HttpConfiguration) o;
            return outputBufferSize == that.outputBufferSize &&
                    outputAggregationSize == that.outputAggregationSize &&
                    requestHeaderSize == that.requestHeaderSize &&
                    responseHeaderSize == that.responseHeaderSize &&
                    headerCacheSize == that.headerCacheSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(outputBufferSize, outputAggregationSize, requestHeaderSize, responseHeaderSize, headerCacheSize);
        }
    }
}
