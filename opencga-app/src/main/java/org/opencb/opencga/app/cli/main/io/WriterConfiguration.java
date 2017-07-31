/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.app.cli.main.io;

/**
 * Created by pfurio on 28/11/16.
 */
public class WriterConfiguration {

    private boolean metadata;
    private boolean header;
    private boolean pretty;
    private boolean count;
    @Deprecated
    private boolean tree;

    public WriterConfiguration() {
    }

    public boolean isPretty() {
        return pretty;
    }

    public WriterConfiguration setPretty(boolean pretty) {
        this.pretty = pretty;
        return this;
    }

    public boolean isCount() {
        return count;
    }

    public WriterConfiguration setCount(boolean count) {
        this.count = count;
        return this;
    }

    public boolean isTree() {
        return tree;
    }

    public WriterConfiguration setTree(boolean tree) {
        this.tree = tree;
        return this;
    }

    public boolean isMetadata() {
        return metadata;
    }

    public WriterConfiguration setMetadata(boolean metadata) {
        this.metadata = metadata;
        return this;
    }

    public boolean isHeader() {
        return header;
    }

    public WriterConfiguration setHeader(boolean header) {
        this.header = header;
        return this;
    }
}
