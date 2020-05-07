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

package org.opencb.opencga.core.models.file;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class FileRelatedFile {

    private File file;
    private Relation relation;

    public enum Relation {
        PRODUCED_FROM,
        PART_OF_PAIR,
        PEDIGREE,
        REFERENCE_GENOME
    }

    public FileRelatedFile() {
    }

    public FileRelatedFile(File file, Relation relation) {
        this.file = file;
        this.relation = relation;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RelatedFile{");
        sb.append("file=").append(file);
        sb.append(", relation=").append(relation);
        sb.append('}');
        return sb.toString();
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public Relation getRelation() {
        return relation;
    }

    public FileRelatedFile setRelation(Relation relation) {
        this.relation = relation;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        FileRelatedFile that = (FileRelatedFile) o;

        if (file == null || that.file == null) {
            return false;
        }

        return new EqualsBuilder()
                .append(file.getId(), that.file.getId())
                .append(relation, that.relation)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(file.getId())
                .append(relation)
                .toHashCode();
    }
}
