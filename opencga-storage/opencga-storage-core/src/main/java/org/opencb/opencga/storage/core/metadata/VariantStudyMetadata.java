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

package org.opencb.opencga.storage.core.metadata;

import htsjdk.variant.vcf.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.opencb.biodata.models.variant.VariantSource;

import java.util.*;

/**
 * Created on 20/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStudyMetadata {

    private Map<String, VariantMetadataRecord> info;    // Map from ID to VariantMetadataRecord
    private Map<String, VariantMetadataRecord> format;  // Map from ID to VariantMetadataRecord

    private Map<String, String> alternates; // Symbolic alternates
    private Map<String, String> filter;
    private LinkedHashMap<String, Long> contig;

    // Do not store other metadata fields as they may be totally different
    // private Map<String, String> other;

    public VariantStudyMetadata() {
        info = new HashMap<>();
        format = new HashMap<>();
        alternates = new HashMap<>();
        filter = new HashMap<>();
        contig = new LinkedHashMap<>();
    }

    public VariantStudyMetadata(VariantStudyMetadata copy) {
        info = new HashMap<>(copy == null ? Collections.emptyMap() : copy.info); // VariantMetadataRecord are immutable objects
        format = new HashMap<>(copy == null ? Collections.emptyMap() : copy.format); // VariantMetadataRecord are immutable objects
        alternates = new HashMap<>(copy == null ? Collections.emptyMap() : copy.alternates);
        filter = new HashMap<>(copy == null ? Collections.emptyMap() : copy.filter);
        contig = new LinkedHashMap<>(copy == null ? Collections.emptyMap() : copy.contig);
    }

    public Map<String, VariantMetadataRecord> getInfo() {
        return info;
    }

    public VariantStudyMetadata setInfo(Map<String, VariantMetadataRecord> info) {
        this.info = info;
        return this;
    }

    public VariantStudyMetadata addInfoRecords(Collection<VCFInfoHeaderLine> collection) {
        for (VCFInfoHeaderLine line : collection) {
            info.put(line.getID(), new VariantMetadataRecord(line));
        }
        return this;
    }

    public VariantStudyMetadata addInfoRecord(Map<String, Object> record) {
        VariantMetadataRecord r = new VariantMetadataRecord(record);
        info.put(r.getId(), r);
        return this;
    }

    public Map<String, VariantMetadataRecord> getFormat() {
        return format;
    }

    public VariantStudyMetadata setFormat(Map<String, VariantMetadataRecord> format) {
        this.format = format;
        return this;
    }

    public VariantStudyMetadata addFormatRecords(Collection<VCFFormatHeaderLine> collection) {
        for (VCFFormatHeaderLine line : collection) {
            format.put(line.getKey(), new VariantMetadataRecord(line));
        }
        return this;
    }

    public VariantStudyMetadata addFormatRecord(Map<String, Object> record) {
        VariantMetadataRecord r = new VariantMetadataRecord(record);
        format.put(r.getId(), r);
        return this;
    }

    public Map<String, String> getAlternates() {
        return alternates;
    }

    public VariantStudyMetadata setAlternates(Map<String, String> alternates) {
        this.alternates = alternates;
        return this;
    }

    public Map<String, String> getFilter() {
        return filter;
    }

    public VariantStudyMetadata setFilter(Map<String, String> filter) {
        this.filter = filter;
        return this;
    }

    public LinkedHashMap<String, Long> getContig() {
        return contig;
    }

    public VariantStudyMetadata setContig(LinkedHashMap<String, Long> contig) {
        this.contig = contig;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VariantStudyMetadata)) {
            return false;
        }
        VariantStudyMetadata that = (VariantStudyMetadata) o;
        return Objects.equals(info, that.info)
                && Objects.equals(format, that.format)
                && Objects.equals(alternates, that.alternates)
                && Objects.equals(filter, that.filter)
                && Objects.equals(contig, that.contig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(info, format, alternates, filter, contig);
    }

    @Override
    public String toString() {
        return toString(ToStringStyle.SIMPLE_STYLE);
    }

    public String toJson() {
        return toString(ToStringStyle.JSON_STYLE);
    }

    public String toString(ToStringStyle style) {
        return new ToStringBuilder(this, style)
                .append("info", info)
                .append("format", format)
                .append("alternates", alternates)
                .append("filter", filter)
                .append("contigs", contig)
                .toString();
    }

    public VariantStudyMetadata addVariantSource(VariantSource source) {
        return addVariantSource(source, null);
    }

    public VariantStudyMetadata addVariantSource(VariantSource source, Collection<String> formats) {

        List<Map<String, Object>> format = (List) source.getHeader().getMeta().getOrDefault("FORMAT", Collections.emptyList());
        for (Map<String, Object> line : format) {
            if (formats == null || formats.contains(line.get("ID").toString())) {
                addFormatRecord(line);
            }
        }

        List<Map<String, Object>> info = (List) source.getHeader().getMeta().getOrDefault("INFO", Collections.emptyList());
        for (Map<String, Object> line : info) {
            addInfoRecord(line);
        }

        List<Map<String, Object>> filter = (List) source.getHeader().getMeta().getOrDefault("FILTER", Collections.emptyList());
        for (Map<String, Object> line : filter) {
            getFilter().put(line.get("ID").toString(), line.get("Description").toString());
        }

        List<Map<String, Object>> alt = (List) source.getHeader().getMeta().getOrDefault("ALT", Collections.emptyList());
        for (Map<String, Object> line : alt) {
            getAlternates().put(line.get("ID").toString(), line.get("Description").toString());
        }

        List<Map<String, Object>> config = (List) source.getHeader().getMeta().getOrDefault("contig", Collections.emptyList());
        for (Map<String, Object> line : config) {
            getContig().put(line.get("ID").toString(), Long.valueOf(line.get("length").toString()));
        }

        return this;
    }


    public static class VariantMetadataRecord {
        private String id;
        private VCFHeaderLineCount numberType;
        private Integer number;
        private VCFHeaderLineType type;
        private String description;
//        private Map<String, String> other;

        protected VariantMetadataRecord() {
        }

        public VariantMetadataRecord(String id, VCFHeaderLineCount numberType, Integer number, VCFHeaderLineType type, String description) {
            this.id = id;
            this.numberType = numberType;
            this.number = number;
            this.type = type;
            this.description = description;
        }

        public VariantMetadataRecord(VCFCompoundHeaderLine line) {
            id = line.getID();
            numberType = line.getCountType();
            number = line.getCount();
            type = line.getType();
            description = line.getDescription();
        }

        public VariantMetadataRecord(Map<String, Object> record) {
            id = record.get("ID").toString();
            String numberStr = record.get("Number").toString();
            if (numberStr.equals(".")) {
                numberType = VCFHeaderLineCount.UNBOUNDED;
                number = null;
            } else if (StringUtils.isNumeric(numberStr)) {
                numberType = VCFHeaderLineCount.INTEGER;
                number = Integer.valueOf(numberStr);
            } else {
                numberType = VCFHeaderLineCount.valueOf(numberStr);
                number = null;
            }
            type = VCFHeaderLineType.valueOf(record.get("Type").toString());
            description = record.get("Description").toString();
        }

        public String getId() {
            return id;
        }

        protected VariantMetadataRecord setId(String id) {
            this.id = id;
            return this;
        }

        public VCFHeaderLineCount getNumberType() {
            return numberType;
        }

        protected VariantMetadataRecord setNumberType(VCFHeaderLineCount numberType) {
            this.numberType = numberType;
            return this;
        }

        public Integer getNumber() {
            return number;
        }

        protected VariantMetadataRecord setNumber(Integer number) {
            this.number = number;
            return this;
        }

        public VCFHeaderLineType getType() {
            return type;
        }

        protected VariantMetadataRecord setType(VCFHeaderLineType type) {
            this.type = type;
            return this;
        }

        public String getDescription() {
            return description;
        }

        protected VariantMetadataRecord setDescription(String description) {
            this.description = description;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VariantMetadataRecord)) {
                return false;
            }
            VariantMetadataRecord that = (VariantMetadataRecord) o;
            return Objects.equals(id, that.id)
                    && Objects.equals(number, that.number)
                    && numberType == that.numberType
                    && type == that.type
                    && Objects.equals(description, that.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, number, numberType, type, description);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                    .append("id", id)
                    .append("numberType", numberType)
                    .append("number", number)
                    .append("type", type)
                    .append("description", description)
                    .toString();
        }

    }
}
