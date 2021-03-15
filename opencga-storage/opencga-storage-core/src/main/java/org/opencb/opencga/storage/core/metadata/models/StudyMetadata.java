package org.opencb.opencga.storage.core.metadata.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderSimpleLine;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 10/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyMetadata {

    public static final String UNKNOWN_HEADER_ATTRIBUTE = ".";
    private int id;
    private String name;
    private Aggregation aggregation;
    private Long timeStamp;
    private VariantFileHeader variantHeader;
    private List<VariantScoreMetadata> variantScores;
    private List<SampleIndexConfigurationVersioned> sampleIndexConfigurations;

    private ObjectMap attributes;

    private static Logger logger = LoggerFactory.getLogger(StudyMetadata.class);

    public StudyMetadata() {
        this(-1, null);
    }

    public StudyMetadata(int id, String name) {
        this.id = id;
        this.name = name;
        this.variantHeader = VariantFileHeader.newBuilder().setVersion("").build();
        variantScores = new ArrayList<>();
        attributes = new ObjectMap();
    }

    @Deprecated
    public StudyMetadata(StudyConfiguration sc) {
        id = sc.getId();
        name = sc.getName();
        aggregation = sc.getAggregation();
        variantHeader = sc.getVariantHeader();
        timeStamp = sc.getTimeStamp();
        attributes = sc.getAttributes();
    }

    public int getId() {
        return id;
    }

    public StudyMetadata setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public StudyMetadata setName(String name) {
        this.name = name;
        return this;
    }

    @Deprecated
    public int getStudyId() {
        return getId();
    }

    @Deprecated
    public String getStudyName() {
        return getName();
    }

    public Aggregation getAggregation() {
        return aggregation;
    }

    public void setAggregation(Aggregation aggregation) {
        this.aggregation = aggregation;
    }

    public void setAggregationStr(String aggregation) {
        this.aggregation = AggregationUtils.valueOf(aggregation);
    }

    @JsonIgnore
    public boolean isAggregated() {
        return AggregationUtils.isAggregated(getAggregation());
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public StudyMetadata setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
        return this;
    }

    public VariantFileHeader getVariantHeader() {
        return variantHeader;
    }

    public StudyMetadata setVariantHeader(VariantFileHeader variantHeader) {
        this.variantHeader = variantHeader;
        return this;
    }

    public List<VariantScoreMetadata> getVariantScores() {
        return variantScores;
    }

    public StudyMetadata setVariantScores(List<VariantScoreMetadata> variantScores) {
        if (variantScores == null) {
            this.variantScores = new ArrayList<>();
        } else {
            this.variantScores = variantScores;
        }
        return this;
    }

    public SampleIndexConfigurationVersioned getSampleIndexConfigurationLatest() {
        if (sampleIndexConfigurations == null || sampleIndexConfigurations.isEmpty()) {
            return new SampleIndexConfigurationVersioned(SampleIndexConfiguration.defaultConfiguration(), 1, Date.from(Instant.now()));
        } else {
            SampleIndexConfigurationVersioned conf = sampleIndexConfigurations.get(0);
            for (SampleIndexConfigurationVersioned thisConf : sampleIndexConfigurations) {
                if (thisConf.getVersion() > conf.getVersion()) {
                    conf = thisConf;
                }
            }
            return conf;
        }
    }

    public List<SampleIndexConfigurationVersioned> getSampleIndexConfigurations() {
        return sampleIndexConfigurations;
    }

    public StudyMetadata setSampleIndexConfigurations(List<SampleIndexConfigurationVersioned> sampleIndexConfigurations) {
        this.sampleIndexConfigurations = sampleIndexConfigurations;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public StudyMetadata setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }

    public Map<String, VariantFileHeaderComplexLine> getVariantHeaderLines(String key) {
        return variantHeader.getComplexLines()
                .stream()
                .filter(l -> l.getKey().equalsIgnoreCase(key))
                .collect(Collectors.toMap(VariantFileHeaderComplexLine::getId, l -> l));
    }

    public VariantFileHeaderComplexLine getVariantHeaderLine(String key, String id) {
        return variantHeader.getComplexLines()
                .stream()
                .filter(l -> l.getKey().equalsIgnoreCase(key) && l.getId().equalsIgnoreCase(id))
                .findFirst().orElse(null);
    }

    public StudyMetadata addVariantFileHeader(VariantFileHeader header, List<String> formats) {
        Map<String, Map<String, VariantFileHeaderComplexLine>> map = new HashMap<>();
        for (VariantFileHeaderComplexLine line : this.variantHeader.getComplexLines()) {
            Map<String, VariantFileHeaderComplexLine> keyMap = map.computeIfAbsent(line.getKey(), key -> new HashMap<>());
            keyMap.put(line.getId(), line);
        }
        for (VariantFileHeaderComplexLine line : header.getComplexLines()) {
            if (formats == null || !line.getKey().equalsIgnoreCase("format") || formats.contains(line.getId())) {
                Map<String, VariantFileHeaderComplexLine> keyMap = map.computeIfAbsent(line.getKey(), key -> new HashMap<>());
                if (keyMap.containsKey(line.getId())) {
                    VariantFileHeaderComplexLine prevLine = keyMap.get(line.getId());
                    if (!prevLine.equals(line)) {
                        logger.warn("Previous header line does not match with new header. previous: " + prevLine + " , new: " + line);
//                        throw new IllegalArgumentException();
                    }
                } else {
                    keyMap.put(line.getId(), line);
                    variantHeader.getComplexLines().add(line);
                }
            }
        }
        Map<String, String> simpleLines = this.variantHeader.getSimpleLines()
                .stream()
                .collect(Collectors.toMap(VariantFileHeaderSimpleLine::getKey, VariantFileHeaderSimpleLine::getValue));
        header.getSimpleLines().forEach((line) -> {
            String oldValue = simpleLines.put(line.getKey(), line.getValue());
            if (oldValue != null && !oldValue.equals(line.getValue())) {
                // If the value changes among files, replace it with a dot, as it is an unknown value.
                simpleLines.put(line.getKey(), UNKNOWN_HEADER_ATTRIBUTE);
//                throw new IllegalArgumentException();
            }
        });
        this.variantHeader.setSimpleLines(simpleLines.entrySet()
                .stream()
                .map(e -> new VariantFileHeaderSimpleLine(e.getKey(), e.getValue()))
                .collect(Collectors.toList()));

        return this;
    }

    public static class SampleIndexConfigurationVersioned {
        private SampleIndexConfiguration configuration;
        private int version;
        private Date date;
//        private int numSamples;


        public SampleIndexConfigurationVersioned() {
        }

        public SampleIndexConfigurationVersioned(SampleIndexConfiguration configuration, int version, Date date) {
            this.configuration = configuration;
            this.version = version;
            this.date = date;
        }

        public SampleIndexConfiguration getConfiguration() {
            return configuration;
        }

        public SampleIndexConfigurationVersioned setConfiguration(SampleIndexConfiguration configuration) {
            this.configuration = configuration;
            return this;
        }

        public int getVersion() {
            return version;
        }

        public SampleIndexConfigurationVersioned setVersion(int version) {
            this.version = version;
            return this;
        }

        public Date getDate() {
            return date;
        }

        public SampleIndexConfigurationVersioned setDate(Date date) {
            this.date = date;
            return this;
        }

//        public int getNumSamples() {
//            return numSamples;
//        }
//
//        public SampleIndexConfigurationVersioned setNumSamples(int numSamples) {
//            this.numSamples = numSamples;
//            return this;
//        }
    }
}
