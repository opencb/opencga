package org.opencb.opencga.storage.core.variant.adaptors.sample;


import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 27/03/19.
 */
public class VariantSampleData {
    private String id;
    private String studyId;
    private Map<String, List<SampleData>> samples;
    private Map<String, FileEntry> files;
    private Map<String, VariantStats> stats;

    public VariantSampleData() {
    }

    public VariantSampleData(String id, String studyId,
                             Map<String, List<SampleData>> samples,
                             Map<String, FileEntry> files,
                             Map<String, VariantStats> stats) {
        this.id = id;
        this.studyId = studyId;
        this.samples = samples;
        this.files = files;
        this.stats = stats;
    }

    public String getId() {
        return id;
    }

    public VariantSampleData setId(String id) {
        this.id = id;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public VariantSampleData setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public Map<String, List<SampleData>> getSamples() {
        return samples;
    }

    public VariantSampleData setSamples(Map<String, List<SampleData>> samples) {
        this.samples = samples;
        return this;
    }

    public Map<String, FileEntry> getFiles() {
        return files;
    }

    public VariantSampleData setFiles(Map<String, FileEntry> files) {
        this.files = files;
        return this;
    }

    public Map<String, VariantStats> getStats() {
        return stats;
    }

    public VariantSampleData setStats(Map<String, VariantStats> stats) {
        this.stats = stats;
        return this;
    }

    @Override
    public String toString() {
        return "VariantSampleData{"
                + "id='" + id + '\''
                + ", studyId='" + studyId + '\''
                + ", samples=" + samples
                + ", files=" + files
                + ", stats=" + stats
                + '}';
    }
}
