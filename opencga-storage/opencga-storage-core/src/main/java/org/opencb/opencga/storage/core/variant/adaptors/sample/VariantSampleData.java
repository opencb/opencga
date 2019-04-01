package org.opencb.opencga.storage.core.variant.adaptors.sample;


import org.opencb.biodata.models.variant.avro.FileEntry;

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

    public VariantSampleData() {
    }

    public VariantSampleData(String id, String studyId, Map<String, List<SampleData>> samples, Map<String, FileEntry> files) {
        this.id = id;
        this.studyId = studyId;
        this.samples = samples;
        this.files = files;
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
}
