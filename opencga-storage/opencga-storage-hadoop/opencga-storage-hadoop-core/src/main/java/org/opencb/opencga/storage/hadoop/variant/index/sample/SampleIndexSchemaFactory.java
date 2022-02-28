package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SampleIndexSchemaFactory {
    private final VariantStorageMetadataManager metadataManager;

    public SampleIndexSchemaFactory(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public SampleIndexSchema getSchema(int studyId, int sampleId) {
        return getSchema(studyId, sampleId, false);
    }

    public SampleIndexSchema getSchema(int studyId, int sampleId, boolean acceptPartialAnnotationIndex) {
        Collection<Integer> versions = getSampleIndexConfigurationVersions(studyId, sampleId, true);
        if (versions.isEmpty() && acceptPartialAnnotationIndex) {
            versions = getSampleIndexConfigurationVersions(studyId, sampleId, false);
        }
        if (versions.isEmpty()) {
            throw sampleIndexNotFound(Collections.singletonList(metadataManager.getSampleName(studyId, sampleId)));
        }
        int version = versions.stream().mapToInt(i -> i).max().getAsInt();
        SampleIndexConfiguration sampleIndexConfiguration = getSampleIndexConfiguration(studyId, version);

        if (sampleIndexConfiguration == null) {
            throw new VariantQueryException("Unable to use sample index version " + version + " required to query sample "
                    + metadataManager.getSampleName(studyId, sampleId));
        }
        return new SampleIndexSchema(sampleIndexConfiguration, version);
    }

    public SampleIndexSchema getSchema(int studyId, Collection<String> samples) {
        return getSchema(studyId, samples, false);
    }

    public SampleIndexSchema getSchema(int studyId, Collection<String> samples, boolean acceptPartialAnnotationIndex) {
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("Missing samples");
        }
        int version = getSampleIndexConfigurationVersion(studyId, samples, acceptPartialAnnotationIndex);
        SampleIndexConfiguration sampleIndexConfiguration = getSampleIndexConfiguration(studyId, version);

        if (sampleIndexConfiguration == null) {
            throw new VariantQueryException("Unable to use sample index version " + version + " required to query samples " + samples);
        }
        return new SampleIndexSchema(sampleIndexConfiguration, version);
    }

    public Collection<Integer> getSampleIndexConfigurationVersions(int studyId, Collection<?> samples) {
        return getSampleIndexConfigurationVersions(studyId, samples, true);
    }

    public Collection<Integer> getSampleIndexConfigurationVersions(int studyId, Collection<?> samples, boolean withAnnotation) {
        List<Collection<Integer>> allVersions = new ArrayList<>(samples.size());
        for (Object sample : samples) {
            allVersions.add(getSampleIndexConfigurationVersions(studyId, sample, withAnnotation));
        }
        Collection<Integer> intersection = allVersions.get(0);
        for (int i = 1; i < allVersions.size(); i++) {
            intersection = CollectionUtils.intersection(intersection, allVersions.get(i));
        }
        return intersection;
    }

    private Collection<Integer> getSampleIndexConfigurationVersions(int studyId, Object sample, boolean withAnnotation) {
        int sampleId = metadataManager.getSampleIdOrFail(studyId, sample);
        SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
        if (withAnnotation) {
            return CollectionUtils.intersection(
                    sampleMetadata.getSampleIndexVersions(),
                    sampleMetadata.getSampleIndexAnnotationVersions());
        } else {
            return sampleMetadata.getSampleIndexVersions();
        }
    }

    public int getSampleIndexConfigurationVersion(int studyId, Collection<?> samples) {
        return getSampleIndexConfigurationVersion(studyId, samples, false);
    }

    public int getSampleIndexConfigurationVersion(int studyId, Collection<?> samples, boolean acceptPartialAnnotationIndex) {
        Collection<Integer> validVersions = getSampleIndexConfigurationVersions(studyId, samples, true);
        if (validVersions.isEmpty() && acceptPartialAnnotationIndex) {
            validVersions = getSampleIndexConfigurationVersions(studyId, samples, false);
        }

        if (validVersions.isEmpty()) {
            throw sampleIndexNotFound(samples);
        }
        return validVersions.stream().mapToInt(i -> i).max().getAsInt();
    }

    private VariantQueryException sampleIndexNotFound(Collection<?> samples) {
        if (samples.size() == 1) {
            throw new VariantQueryException("Not found valid sample index for sample " + samples.iterator().next());
        } else {
            throw new VariantQueryException("Not found valid sample index for samples " + samples);
        }
    }

    public SampleIndexSchema getSchemaLatest(int studyId) {
        StudyMetadata.SampleIndexConfigurationVersioned latest = getSampleIndexConfigurationLatest(studyId);
        return new SampleIndexSchema(latest.getConfiguration(), latest.getVersion());
    }

    public StudyMetadata.SampleIndexConfigurationVersioned getSampleIndexConfigurationLatest(int studyId) {
        return metadataManager.getStudyMetadata(studyId).getSampleIndexConfigurationLatest();
    }

    public SampleIndexConfiguration getSampleIndexConfiguration(int studyId, int version) {
        return metadataManager.getStudyMetadata(studyId).getSampleIndexConfiguration(version);
    }


}
