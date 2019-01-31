package org.opencb.opencga.storage.core.metadata.adaptors;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;

import java.util.*;

/**
 * Created by jacobo on 20/01/19.
 */
public interface SampleMetadataDBAdaptor {


    SampleMetadata getSampleMetadata(int studyId, int sampleId, Long timeStamp);

    void updateSampleMetadata(int studyId, SampleMetadata sample, Long timeStamp);

    Iterator<SampleMetadata> sampleMetadataIterator(int studyId);

    default BiMap<String, Integer> getIndexedSamplesMap(int studyId) {
        // FIXME!
        BiMap<String, Integer> map = HashBiMap.create();
        sampleMetadataIterator(studyId).forEachRemaining(s -> {
            if (s.isIndexed()) {
                map.put(s.getName(), s.getId());
            }
        });
        return map;
    }

    default List<Integer> getIndexedSamples(int studyId) {
        // FIXME!
        List<Integer> samples = new LinkedList<>();
        sampleMetadataIterator(studyId).forEachRemaining(s -> {
            if (s.isIndexed()) {
                samples.add(s.getId());
            }
        });
        return samples;
    }

    Integer getSampleId(int studyId, String sampleName);

}
