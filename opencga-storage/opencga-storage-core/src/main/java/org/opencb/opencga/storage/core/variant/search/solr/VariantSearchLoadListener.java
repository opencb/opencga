package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;

/**
 * Use this class to keep a track of the indexed variants status in the VariantSearchEngine.
 *
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantSearchLoadListener {

    protected final Map<String, Integer> studiesMap;
    protected final boolean overwrite;

    protected VariantSearchLoadListener(Map<String, Integer> studiesMap, boolean overwrite) {
        this.studiesMap = studiesMap;
        this.overwrite = overwrite;
    }

    public void preLoad(List<Variant> variants) throws IOException {
        if (overwrite) {
            // Do nothing. All variants should be synchronized
            return;
        }
        List<Variant> alreadySynchronizedVariants = new ArrayList<>();
        Iterator<Variant> iterator = variants.iterator();
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            if (variant.getAnnotation() != null
                    && variant.getAnnotation().getAdditionalAttributes() != null
                    && variant.getAnnotation().getAdditionalAttributes().get(GROUP_NAME.key()) != null) {
                AdditionalAttribute additionalAttribute = variant.getAnnotation().getAdditionalAttributes().get(GROUP_NAME.key());
                String sync = additionalAttribute.getAttribute().get(AdditionalAttributes.INDEX_SYNCHRONIZATION.key());
                if (VariantStorageEngine.SyncStatus.SYNCHRONIZED.key().equals(sync)) {
                    // Discard variant!
                    iterator.remove();
                } else if (VariantStorageEngine.SyncStatus.UNKNOWN.key().equals(sync)) {
                    String indexedStudiesStr = additionalAttribute.getAttribute().get(AdditionalAttributes.INDEX_STUDIES.key());
                    if (StringUtils.isNotEmpty(indexedStudiesStr)) {
                        String[] indexedStudies = indexedStudiesStr.split(",");
                        if (indexedStudies.length == variant.getStudies().size()) {
                            Set<Integer> studies = variant.getStudies()
                                    .stream()
                                    .map(StudyEntry::getStudyId).map(studiesMap::get)
                                    .collect(Collectors.toSet());
                            boolean allStudiesIndexed = true;
                            for (String indexedStudy : indexedStudies) {
                                allStudiesIndexed &= studies.contains(Integer.valueOf(indexedStudy));
                            }
                            if (allStudiesIndexed) {
                                // Discard variant!
                                alreadySynchronizedVariants.add(variant);
                                iterator.remove();
                            }
                        }
                    }
                }
            }
        }
        if (!alreadySynchronizedVariants.isEmpty()) {
            processAlreadySynchronizedVariants(alreadySynchronizedVariants);
        }
    }

    protected abstract void processAlreadySynchronizedVariants(List<Variant> alreadySynchronizedVariants);

    public abstract void postLoad(List<Variant> variantList) throws IOException;

    public static VariantSearchLoadListener empty(boolean overwrite) {
        return new VariantSearchLoadListener(null, overwrite) {
            @Override
            protected void processAlreadySynchronizedVariants(List<Variant> alreadySynchronizedVariants) {
            }

            @Override
            public void postLoad(List<Variant> variantList) throws IOException {
            }
        };
    }

    public void close() {}
}
