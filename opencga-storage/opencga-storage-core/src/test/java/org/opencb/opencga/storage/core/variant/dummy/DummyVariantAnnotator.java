package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.cellbase.core.models.DataRelease;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.VARIANT_ID;

public class DummyVariantAnnotator extends VariantAnnotator {

    private Logger logger = LoggerFactory.getLogger(DummyVariantAnnotator.class);
    public static final String ANNOT_KEY = "ANNOT_KEY";
    public static final String ANNOT_VERSION = "ANNOT_VERSION";
    public static final String ANNOT_DATARELEASE = "ANNOT_DATARELEASE";
    public static final String ANNOT_METADATA = "ANNOT_METADATA";
    public static final String FAIL = "ANNOT_FAIL";
    public static final String FAIL_AT = "ANNOT_FAIL_AT";
    public static final String SKIP = "ANNOT_SKIP";
    private final boolean fail;
    private final Set<String> failAt;
    private final Set<String> skip;
    private String key;
    private final ProjectMetadata.VariantAnnotationMetadata metadata;

    public DummyVariantAnnotator(StorageConfiguration configuration, ProjectMetadata projectMetadata, ObjectMap options) throws VariantAnnotatorException {
        super(configuration, projectMetadata, options);
        key = options.getString(ANNOT_KEY, "k1");
        String version = options.getString(ANNOT_VERSION, "v1");
        int dataReleaseId = options.getInt(ANNOT_DATARELEASE, 1);
        fail = options.getBoolean(FAIL, false);
        failAt = new HashSet<>(options.getAsStringList(FAIL_AT));
        skip = new HashSet<>(options.getAsStringList(SKIP));
        if (options.containsKey(ANNOT_METADATA)) {
            metadata = options.get(ANNOT_METADATA, ProjectMetadata.VariantAnnotationMetadata.class);
        } else {
            DataRelease dataRelease = new DataRelease();
            dataRelease.setRelease(dataReleaseId);
            metadata = new ProjectMetadata.VariantAnnotationMetadata(-1, null, null,
                    new ProjectMetadata.VariantAnnotatorProgram(key, version, null), new HashMap<>(),
                    Collections.singletonList(new ObjectMap("data", "genes")), dataRelease, null);
        }
    }

    public static String getRs(Variant variant) {
        return "rs" + variant.toString().hashCode();
    }

    private static ArrayList<String> SOURCES = new ArrayList<>(Arrays.asList("cosmic", "hgmd", "clinvar", "civic"));

    @Override
    public List<VariantAnnotation> annotate(List<Variant> variants) throws VariantAnnotatorException {
        if (fail) {
            throw new VariantAnnotatorException("Fail because reasons");
        }
        if (!failAt.isEmpty()) {
            for (Variant variant : variants) {
                if (failAt.contains(variant.toString())) {
                    throw new VariantAnnotatorException("Fail at variant " + variant + " because reasons");
                }
            }
        }
        return variants.stream().map(v -> {
            if (skip.contains(v.toString())) {
                logger.info("Skipping variant {}", v);
                return null;
            }
            VariantAnnotation a = new VariantAnnotation();
            a.setChromosome(v.getChromosome());
            a.setStart(v.getStart());
            a.setEnd(v.getEnd());
            a.setReference(v.getReference());
            a.setAlternate(v.getAlternate());
            a.setId("an id -- " + key);
            ConsequenceType ct = new ConsequenceType();
            ct.setGeneName("a gene from " + key);
            ct.setSequenceOntologyTerms(Collections.emptyList());
            ct.setExonOverlap(Collections.emptyList());
            ct.setTranscriptFlags(Collections.emptyList());
            a.setConsequenceTypes(Collections.singletonList(ct));
            a.setXrefs(Collections.singletonList(new Xref(getRs(v), "dbSNP")));
            EvidenceEntry evidenceEntry = new EvidenceEntry();
            int i = Math.abs(v.toString().hashCode() % SOURCES.size());
            evidenceEntry.setSource(new EvidenceSource(SOURCES.get(i), "v1", ""));
            a.setTraitAssociation(Collections.singletonList(evidenceEntry));
            a.setAdditionalAttributes(
                    Collections.singletonMap(GROUP_NAME.key(),
                            new AdditionalAttribute(Collections.singletonMap(VARIANT_ID.key(), v.toString()))));
            return a;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public ProjectMetadata.VariantAnnotationMetadata getVariantAnnotationMetadata() {
        return new ProjectMetadata.VariantAnnotationMetadata(metadata);
    }

}
