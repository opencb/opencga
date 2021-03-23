package org.opencb.opencga.storage.core.variant.annotation;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.VARIANT_ID;

public class DummyTestAnnotator extends VariantAnnotator {

    public static final String ANNOT_KEY = "ANNOT_KEY";
    public static final String FAIL = "ANNOT_FAIL";
    private final boolean fail;
    private String key;

    public DummyTestAnnotator(StorageConfiguration configuration, ProjectMetadata projectMetadata, ObjectMap options) throws VariantAnnotatorException {
        super(configuration, projectMetadata, options);
        key = options.getString(ANNOT_KEY);
        fail = options.getBoolean(FAIL, false);
    }

    @Override
    public List<VariantAnnotation> annotate(List<Variant> variants) throws VariantAnnotatorException {
        if (fail) {
            throw new VariantAnnotatorException("Fail because reasons");
        }
        return variants.stream().map(v -> {
            VariantAnnotation a = new VariantAnnotation();
            a.setChromosome(v.getChromosome());
            a.setStart(v.getStart());
            a.setEnd(v.getEnd());
            a.setReference(v.getReference());
            a.setAlternate(v.getAlternate());
            a.setId("an id -- " + key);
            ConsequenceType ct = new ConsequenceType();
            ct.setGeneName("a gene");
            ct.setSequenceOntologyTerms(Collections.emptyList());
            ct.setExonOverlap(Collections.emptyList());
            ct.setTranscriptAnnotationFlags(Collections.emptyList());
            a.setConsequenceTypes(Collections.singletonList(ct));
            a.setAdditionalAttributes(
                    Collections.singletonMap(GROUP_NAME.key(),
                            new AdditionalAttribute(Collections.singletonMap(VARIANT_ID.key(), v.toString()))));
            return a;
        }).collect(Collectors.toList());
    }

    @Override
    public ProjectMetadata.VariantAnnotatorProgram getVariantAnnotatorProgram() throws IOException {
        return new ProjectMetadata.VariantAnnotatorProgram("MyAnnotator", key, null);
    }

    @Override
    public List<ObjectMap> getVariantAnnotatorSourceVersion() throws IOException {
        return Collections.singletonList(new ObjectMap("data", "genes"));
    }
}
