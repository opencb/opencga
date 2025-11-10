package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.VariantAnnotatorExtensionTask;

import java.util.LinkedList;
import java.util.List;

public class VariantAnnotatorWithExtensions extends VariantAnnotator {
    private final VariantAnnotator annotator;
    private final List<? extends VariantAnnotatorExtensionTask> annotatorExtensions;

    public VariantAnnotatorWithExtensions(StorageConfiguration configuration, ProjectMetadata projectMetadata, ObjectMap options,
                                          VariantAnnotator annotator,
                                          List<? extends VariantAnnotatorExtensionTask> annotatorExtensions)
            throws VariantAnnotatorException {
        super(configuration, projectMetadata, options);
        this.annotator = annotator;
        this.annotatorExtensions = annotatorExtensions;
    }

    @Override
    public void pre() throws Exception {
        annotator.pre();
        for (VariantAnnotatorExtensionTask extension : annotatorExtensions) {
            extension.pre();
        }
    }

    @Override
    public List<VariantAnnotation> annotate(List<Variant> variants) throws Exception {
        List<VariantAnnotation> annotated = annotator.annotate(variants);
        for (VariantAnnotatorExtensionTask extension : annotatorExtensions) {
            annotated = extension.apply(annotated);
        }
        return annotated;
    }

    @Override
    public List<VariantAnnotation> drain() throws Exception {
        List<VariantAnnotation> annotated = new LinkedList<>(annotator.drain());
        for (VariantAnnotatorExtensionTask extension : annotatorExtensions) {
            annotated = extension.apply(annotated);
            annotated.addAll(extension.drain());
        }
        return annotated;
    }

    @Override
    public void post() throws Exception {
        annotator.post();
        for (VariantAnnotatorExtensionTask extension : annotatorExtensions) {
            extension.post();
        }
    }

    @Override
    public ProjectMetadata.VariantAnnotationMetadata getVariantAnnotationMetadata() throws VariantAnnotatorException {
        ProjectMetadata.VariantAnnotationMetadata metadata = annotator.getVariantAnnotationMetadata();
        for (VariantAnnotatorExtensionTask extension : annotatorExtensions) {
            metadata.addExtension(extension.getId(), extension.getMetadata());
        }
        return metadata;
    }
}
