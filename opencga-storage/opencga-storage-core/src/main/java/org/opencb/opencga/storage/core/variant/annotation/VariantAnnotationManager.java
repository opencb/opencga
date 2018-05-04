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

package org.opencb.opencga.storage.core.variant.annotation;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantAnnotationManager {

    public static final String SPECIES = "species";
    public static final String ASSEMBLY = "assembly";
    public static final String ANNOTATOR = "annotator";
    @Deprecated
    public static final String ANNOTATION_SOURCE = "annotationSource";
    public static final String OVERWRITE_ANNOTATIONS = "overwriteAnnotations";
    public static final String VARIANT_ANNOTATOR_CLASSNAME = "variant.annotator.classname";
    // File to load.
    public static final String CREATE = "annotation.create";
    public static final String LOAD_FILE = "annotation.load.file";
    public static final String CUSTOM_ANNOTATION_KEY = "custom_annotation_key";
    public static final String LATEST = "LATEST";

    private static Logger logger = LoggerFactory.getLogger(VariantAnnotationManager.class);

    public abstract void annotate(Query query, ObjectMap options) throws VariantAnnotatorException, IOException, StorageEngineException;

    public abstract void createAnnotationSnapshot(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException;

    public abstract void deleteAnnotationSnapshot(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException;

    protected final ProjectMetadata.VariantAnnotationMetadata checkCurrentAnnotation(VariantAnnotator annotator,
                                                                                     ProjectMetadata projectMetadata)
            throws IOException {
        ProjectMetadata.VariantAnnotatorProgram newAnnotator = annotator.getVariantAnnotatorProgram();
        List<ObjectMap> newSourceVersion = annotator.getVariantAnnotatorSourceVersion();
        if (newSourceVersion == null) {
            newSourceVersion = Collections.emptyList();
        }

        ProjectMetadata.VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
        if (current == null) {
            current = new ProjectMetadata.VariantAnnotationMetadata();
            projectMetadata.getAnnotation().setCurrent(current);
            current.setId(-1);
            current.setName(LATEST);
        }

        // Check using same annotator and same source version
        ProjectMetadata.VariantAnnotatorProgram currentAnnotator = current.getAnnotator();
        if (currentAnnotator != null && !currentAnnotator.equals(newAnnotator)) {
            if (newAnnotator == null) {
                throw new IllegalArgumentException("Missing annotator information for VariantAnnotator: " + annotator.getClass());
            }
            if (!currentAnnotator.getName().equals(newAnnotator.getName())
                    || !currentAnnotator.getVersion().equals(newAnnotator.getVersion())) {
                String msg = "Using a different annotator! "
                        + "Existing annotation calculated with " + currentAnnotator.toString()
                        + ", attempting to annotate with " + newAnnotator.toString();
                logger.error(msg);
//                throw new VariantAnnotatorException(msg);
            } else if (!currentAnnotator.getCommit().equals(newAnnotator.getCommit())) {
                logger.warn("Using a different commit for annotating variants. "
                        + "Existing annotation calculated with " + currentAnnotator.toString()
                        + ", attempting to annotate with " + newAnnotator.toString());
            }
        }
        current.setAnnotator(newAnnotator);

        List<ObjectMap> currentSourceVersion = current.getSourceVersion();
        if (CollectionUtils.isNotEmpty(currentSourceVersion) && !currentSourceVersion.equals(newSourceVersion)) {
            if (newSourceVersion.isEmpty()) {
                throw new IllegalArgumentException("Missing annotator source version for VariantAnnotator: " + annotator.getClass());
            }
            String msg = "Source version of the annotator has changed. "
                    + "Existing annotation calculated with "
                    + currentSourceVersion.stream().map(ObjectMap::toJson).collect(Collectors.joining(" , ", "[ ", " ]"))
                    + ", attempting to annotate with "
                    + newSourceVersion.stream().map(ObjectMap::toJson).collect(Collectors.joining(" , ", "[ ", " ]"));
            logger.error(msg);
//            throw new VariantAnnotatorException(msg);
        }
        current.setSourceVersion(newSourceVersion);

        return current;
    }

    protected final ProjectMetadata.VariantAnnotationMetadata registerNewAnnotationSnapshot(String name, VariantAnnotator annotator,
                                                                                            ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {
        ProjectMetadata.VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
        if (current == null) {
            // Should never enter here
            try {
                current = checkCurrentAnnotation(annotator, projectMetadata);
            } catch (IOException e) {
                throw new VariantAnnotatorException("Missing current annotation metadata!", e);
            }
        }

        boolean nameDuplicated = projectMetadata.getAnnotation().getSaved()
                .stream()
                .map(ProjectMetadata.VariantAnnotationMetadata::getName)
                .anyMatch(s -> s.equalsIgnoreCase(name));

        if (nameDuplicated) {
            throw new VariantAnnotatorException("Annotation snapshot name already exists!");
        }
        Integer maxId = projectMetadata.getAnnotation().getSaved()
                .stream()
                .map(ProjectMetadata.VariantAnnotationMetadata::getId)
                .max(Integer::compareTo)
                .orElse(0);

        ProjectMetadata.VariantAnnotationMetadata newSnapshot = new ProjectMetadata.VariantAnnotationMetadata(
                maxId + 1,
                name,
                Date.from(Instant.now()),
                current.getAnnotator(),
                current.getSourceVersion());
        projectMetadata.getAnnotation().getSaved().add(newSnapshot);

        return newSnapshot;
    }

    protected final ProjectMetadata.VariantAnnotationMetadata removeAnnotationSnapshot(String name, ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {
        Iterator<ProjectMetadata.VariantAnnotationMetadata> iterator = projectMetadata.getAnnotation().getSaved().iterator();
        ProjectMetadata.VariantAnnotationMetadata annotation = null;
        boolean found = false;
        while (iterator.hasNext()) {
            annotation = iterator.next();
            if (annotation.getName().equals(name)) {
                found = true;
                iterator.remove();
                break;
            }
        }
        if (found) {
            return annotation;
        } else {
            throw new VariantAnnotatorException("Variant Annotation snapshot \"" + name + "\" not found!");
        }
    }
}
