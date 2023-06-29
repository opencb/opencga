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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata.VariantAnnotationMetadata;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata.VariantAnnotatorProgram;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantAnnotationManager {

    @Deprecated
    public static final String ANNOTATION_SOURCE = "annotationSource";
    // File to load.
    public static final String CREATE = "annotation.create";
    public static final String LOAD_FILE = "annotation.load.file";
    public static final String CUSTOM_ANNOTATION_KEY = "custom_annotation_key";
    public static final String CURRENT = "CURRENT";

    private static Logger logger = LoggerFactory.getLogger(VariantAnnotationManager.class);

    public abstract long annotate(Query query, ObjectMap options) throws VariantAnnotatorException, IOException, StorageEngineException;

    public abstract void saveAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException;

    public abstract void deleteAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException;

    protected final VariantAnnotationMetadata checkCurrentAnnotation(VariantAnnotator annotator, ProjectMetadata projectMetadata,
                                                                     boolean overwrite)
            throws VariantAnnotatorException {
//        VariantAnnotatorProgram newAnnotator = annotator.getVariantAnnotatorProgram();
//        List<ObjectMap> newSourceVersion = annotator.getVariantAnnotatorSourceVersion();

//        if (newSourceVersion == null) {
//            newSourceVersion = Collections.emptyList();
//        }
//        if (newAnnotator == null) {
//            throw new IllegalArgumentException("Missing annotator information for VariantAnnotator: " + annotator.getClass());
//        }
//        if (newSourceVersion.isEmpty()) {
//            throw new IllegalArgumentException("Missing annotator source version for VariantAnnotator: " + annotator.getClass());
//        }
        ProjectMetadata.VariantAnnotationMetadata newVariantAnnotationMetadata = annotator.getVariantAnnotationMetadata();
        return checkCurrentAnnotation(projectMetadata, overwrite, newVariantAnnotationMetadata);
    }

    protected final VariantAnnotationMetadata checkCurrentAnnotation(ProjectMetadata projectMetadata, boolean overwrite,
                                                                     VariantAnnotationMetadata newVariantAnnotationMetadata)
            throws VariantAnnotatorException {
        VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
        if (current == null) {
            current = new VariantAnnotationMetadata();
            projectMetadata.getAnnotation().setCurrent(current);
            current.setId(1);
            current.setName(CURRENT);
        }

        // Check using same annotator and same source version
        VariantAnnotatorProgram currentAnnotator = current.getAnnotator();
        VariantAnnotatorProgram newAnnotator = newVariantAnnotationMetadata.getAnnotator();
        if (currentAnnotator != null && !currentAnnotator.equals(newAnnotator)) {
            String currentVersion = removePatchFromVersion(currentAnnotator.getVersion());
            String newVersion = removePatchFromVersion(newAnnotator.getVersion());
            if (!currentAnnotator.getName().equals(newAnnotator.getName())
                    || !currentVersion.equals(newVersion)) {
                String msg = "Using a different annotator! "
                        + "Existing annotation calculated with " + currentAnnotator.toString()
                        + ", attempting to annotate with " + newAnnotator.toString();
                if (overwrite) {
                    logger.info(msg);
                } else {
                    throw new VariantAnnotatorException(msg);
                }
            } else if (!currentAnnotator.getCommit().equals(newAnnotator.getCommit())) {
                String msg = "Using a different patch version for annotating variants. "
                        + "Existing annotation calculated with " + currentAnnotator.toString()
                        + ", attempting to annotate with " + newAnnotator.toString();
                if (overwrite) {
                    logger.info(msg);
                } else {
                    logger.warn(msg);
                }
            }
        }

        if (current.getDataRelease() != null && newVariantAnnotationMetadata.getDataRelease() == null) {
            // Regression. DataRelease is lost.
            String msg = "DataRelease missing. "
                            + "Existing annotation calculated with dataRelease " + current.getDataRelease().getRelease()
                            + ", attempting to annotate without explicit dataRelease";

            if (overwrite) {
                logger.info(msg);
            } else {
                throw new VariantAnnotatorException(msg);
            }
        }

        List<String> currentPrivateSources = current.getPrivateSources();
        List<String> newPrivateSources = newVariantAnnotationMetadata.getPrivateSources();
        if (currentPrivateSources == null) {
            currentPrivateSources = Collections.emptyList();
        }
        if (newPrivateSources == null) {
            newPrivateSources = Collections.emptyList();
        }
        if (!new HashSet<>(currentPrivateSources).equals(new HashSet<>(newPrivateSources))) {
            String msg = "Private sources has changed. "
                    + "Existing annotation calculated with private sources " + currentPrivateSources
                    + ", attempting to annotate with " + newPrivateSources;

            if (overwrite) {
                logger.info(msg);
            } else {
                throw new VariantAnnotatorException(msg);
            }
        }

        if (newVariantAnnotationMetadata.getDataRelease() != null) {
            if (current.getDataRelease() == null) {
                // Missing current dataRelease. Continue.
            } else {
                if (!current.getDataRelease().equals(newVariantAnnotationMetadata.getDataRelease())) {
                    String msg = "DataRelease has changed. "
                            + "Existing annotation calculated with dataRelease " + current.getDataRelease().getRelease()
                            + ", attempting to annotate with " + newVariantAnnotationMetadata.getDataRelease().getRelease();

                    if (overwrite) {
                        logger.info(msg);
                    } else {
                        throw new VariantAnnotatorException(msg);
                    }
                }
            }
        } else {
            // Check sources for old cellbase versions
            List<ObjectMap> currentSourceVersion = current.getSourceVersion();
            List<ObjectMap> newSourceVersion = newVariantAnnotationMetadata.getSourceVersion();
            if (CollectionUtils.isNotEmpty(currentSourceVersion) && !sameSourceVersion(newSourceVersion, currentSourceVersion)) {
                String msg = "Source version of the annotator has changed. "
                        + "Existing annotation calculated with "
                        + currentSourceVersion.stream().map(ObjectMap::toJson).collect(Collectors.joining(" , ", "[ ", " ]"))
                        + ", attempting to annotate with "
                        + newSourceVersion.stream().map(ObjectMap::toJson).collect(Collectors.joining(" , ", "[ ", " ]"));

                if (overwrite) {
                    logger.info(msg);
                } else {
                    // List of sources from cellbase 5.0.x is not reliable, and should
                    // not be taken into account to force a full annotation overwrite
                    if (newAnnotator.getName().toLowerCase().contains("cellbase") && newAnnotator.getVersion().startsWith("5.0")) {
                        logger.warn(msg);
                        logger.info("Ignore source version change at Cellbase v5.0.x");
                    } else {
                        throw new VariantAnnotatorException(msg);
                    }
                }
            }
        }

        return current;
    }

    private static String removePatchFromVersion(String version) {
        String[] split = StringUtils.split(version, '.');
        if (split.length <= 1) {
            return version;
        }
        return split[0] + "." + split[1];
    }

    private boolean sameSourceVersion(List<ObjectMap> newSourceVersion, List<ObjectMap> currentSourceVersion) {
        if (currentSourceVersion.size() != newSourceVersion.size()) {
            return false;
        }
        Set<ObjectMap> newSourceVersionSet = new HashSet<>(newSourceVersion);
        return newSourceVersionSet.containsAll(currentSourceVersion);
    }

    protected final void updateCurrentAnnotation(VariantAnnotator annotator, ProjectMetadata projectMetadata, boolean overwrite)
            throws VariantAnnotatorException {
        updateCurrentAnnotation(annotator, projectMetadata, overwrite, annotator.getVariantAnnotationMetadata());
    }


    protected final void updateCurrentAnnotation(VariantAnnotator annotator, ProjectMetadata projectMetadata,
                                                 boolean overwrite, VariantAnnotationMetadata newAnnotationMetadata)
            throws VariantAnnotatorException {
        List<ObjectMap> newSourceVersion = newAnnotationMetadata.getSourceVersion();
        VariantAnnotatorProgram newAnnotator = newAnnotationMetadata.getAnnotator();
        if (newSourceVersion == null) {
            newSourceVersion = Collections.emptyList();
        }
        if (newAnnotator == null) {
            throw new IllegalArgumentException("Missing annotator information for VariantAnnotator: " + annotator.getClass());
        }
        if (newSourceVersion.isEmpty()) {
            throw new IllegalArgumentException("Missing annotator source version for VariantAnnotator: " + annotator.getClass());
        }
        checkCurrentAnnotation(projectMetadata, overwrite, newAnnotationMetadata);

        VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
        current.setAnnotator(newAnnotator);
        current.setSourceVersion(newSourceVersion);
        current.setDataRelease(newAnnotationMetadata.getDataRelease());
        current.setPrivateSources(newAnnotationMetadata.getPrivateSources());
    }

    protected final VariantAnnotationMetadata registerNewAnnotationSnapshot(String name, VariantAnnotator annotator,
                                                                                            ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {
        VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
        if (current == null) {
            // Should never enter here
            current = checkCurrentAnnotation(annotator, projectMetadata, true);
        }

        boolean nameDuplicated = projectMetadata.getAnnotation().getSaved()
                .stream()
                .map(VariantAnnotationMetadata::getName)
                .anyMatch(s -> s.equalsIgnoreCase(name))
                || VariantAnnotationManager.CURRENT.equalsIgnoreCase(name);

        if (nameDuplicated) {
            throw new VariantAnnotatorException("Annotation snapshot name already exists!");
        }

        VariantAnnotationMetadata newSnapshot = new VariantAnnotationMetadata(
                current.getId(),
                name,
                Date.from(Instant.now()),
                current.getAnnotator(),
                current.getSourceVersion(),
                current.getDataRelease(),
                current.getPrivateSources());
        projectMetadata.getAnnotation().getSaved().add(newSnapshot);

        // Increment ID of the current annotation
        current.setId(current.getId() + 1);

        return newSnapshot;
    }

    protected final VariantAnnotationMetadata removeAnnotationSnapshot(String name, ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {

        if (VariantAnnotationManager.CURRENT.equalsIgnoreCase(name)) {
            throw new VariantAnnotatorException("Can not delete " + VariantAnnotationManager.CURRENT + " annotation");
        }

        Iterator<VariantAnnotationMetadata> iterator = projectMetadata.getAnnotation().getSaved().iterator();
        VariantAnnotationMetadata annotation = null;
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
