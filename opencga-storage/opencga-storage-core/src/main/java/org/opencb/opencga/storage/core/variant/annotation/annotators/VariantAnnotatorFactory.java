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

package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.*;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class VariantAnnotatorFactory {

    public enum AnnotationEngine {
        CELLBASE_REST,
        CELLBASE,
        VEP,
        OTHER
    }

    protected static Logger logger = LoggerFactory.getLogger(VariantAnnotatorFactory.class);

    public static VariantAnnotator buildVariantAnnotator(StorageConfiguration configuration,
                                                         ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {
        return buildVariantAnnotator(configuration, projectMetadata, configuration.getVariant().getOptions());
    }

    public static VariantAnnotator buildVariantAnnotator(StorageConfiguration configuration,
                                                         ProjectMetadata projectMetadata, ObjectMap options)
            throws VariantAnnotatorException {

        AnnotationEngine defaultValue = options.containsKey(VariantStorageOptions.ANNOTATOR_CLASS.key())
                ? AnnotationEngine.OTHER
                : AnnotationEngine.CELLBASE;
        AnnotationEngine annotationEngine;
        String annotator = options.getString(VariantStorageOptions.ANNOTATOR.key());
        if (StringUtils.isNotBlank(annotator)) {
            annotationEngine = AnnotationEngine.valueOf(annotator.toUpperCase());
        } else if (StringUtils.isNotBlank(options.getString(ANNOTATION_SOURCE))) {
            annotationEngine = AnnotationEngine.valueOf(options.getString(ANNOTATION_SOURCE).toUpperCase());
            logger.warn("Using deprecated parameter '" + ANNOTATION_SOURCE + "'. Use '" + VariantStorageOptions.ANNOTATOR + "' instead");
        } else {
            annotationEngine = defaultValue;
        }

        switch (annotationEngine) {
            case CELLBASE_REST:
            case CELLBASE:
                return new CellBaseRestVariantAnnotator(configuration, projectMetadata, options);
            case VEP:
                return VepVariantAnnotator.buildVepAnnotator();
            case OTHER:
                String className = options.getString(VariantStorageOptions.ANNOTATOR_CLASS.key());
                logger.info("Annotating with {} = {}", annotationEngine, className);
                try {
                    Class<?> clazz = Class.forName(className);
                    if (VariantAnnotator.class.isAssignableFrom(clazz)) {
                        return (VariantAnnotator) clazz.getConstructor(StorageConfiguration.class, ProjectMetadata.class, ObjectMap.class)
                                .newInstance(configuration, projectMetadata, options);
                    } else {
                        throw new VariantAnnotatorException("Invalid VariantAnnotator class: " + className);
                    }
                } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
                        | IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                    throw new VariantAnnotatorException("Unable to create annotation source from \"" + className + "\"", e);
                }
            default:
                throw new IllegalArgumentException("Unknown annotation engine " + annotationEngine);
        }

    }

}
