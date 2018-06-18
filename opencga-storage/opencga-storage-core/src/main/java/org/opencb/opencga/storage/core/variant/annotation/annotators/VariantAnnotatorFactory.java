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

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.ProjectMetadata;
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

    public enum AnnotationSource {
        CELLBASE_DB_ADAPTOR,
        CELLBASE_REST,
        VEP,
        OTHER
    }

    protected static Logger logger = LoggerFactory.getLogger(VariantAnnotatorFactory.class);

    public static VariantAnnotator buildVariantAnnotator(StorageConfiguration configuration, String storageEngineId,
                                                         ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {
        return buildVariantAnnotator(configuration, storageEngineId, projectMetadata, null);
    }

    public static VariantAnnotator buildVariantAnnotator(StorageConfiguration configuration, String storageEngineId,
                                                         ProjectMetadata projectMetadata, ObjectMap options)
            throws VariantAnnotatorException {
        ObjectMap storageOptions = new ObjectMap(configuration.getStorageEngine(storageEngineId).getVariant().getOptions());
        if (options != null) {
            options.forEach(storageOptions::putIfNotNull);
        }
        AnnotationSource defaultValue = storageOptions.containsKey(VARIANT_ANNOTATOR_CLASSNAME)
                ? AnnotationSource.OTHER
                : AnnotationSource.CELLBASE_REST;
        AnnotationSource annotationSource;
        if (StringUtils.isNotBlank(storageOptions.getString(ANNOTATOR))) {
            annotationSource = AnnotationSource.valueOf(storageOptions.getString(ANNOTATOR).toUpperCase());
        } else if (StringUtils.isNotBlank(storageOptions.getString(ANNOTATION_SOURCE))) {
            annotationSource = AnnotationSource.valueOf(storageOptions.getString(ANNOTATION_SOURCE).toUpperCase());
            logger.warn("Using deprecated parameter '" + ANNOTATION_SOURCE + "'. Use '" + ANNOTATOR + "' instead");
        } else {
            annotationSource = defaultValue;
        }

        switch (annotationSource) {
            case CELLBASE_DB_ADAPTOR:
                return new CellBaseDirectVariantAnnotator(configuration, projectMetadata, storageOptions);
            case CELLBASE_REST:
                return new CellBaseRestVariantAnnotator(configuration, projectMetadata, storageOptions);
            case VEP:
                return VepVariantAnnotator.buildVepAnnotator();
            case OTHER:
            default:
                String className = storageOptions.getString(VARIANT_ANNOTATOR_CLASSNAME);
                logger.info("Annotating with {} = {}", annotationSource, className);
                try {
                    Class<?> clazz = Class.forName(className);
                    if (VariantAnnotator.class.isAssignableFrom(clazz)) {
                        return (VariantAnnotator) clazz.getConstructor(StorageConfiguration.class, ProjectMetadata.class, ObjectMap.class)
                                .newInstance(configuration, projectMetadata, storageOptions);
                    } else {
                        throw new VariantAnnotatorException("Invalid VariantAnnotator class: " + className);
                    }
                } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
                        | IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                    throw new VariantAnnotatorException("Unable to create annotation source from \"" + className + "\"", e);
                }
        }

    }

}
