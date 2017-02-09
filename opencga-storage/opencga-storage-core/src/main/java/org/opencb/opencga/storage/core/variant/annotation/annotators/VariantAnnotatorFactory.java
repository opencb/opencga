package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.ANNOTATION_SOURCE;
import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.VARIANT_ANNOTATOR_CLASSNAME;

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

    public static VariantAnnotator buildVariantAnnotator(StorageConfiguration configuration, String storageEngineId)
            throws VariantAnnotatorException {
        return buildVariantAnnotator(configuration, storageEngineId, null);
    }

    public static VariantAnnotator buildVariantAnnotator(StorageConfiguration configuration, String storageEngineId, ObjectMap options)
            throws VariantAnnotatorException {
        ObjectMap storageOptions = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();
        if (options != null) {
            storageOptions.putAll(options);
        }
        String defaultValue = storageOptions.containsKey(VARIANT_ANNOTATOR_CLASSNAME)
                ? AnnotationSource.OTHER.name()
                : AnnotationSource.CELLBASE_REST.name();
        AnnotationSource annotationSource =
                AnnotationSource.valueOf(storageOptions.getString(ANNOTATION_SOURCE, defaultValue).toUpperCase());


        switch (annotationSource) {
            case CELLBASE_DB_ADAPTOR:
                return new CellBaseDirectVariantAnnotator(configuration, storageOptions);
            case CELLBASE_REST:
                return new CellBaseRestVariantAnnotator(configuration, storageOptions);
            case VEP:
                return VepVariantAnnotator.buildVepAnnotator();
            case OTHER:
            default:
                String className = storageOptions.getString(VARIANT_ANNOTATOR_CLASSNAME);
                logger.info("Annotating with {} = {}", annotationSource, className);
                try {
                    Class<?> clazz = Class.forName(className);
                    if (VariantAnnotator.class.isAssignableFrom(clazz)) {
                        return (VariantAnnotator) clazz.getConstructor(StorageConfiguration.class, ObjectMap.class)
                                .newInstance(configuration, storageOptions);
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
