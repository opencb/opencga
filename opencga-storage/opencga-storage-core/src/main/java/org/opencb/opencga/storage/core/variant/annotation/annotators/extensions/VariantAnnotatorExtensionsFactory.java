package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;

public class VariantAnnotatorExtensionsFactory {

    public List<VariantAnnotatorExtensionTask> getVariantAnnotatorExtensions(ObjectMap options) {

        List<VariantAnnotatorExtensionTask> tasks = new LinkedList<>();
        for (String extensionId : options.getAsStringList(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key())) {
            VariantAnnotatorExtensionTask task = null;
            switch (extensionId) {
//                case CosmicVariantAnnotatorExtensionTask.ID:
//                    task = new CosmicVariantAnnotatorExtensionTask(options);
//                    break;
                default:
                    String extensionClass = options.getString(VariantStorageOptions.ANNOTATOR_EXTENSION_PREFIX.key() + extensionId);
                    if (extensionClass != null) {
                        task = getVariantAnnotatorExtension(extensionClass, options);
                    } else {
                        throw new IllegalArgumentException("Unknown annotator extension '" + extensionId + "'");
                    }
            }

            if (task == null) {
                throw new IllegalArgumentException("Unable to create annotator extension '" + extensionId + "'");
            }

            tasks.add(task);
        }
        return tasks;
    }

    private VariantAnnotatorExtensionTask getVariantAnnotatorExtension(String className, ObjectMap options) {
        try {
            Class<?> clazz = Class.forName(className);
            return (VariantAnnotatorExtensionTask) clazz.getConstructor(ObjectMap.class).newInstance(options);
        } catch (ClassNotFoundException
                 | NoSuchMethodException
                 | InstantiationException
                 | IllegalAccessException
                 | InvocationTargetException e) {
            throw new IllegalArgumentException("Unable to create VariantAnnotatorExtensionTask from class " + className, e);
        }
    }


}
