package org.opencb.opencga.core.params;

import java.nio.file.Files;
import java.nio.file.Paths;

public class ModelParamsGenerator {


    public static void main(String[] args) {
        String workspace = args[0];
        String paramConstantsPath = workspace + "/opencga/opencga-core/src/main/java/org/opencb/opencga/core/api/ParamConstants.java";
        try {
            Files.walk(Paths.get(workspace + "/opencga/opencga-core/src/main/java/org/opencb/opencga/core/models")).forEach(ruta -> {
                try {
                    if (!Files.isDirectory(ruta) && ruta.toString().endsWith("java")) {
                        ModelClass mc = new ModelClass(ruta);
                        mc.writeDataFieldTags();
                        mc.writeConstants(paramConstantsPath);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
