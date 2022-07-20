package org.opencb.opencga.core.params;

import org.opencb.commons.annotations.DataField;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ModelClass {

    private static Set<String> constants = new HashSet<>();
    private Path path;
    private Class clase;
    private String canonicalName;
    private List<ParamDescription> params;

    public ModelClass(Path path) throws Exception {
        this.path = path;
        this.canonicalName = getCanonicalName(path);
        this.clase = Class.forName(canonicalName);
        params = getParamDescriptions();
    }

    @Override
    public String toString() {
        return "ModelClass{" +
                "path=" + path +
                ", clase=" + clase +
                ", canonicalName='" + canonicalName + '\'' +
                ", params=" + params +
                '}';
    }

    private String getCanonicalName(Path ruta) throws Exception {
        AtomicReference<String> res = new AtomicReference<>("");
        try (Stream<String> lines = Files.lines(ruta)) {
            lines.forEach(line -> {
                if (line.startsWith("package")) {
                    res.set(line.replaceAll("package ", "").replaceAll(";", ""));
                }
            });
        }
        String className = ruta.toString().substring(ruta.toString().lastIndexOf("/"))
                .replaceAll("\\.java", "").replaceAll("/", ".");
        return res.get() + className;
    }

    private List<ParamDescription> getParamDescriptions() {
        Field[] fields = clase.getDeclaredFields();
        List<ParamDescription> res = new ArrayList<>();
        for (Field field : fields) {
            Annotation annotation = field.getAnnotation(DataField.class);
            if (annotation == null && !Modifier.isStatic(field.getModifiers()) && !Modifier.isFinal(field.getModifiers()) && Modifier.isPrivate(field.getModifiers())) {
                String name = getAsFinalVariable(clase.getSimpleName()) + "_" + getAsFinalVariable(field.getName()) + "_DESCRIPTION";
                String value = "The body " + field.getName() + " " + clase.getSimpleName() + " web service parameter";
                ParamDescription pd = new ParamDescription(name, value, field.getName(), field.getType().getSimpleName());
                res.add(pd);
            }
        }
        return res;
    }

    private String getAsFinalVariable(String toUpperCase) {

        String ret = toUpperCase.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z])([A-Z])", "$1_$2");

        return ret.replaceAll("-", "_").toUpperCase();
    }

    public Path getPath() {
        return path;
    }

    public ModelClass setPath(Path path) {
        this.path = path;
        return this;
    }

    public Class getClase() {
        return clase;
    }

    public ModelClass setClase(Class clase) {
        this.clase = clase;
        return this;
    }

    public String getCanonicalName() {
        return canonicalName;
    }

    public ModelClass setCanonicalName(String canonicalName) {
        this.canonicalName = canonicalName;
        return this;
    }

    public List<ParamDescription> getParams() {
        return params;
    }

    public ModelClass setParams(List<ParamDescription> params) {
        this.params = params;
        return this;
    }


    public String writeDataFieldTags() {
        String res = "";
        try {
            BufferedReader buffer = new BufferedReader(new FileReader(path.toFile()));
            String linea;
            boolean insertedDataField = false;
            boolean insertedParamConstants = false;
            boolean insertedImports = false;
            while ((linea = buffer.readLine()) != null) {
                if (linea.contains("import org.opencb.commons.annotations.DataField;")) {
                    insertedDataField = true;
                }
                if (linea.contains("import org.opencb.opencga.core.api.ParamConstants;")) {
                    insertedParamConstants = true;
                }

                if ((!insertedImports) && (linea.startsWith("public class ") || linea.startsWith("abstract ") || linea.startsWith("public interface ") || linea.startsWith("@") || linea.startsWith("public abstract class "))) {
                    if (!insertedDataField) {
                        res += "import org.opencb.commons.annotations.DataField;\n";
                    }
                    if (!insertedParamConstants) {
                        res += "import org.opencb.opencga.core.api.ParamConstants;\n";
                    }
                    res += "\n";
                    insertedImports = true;
                }
                res += getLine(params, linea);
            }
            buffer.close();
            BufferedWriter bufferW = new BufferedWriter(new FileWriter(path.toFile()));
            bufferW.write(res);
            bufferW.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return res;
    }

    public void writeConstants(String paramConstantsPath) {
        String toWrite = "";
        for (ParamDescription param : params) {
            try {
                Field f = org.opencb.opencga.core.api.ParamConstants.class.getDeclaredField(param.getName());
            } catch (Exception ignore) {
                if (!constants.contains(param.getConstant())) {
                    toWrite += param.getConstant() + "\n";
                    constants.add(param.getConstant());
                }
            }
        }
        writeParamConstants(paramConstantsPath, toWrite);
    }

    private String writeParamConstants(String paramConstantsPath, String toWrite) {
        String res = "";
        try {
            BufferedReader buffer = new BufferedReader(new FileReader(paramConstantsPath));
            String linea;
            boolean inserted = false;
            while ((linea = buffer.readLine()) != null) {
                res += linea + "\n";
            }
            res = res.substring(0, res.lastIndexOf("}"));
            res += "\n" + toWrite + "\n}\n";
            buffer.close();
            BufferedWriter bufferW = new BufferedWriter(new FileWriter(paramConstantsPath));
            bufferW.write(res);
            bufferW.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return res;
    }

    private String getLine(List<ParamDescription> params, String linea) {
        for (ParamDescription param : params) {
            if (linea.contains("private ") && linea.contains(param.getType()) && linea.contains(param.getFieldName() + ";")) {
                return param.getDataFieldTag() + "\n" + linea + "\n";
            }
        }
        return linea + "\n";
    }

}
