package org.opencb.opencga.catalog;

import org.opencb.opencga.catalog.CatalogException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ParamsUtils {
    public static void checkId(int id, String name) throws CatalogException {
        if (id < 0) {
            throw new CatalogException("Error in id: '" + name + "' is not valid: "
                    + id + ".");
        }
    }

    public static void checkParameter(String param, String name) throws CatalogException {
        if (param == null || param.equals("") || param.equals("null")) {
            throw new CatalogException("Error in parameter: parameter '" + name + "' is null or empty: "
                    + param + ".");
        }
    }

    public static void checkParameters(String... args) throws CatalogException {
        if (args.length % 2 == 0) {
            for (int i = 0; i < args.length; i += 2) {
                checkParameter(args[i], args[i + 1]);
            }
        } else {
            throw new CatalogException("Error in parameter: parameter list is not multiple of 2");
        }
    }

    public static void checkObj(Object obj, String name) throws CatalogException {
        if (obj == null) {
            throw new CatalogException("parameter '" + name + "' is null.");
        }
    }

    public static void checkRegion(String regionStr, String name) throws CatalogException {
        if (Pattern.matches("^([a-zA-Z0-9])+:([0-9])+-([0-9])+$", regionStr)) {//chr:start-end
            throw new CatalogException("region '" + name + "' is not valid");
        }
    }

    public static void checkPath(String path, String name) throws CatalogException {
        if (path == null) {
            throw new CatalogException("parameter '" + name + "' is null.");
        }
        checkPath(Paths.get(path), name);
    }

    public static void checkPath(Path path, String name) throws CatalogException {
        checkObj(path, name);
        if (path.isAbsolute()) {
            throw new CatalogException("Error in path: Path '" + name + "' can't be absolute");
        } else if (path.toString().matches("\\.|\\.\\.")) {
            throw new CatalogException("Error in path: Path '" + name + "' can't have relative names '.' or '..'");
        }
    }

    public static void checkAlias(String alias, String name) throws CatalogException {
        if (alias == null || alias.isEmpty() || !alias.matches("^[_A-Za-z0-9-\\+]+$")) {
            throw new CatalogException("Error in alias: Invalid alias for '" + name + "'.");
        }
    }

    public static String defaultString(String string, String defaultValue) {
        if (string == null || string.isEmpty()) {
            string = defaultValue;
        }
        return string;
    }

    public static <O> O defaultObject(O object, O defaultObject) {
        if (object == null) {
            object = defaultObject;
        }
        return object;
    }

    public static <O> O defaultObject(O object, Supplier<O> supplier) {
        if (object == null) {
            object = supplier.get();
        }
        return object;
    }
}
