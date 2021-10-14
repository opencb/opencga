package org.opencb.opencga.app.cli.main;

import java.util.Arrays;
import java.util.List;

public class CommandLineUtils {

    public static List getListValues(String value) {
        String[] vec = value.split(",");
        for (int i = 0; i < vec.length; i++) {
            vec[i] = vec[i].trim();
        }
        return Arrays.asList(vec);
    }
}
