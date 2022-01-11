package org.opencb.opencga.app.cli.main;

import org.apache.commons.lang3.StringUtils;
import org.fusesource.jansi.Ansi;

import java.util.Arrays;
import java.util.List;

import static org.fusesource.jansi.Ansi.Color.valueOf;
import static org.fusesource.jansi.Ansi.ansi;

public class CommandLineUtils {

    public static List getListValues(String value) {
        String[] vec = value.split(",");
        for (int i = 0; i < vec.length; i++) {
            vec[i] = vec[i].trim();
        }
        return Arrays.asList(vec);
    }

    public static void printColor(String color, String message) {
        printColor(color, message, true);
    }

    public static void printColor(String color, String message, boolean newLine) {
        Ansi.Color ansiColor = StringUtils.isNotEmpty(color) ? valueOf(color.toUpperCase()) : valueOf(Color.DEFAULT);
        if (newLine) {
            System.out.println(ansi().fg(ansiColor).a(message).reset());
        } else {
            System.out.print(ansi().fg(ansiColor).a(message).reset());
        }
    }

    public class Color {
        public final static String BLACK = "BLACK";
        public final static String RED = "RED";
        public final static String GREEN = "GREEN";
        public final static String YELLOW = "YELLOW";
        public final static String BLUE = "BLUE";
        public final static String MAGENTA = "MAGENTA";
        public final static String CYAN = "CYAN";
        public final static String WHITE = "WHITE";
        public final static String DEFAULT = "DEFAULT";
    }
}
