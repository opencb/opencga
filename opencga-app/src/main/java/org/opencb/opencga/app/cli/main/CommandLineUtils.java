package org.opencb.opencga.app.cli.main;

import org.apache.commons.lang3.StringUtils;
import org.fusesource.jansi.Ansi;

import java.util.Arrays;
import java.util.List;

import static org.fusesource.jansi.Ansi.Color.*;
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


    @Deprecated
    public static void printlnBlue(String message) {
        System.out.println(ansi().fg(BLUE).a(message).reset());
    }

    @Deprecated
    public static void printBlue(String message) {
        System.out.print(ansi().fg(BLUE).a(message).reset());
    }

    @Deprecated
    public static void printlnGreen(String message) {
        System.out.println(ansi().fg(GREEN).a(message).reset());
    }

    @Deprecated
    public static void printGreen(String message) {
        System.out.print(ansi().fg(GREEN).a(message).reset());
    }

    @Deprecated
    public static void printlnYellow(String message) {
        System.out.println(ansi().fg(YELLOW).a(message).reset());
    }

    @Deprecated
    public static void printYellow(String message) {
        System.out.print(ansi().fg(YELLOW).a(message).reset());
    }

    @Deprecated
    public static void printlnRed(String message) {
        System.out.println(ansi().fg(RED).a(message).reset());
    }

    @Deprecated
    public static void printRed(String message) {
        System.out.print(ansi().fg(RED).a(message).reset());
    }

    public static void printColor(String message, String color) {
        printColor(message, color, true);
    }

    public static void printColor(String message, String color, boolean newLine) {
        Ansi.Color ansiColor = StringUtils.isNotEmpty(color) ? valueOf(color.toUpperCase()) : valueOf("DEFAULT");
        if (newLine) {
            System.out.println(ansi().fg(ansiColor).a(message).reset());
        } else {
            System.out.print(ansi().fg(ansiColor).a(message).reset());
        }
    }


}
