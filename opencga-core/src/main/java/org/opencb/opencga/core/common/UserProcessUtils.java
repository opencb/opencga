package org.opencb.opencga.core.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class UserProcessUtils {

    public static String getUserUid() {
        try {
            return execCommand("id -u");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getGroupId() {
        try {
            return execCommand("id -g");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Method to execute a shell command and get the output
    private static String execCommand(String command) throws Exception {
        ProcessBuilder builder = new ProcessBuilder("sh", "-c", command);
        Process process = builder.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = reader.readLine(); // Read the output
        reader.close();

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Command failed with exit code: " + exitCode);
        }

        return line;
    }

}
