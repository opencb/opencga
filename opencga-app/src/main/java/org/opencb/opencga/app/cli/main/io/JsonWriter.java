package org.opencb.opencga.app.cli.main.io;

import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.QueryResponse;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

/**
 * Created by pfurio on 28/07/16.
 */
public class JsonWriter implements IWriter {

    @Override
    public void print(QueryResponse queryResponse, boolean beauty) {
        if (!checkErrors(queryResponse)) {
            generalPrint(queryResponse, beauty);
        }
    }

    @Override
    public void writeToFile(QueryResponse queryResponse, Path filePath, boolean beauty) {
        if (checkErrors(queryResponse)) {
            return;
        }

        // Redirect the output
        try {
            FileOutputStream fos = new FileOutputStream(filePath.toFile());
            PrintStream ps = new PrintStream(fos);
            System.setOut(ps);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        generalPrint(queryResponse, beauty);
    }

    /**
     * Print errors or warnings and return true if any error was found.
     *
     * @param queryResponse queryResponse object
     * @return true if the query gave an error.
     */
    private boolean checkErrors(QueryResponse queryResponse) {
        if (!queryResponse.getError().isEmpty()) {
            System.out.println(queryResponse.getError());
            return true;
        }

        // Print warnings
        if (!queryResponse.getWarning().isEmpty()) {
            System.out.println(queryResponse.getWarning());
        }

        return false;
    }

    private void generalPrint(QueryResponse queryResponse, boolean beauty) {
        if (beauty) {
            try {
                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().
                        writeValueAsString(queryResponse.getResponse()));
            } catch (IOException e) {
                System.out.println("Error parsing the queryResponse to print as a beautiful JSON");
            }
        } else {
            System.out.println(queryResponse.getResponse());
        }
    }
}
