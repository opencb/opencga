package org.opencb.opencga.app.cli.main.io;

import org.opencb.commons.datastore.core.QueryResponse;

import java.nio.file.Path;

/**
 * Created by pfurio on 28/07/16.
 */
public interface IWriter {

    /**
     * Prints the queryResponse to stdout.
     *
     * @param queryResponse queryResponse object to be printed.
     * @param beauty boolean indicating whether to print with a beauty format.
     */
    void print(QueryResponse queryResponse, boolean beauty);

    /**
     * Writes the output from queryResponse to the file.
     *
     * @param queryResponse queryResponse object to be written.
     * @param filePath file where the output will be written.
     * @param beauty boolean indicating whether to print with a beauty format.
     */
    void writeToFile(QueryResponse queryResponse, Path filePath, boolean beauty);

}
