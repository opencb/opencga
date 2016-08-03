package org.opencb.opencga.storage.core.variant.transform;

import org.opencb.commons.utils.FileUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.function.BiConsumer;

/**
 * Created on 02/08/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MalformedVariantHandler implements BiConsumer<String, RuntimeException> {

    private PrintStream out;
    private final Path outputFile;
    private long counter = 0;

    public MalformedVariantHandler(Path outputFile) throws IOException {
        this.outputFile = outputFile;
        FileUtils.checkDirectory(outputFile.toAbsolutePath().getParent(), true);
    }

    @Override
    public synchronized void accept(String line, RuntimeException exception) {
        counter++;

        if (out == null) {
            try {
                // Only create file if required
                out = new PrintStream(new FileOutputStream(outputFile.toFile()));
            } catch (FileNotFoundException e) {
                // This should not happen. Permissions already checked at creation.
                // May have changes?
                throw new UncheckedIOException(e);
            }
        }

        out.print("ERROR >>>>\t");
        out.println(line);
        exception.printStackTrace(out);

        out.flush();
    }

    public void close() {
        if (out != null) {
            out.close();
        }
    }

    public long getMalformedLines() {
        return counter;
    }
}
