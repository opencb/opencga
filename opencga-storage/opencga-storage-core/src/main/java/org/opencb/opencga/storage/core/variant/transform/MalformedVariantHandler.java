/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
