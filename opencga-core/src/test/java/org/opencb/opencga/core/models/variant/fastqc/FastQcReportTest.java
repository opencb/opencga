package org.opencb.opencga.core.models.variant.fastqc;

import org.junit.Test;
import org.opencb.opencga.core.tools.utils.FastQcParser;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FastQcReportTest {

    @Test
    public void parse() throws IOException {
        // ~/data150/data/corpasome/SonsAlignedBamFile_fastqc/fastqc_data.txt
        Path path = Paths.get("");
        FastQcReport fastQcReport = FastQcParser.parse(path.toFile());
        System.out.println(fastQcReport);
    }
}