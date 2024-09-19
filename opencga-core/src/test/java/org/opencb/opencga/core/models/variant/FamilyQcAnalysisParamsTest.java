package org.opencb.opencga.core.models.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Test;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.variant.qc.FamilyQcAnalysisParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class FamilyQcAnalysisParamsTest {

    @Test
    public void testObjectWriter() throws IOException {
        FamilyQcAnalysisParams params = new FamilyQcAnalysisParams();
        params.setFamilies(Arrays.asList("f1", "f2"));
        params.setOverwrite(true);

        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectWriter objectWriter = objectMapper.writerFor(FamilyQcAnalysisParams.class);
        ObjectReader objectReader = objectMapper.readerFor(FamilyQcAnalysisParams.class);

        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);


        Path outDir = Paths.get("/tmp/kk/");
        Files.exists(outDir);

        objectWriter.writeValue(outDir.resolve("config.json").toFile(), params);
        FamilyQcAnalysisParams params1 = objectReader.readValue(outDir.resolve("config.json").toFile());
        System.out.println("(params1.getFamily() == null) = " + (params1.getFamily() == null));
        System.out.println("params1.getOverwrite() = " + params1.getOverwrite());
        System.out.println("Boolean.TRUE.equals(params1.getOverwrite()) = " + Boolean.TRUE.equals(params1.getOverwrite()));
//        System.out.println("(params1.getOverwrite() == true) = " + (true == params1.getOverwrite()));
        System.out.println("params1.getSkipIndex() = " + params1.getSkipIndex());
        System.out.println("Boolean.TRUE.equals(params1.getSkipIndex()) = " + Boolean.TRUE.equals(params1.getSkipIndex()));
//        System.out.println("(params1.getSkipIndex() == true) = " + (true == params1.getSkipIndex()));
        System.out.println("params1 = " + params1);
    }
}