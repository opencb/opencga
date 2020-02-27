package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.*;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.TPED;

public class MendelianInconsistenciesComputation {

    public static File compute(String study, List<String> samples, String population, Path outDir, VariantStorageManager storageManager, String token)
            throws ToolException {
        String basename = "variants";

        // Select markers
        if (!outDir.resolve(basename + ".tped").toFile().exists() || !outDir.resolve(basename + ".tfam").toFile().exists()) {
            GeneticChecksUtils.selectMarkers(basename, study, samples, population, outDir, storageManager, token);
        }

        // run Mendel and return the result file
        return runMendel("variants", outDir);
    }

    private static File runMendel(String basename, Path outDir) throws ToolException {
        String outFilename = "out";
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");

        // Compute Mendelian inconsistencies using PLINK allowing for duos and multiple generations
        String plinkParams = "plink --tfile /data/output/" + basename + " --mendel --mendel-duos --mendel-multigen --out /data/output/"
                + outFilename;
        try {
            DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }

        // Check output file
        File outFile = new File(outputBinding.getKey() + "/" + outFilename + ".imendel");
        if (!outFile.exists()) {
            throw new ToolException("Something wrong executing mendelian inconsitencies for genetic checks analysis in PLINK docker.");
        }

        return outFile;
    }
}
