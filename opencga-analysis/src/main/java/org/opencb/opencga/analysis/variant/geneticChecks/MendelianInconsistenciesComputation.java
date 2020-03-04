package org.opencb.opencga.analysis.variant.geneticChecks;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.MendelianErrorsReport;
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

    public static MendelianErrorsReport compute(String study, String family, List<String> samples, Path outDir, VariantStorageManager storageManager, String token)
            throws ToolException {
        // TODO implement using variant query
        return null;
//        // run Mendel and return the result file
//        runMendel(family, outDir);
//
//        return GeneticChecksUtils.buildMendelianErrorsReport(family, outDir);
    }

//    private static File runMendel(String basename, Path outDir) throws ToolException {
//        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
//                "/data/output");
//
//        // Compute Mendelian inconsistencies using PLINK allowing for duos and multiple generations
//        String plinkParams = "plink --tfile /data/output/" + basename + " --mendel --mendel-duos --mendel-multigen --out /data/output/"
//                + basename;
//        try {
//            DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
//        } catch (IOException e) {
//            throw new ToolException(e);
//        }
//
//        // Check output file
//        File outFile = new File(outputBinding.getKey() + "/" + basename + ".imendel");
//        if (!outFile.exists()) {
//            throw new ToolException("Something wrong executing mendelian inconsitencies for genetic checks analysis in PLINK docker.");
//        }
//
//        return outFile;
//    }
}
