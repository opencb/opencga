package org.opencb.opencga.analysis.variant.relatedness;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.List;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.TPED;

public class IBDComputation {

    public static File compute(String study, List<String> samples, Path outDir, VariantStorageManager storageManager, String token)
            throws ToolException {
        String filename = "variants";
        File tpedFile = outDir.resolve(filename + ".tped").toFile();
        File tfamFile = outDir.resolve(filename + ".tfam").toFile();

        // Export variants in format .tped and .tfam to run plink
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.SAMPLE.key(), samples);
        //.append(VariantQueryParam.FILTER.key(), "PASS")

        try {
            storageManager.exportData(tpedFile.getAbsolutePath(), TPED, null, query, QueryOptions.empty(), token);
        } catch (CatalogException | IOException | StorageEngineException e) {
            throw new ToolException(e);
        }

        if (!tpedFile.exists() || !tfamFile.exists()) {
            throw new ToolException("Something wrong exporting data to TPED/TFAM format");
        }

        // Execute PLINK in docker
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(outDir.toAbsolutePath().toString(),
                "/data/output");
        String plinkParams = "--noweb -tfile /data/output/variants --genome --out /data/output/relatedness";

        String cmdline = null;
        try {
            cmdline = DockerUtils.run(PlinkWrapperAnalysis.PLINK_DOCKER_IMAGE, null, outputBinding, plinkParams, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }
        System.out.println("Docker command line: " + cmdline);

        // Check output file
        File outFile = new File(outDir + "/relatedness.genome");
        if (!outFile.exists()) {
            throw new ToolException("Something wrong executing relatedness analysis (i.e., IBD/IBS computation) in PLINK docker.");
        }

        // return output file
        return outFile;
    }
}
