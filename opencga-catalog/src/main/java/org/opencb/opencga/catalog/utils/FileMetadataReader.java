/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import htsjdk.samtools.SAMFileHeader;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileRelatedFile;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileMetadataReader {

    @Deprecated
    public static final String VARIANT_FILE_STATS = "variantFileStats";
    @Deprecated
    public static final String VARIANT_STATS = "variantStats";
    @Deprecated
    public static final String VARIANT_SOURCE = "variantSource";

    public static final String VARIANT_FILE_METADATA = "variantFileMetadata";
    public static final String FILE_VARIANT_STATS_VARIABLE_SET = "opencga_file_variant_stats";

    private final CatalogManager catalogManager;
    protected static Logger logger = LoggerFactory.getLogger(FileMetadataReader.class);

    public FileMetadataReader(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public void addMetadataInformation(String studyId, File file) throws CatalogException {
        final FileUpdateParams updateParams = extractMetadataInformation(studyId, file);
        file.setBioformat(updateParams.getBioformat() != null ? updateParams.getBioformat() : file.getBioformat());
        file.setFormat(updateParams.getFormat() != null ? updateParams.getFormat() : file.getFormat());
        file.setAttributes(updateParams.getAttributes() != null ? updateParams.getAttributes() : file.getAttributes());
        file.setSampleIds(updateParams.getSampleIds() != null ? updateParams.getSampleIds() : file.getSampleIds());
        file.setSize(updateParams.getSize() != null ? updateParams.getSize() : file.getSize());
    }

    public File updateMetadataInformation(String studyId, File file, String token) throws CatalogException {
        try {
            FileUpdateParams updateParams = extractMetadataInformation(studyId, file);
            ObjectMap updateMap = updateParams.getUpdateMap();

            if (!updateMap.isEmpty()) {
                if (updateParams.getSampleIds() != null && !updateParams.getSampleIds().isEmpty()) {
                    // Check and create missing samples
                    List<String> missingSamples = new LinkedList<>(updateParams.getSampleIds());
                    catalogManager.getSampleManager()
                            .iterator(studyId,
                                    new Query(SampleDBAdaptor.QueryParams.ID.key(), updateParams.getSampleIds()),
                                    new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()), token)
                            .forEachRemaining(sample -> {
                                missingSamples.remove(sample.getId());
                            });
                    if (!missingSamples.isEmpty()) {
                        for (String missingSample : missingSamples) {
                            catalogManager.getSampleManager().create(studyId, new Sample().setId(missingSample), new QueryOptions(), token);
                        }
                    }
                }

                int samplesBatchSize = 4000;
                if (updateParams.getSampleIds() != null && updateParams.getSampleIds().size() > samplesBatchSize) {
                    // Update sampleIds in batches
                    List<String> sampleIds = updateParams.getSampleIds();
                    updateParams.setSampleIds(null);
                    int numMatches = 1 + sampleIds.size() / samplesBatchSize;
                    for (int i = 0; i < numMatches; i++) {
                        List<String> subList = sampleIds.subList(
                                i * samplesBatchSize,
                                Math.min((i + 1) * samplesBatchSize, sampleIds.size()));
                        FileUpdateParams partialUpdate = new FileUpdateParams().setSampleIds(subList);

                        // SET for the first batch, then ADD
                        ParamUtils.UpdateAction action = i == 0
                                ? ParamUtils.UpdateAction.SET
                                : ParamUtils.UpdateAction.ADD;
                        catalogManager.getFileManager().update(studyId, file.getUuid(), partialUpdate,
                                new QueryOptions(Constants.ACTIONS,
                                        Collections.singletonMap(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), action.toString())), token);
                    }

                    catalogManager.getFileManager().update(studyId, file.getUuid(), updateParams, QueryOptions.empty(), token);
                } else {
                    catalogManager.getFileManager().update(studyId, file.getUuid(), updateParams,
                            new QueryOptions(Constants.ACTIONS,
                                    Collections.singletonMap(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(),
                                            ParamUtils.UpdateAction.SET.toString())), token);
                }
                return catalogManager.getFileManager().get(studyId, file.getUuid(), QueryOptions.empty(), token).first();
            }
        } catch (JsonProcessingException e) {
            throw new CatalogException("Unexpected error converting FileUpdateParams object: " + e.getMessage(), e);
        }
        return file;
    }

    private FileUpdateParams extractMetadataInformation(String studyId, File file) throws CatalogException {
        ParamUtils.checkObj(file.getUri(), "uri");

        if (file.getType() == File.Type.DIRECTORY) {
            return new FileUpdateParams();
        }

        FileUpdateParams updateParams = new FileUpdateParams();

        File.Format format = FileUtils.detectFormat(file.getUri());
        File.Bioformat bioformat = FileUtils.detectBioformat(file.getUri());

        if (format != File.Format.UNKNOWN && !format.equals(file.getFormat())) {
            updateParams.setFormat(format);
        }
        if (bioformat != File.Bioformat.NONE && !bioformat.equals(file.getBioformat())) {
            updateParams.setBioformat(bioformat);
        }

        switch (bioformat) {
            case ALIGNMENT: {
                SAMFileHeader alignmentHeader;
                try {
                    alignmentHeader = readAlignmentHeader(file, file.getUri());
                } catch (CatalogIOException e) {
                    throw new CatalogIOException("Unable to read alignment header", e);
                }
                if (alignmentHeader != null) {
                    try {
                        Map<String, Object> alignmentHeaderMap = JacksonUtils.getDefaultObjectMapper().readValue(
                                JacksonUtils.getDefaultObjectMapper().writeValueAsString(alignmentHeader), Map.class);
                        updateParams.setAttributes(file.getAttributes());
                        updateParams.getAttributes().put("alignmentHeader", alignmentHeaderMap);
                    } catch (IOException e) {
                        throw new CatalogIOException("Could not parse SAMFileHeader to Map", e);
                    }
                }
                break;
            }
            case VARIANT: {
                VariantFileMetadata fileMetadata;
                try {
                    fileMetadata = readVariantFileMetadata(file, file.getUri());
                } catch (IOException e) {
                    throw new CatalogIOException("Unable to read VariantSource", e);
                }
                if (fileMetadata != null) {
                    try {
                        Map<String, Object> fileMetadataMap = JacksonUtils.getDefaultObjectMapper()
                                .readValue(JacksonUtils.getDefaultObjectMapper().writeValueAsString(fileMetadata), Map.class);
                        if (file.getAttributes() == null) {
                            updateParams.setAttributes(new HashMap<>());
                        } else {
                            updateParams.setAttributes(new HashMap<>(file.getAttributes()));
                        }
                        updateParams.getAttributes().put(VARIANT_FILE_METADATA, fileMetadataMap);
                    } catch (IOException e) {
                        file.getAttributes().put(VARIANT_FILE_METADATA, fileMetadata);
                        logger.warn("Could not parse Avro content into Map");
                    }
                }
                break;
            }
            default:
                break;
        }

        if (bioformat == File.Bioformat.ALIGNMENT || bioformat == File.Bioformat.VARIANT) {
            Map<String, String> sampleMap = file.getInternal() != null ? file.getInternal().getSampleMap() : null;
            List<String> sampleList = getFileSamples(bioformat, updateParams.getAttributes(), sampleMap);
            updateParams.setSampleIds(sampleList);
        }

        IOManager ioManager;
        try {
            ioManager = catalogManager.getIoManagerFactory().get(file.getUri().getScheme());
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(file.getUri(), e);
        }
        final long fileSize = ioManager.getFileSize(file.getUri());
        if (fileSize != file.getSize()) {
            updateParams.setSize(fileSize);
        }

        return updateParams;
    }

    /**
     * Get samples from file header.
     *
     * @param bioformat             File bioformat.
     * @param attributes            File attributes.
     * @param sampleMap             Map of id found in VCF or BAM files to sample ids.
     * @throws CatalogException     CatalogException.
     * @return                      List of samples in the given file
     */
    public List<String> getFileSamples(File.Bioformat bioformat, Map<String, Object> attributes, Map<String, String> sampleMap)
            throws CatalogException {
        if (attributes == null) {
            return new LinkedList<>();
        }

        //Read samples from file
        List<String> sortedSampleNames = null;
        switch (bioformat) {
            case VARIANT: {
                if (attributes.containsKey(VARIANT_FILE_METADATA)) {
                    Object variantSourceObj = attributes.get(VARIANT_FILE_METADATA);

                    if (variantSourceObj instanceof VariantFileMetadata) {
                        sortedSampleNames = ((VariantFileMetadata) variantSourceObj).getSampleIds();
                    } else if (variantSourceObj instanceof Map) {
                        sortedSampleNames = new ObjectMap((Map) variantSourceObj).getAsStringList("sampleIds");
                    } else {
                        logger.warn("Unexpected object type of variantSource ({}) in file attributes. Expected {} or {}",
                                variantSourceObj.getClass(), VariantFileMetadata.class, Map.class);
                    }
                }
                break;
            }
            case ALIGNMENT: {
                if (attributes.containsKey("alignmentHeader")) {
                    Object alignmentHeaderObj = attributes.get("alignmentHeader");

                    if (alignmentHeaderObj instanceof Map) {
                        sortedSampleNames = getSampleFromAlignmentHeader((Map) alignmentHeaderObj);
                    } else {
                        logger.warn("Unexpected object type of AlignmentHeader ({}) in file attributes. Expected {}",
                                alignmentHeaderObj.getClass(), Map.class);
                    }
                }
                break;
            }
            default:
                return new LinkedList<>();
        }

        if (sortedSampleNames == null || sortedSampleNames.isEmpty()) {
            return new LinkedList<>();
        }

        if (sampleMap != null && !sampleMap.isEmpty()) {
            List<String> tmpSampleNames = new ArrayList<>(sortedSampleNames.size());
            for (String sampleName : sortedSampleNames) {
                if (!sampleMap.containsKey(sampleName)) {
                    throw new CatalogException("Missing sample map for id " + sampleName);
                } else {
                    tmpSampleNames.add(sampleMap.get(sampleName));
                }
            }
            sortedSampleNames = tmpSampleNames;
        }

        return sortedSampleNames;
    }

    private List<String> getSampleFromAlignmentHeader(Map alignmentHeaderObj) {
        List<String> sampleNames;
        sampleNames = new LinkedList<>(new ObjectMap(alignmentHeaderObj).getList("readGroups")
                .stream()
                .map((rg) -> ((Map) rg).get("sample").toString())
                .filter((s) -> s != null)
                .collect(Collectors.toSet()));
        return sampleNames;
    }

    public static VariantFileMetadata readVariantFileMetadata(File file) throws IOException {
        File.Format format = file.getFormat();
        if (format == File.Format.VCF || format == File.Format.GVCF || format == File.Format.BCF) {
            VariantFileMetadata metadata = new VariantFileMetadata(String.valueOf(file.getUid()), file.getName());
            metadata.setId(String.valueOf(file.getUid()));
            return VariantMetadataUtils.readVariantFileMetadata(Paths.get(file.getUri().getPath()), metadata);
        } else {
            return null;
        }
    }

    public static VariantFileMetadata readVariantFileMetadata(File file, URI fileUri)
            throws IOException {

        File.Format format = file.getFormat();
        File.Format detectFormat = FileUtils.detectFormat(fileUri);
        if (format == File.Format.VCF
                || format == File.Format.GVCF
                || format == File.Format.BCF
                || detectFormat == File.Format.VCF
                || detectFormat == File.Format.GVCF
                || detectFormat == File.Format.BCF) {
            VariantFileMetadata metadata = new VariantFileMetadata(String.valueOf(file.getUid()), file.getName());
            metadata.setId(String.valueOf(file.getUid()));
            return VariantMetadataUtils.readVariantFileMetadata(Paths.get(fileUri.getPath()), metadata);
        } else {
            return null;
        }
    }

    public static SAMFileHeader readAlignmentHeader(File file, URI fileUri) throws CatalogIOException {
        try {
            if (file.getFormat() == File.Format.SAM || file.getFormat() == File.Format.BAM
                    || FileUtils.detectFormat(fileUri) == File.Format.SAM || FileUtils.detectFormat(fileUri) == File.Format.BAM) {
                BamManager bamManager = new BamManager(Paths.get(fileUri));
                return bamManager.getHeader();
            } else if (file.getFormat() == File.Format.CRAM || FileUtils.detectFormat(fileUri) == File.Format.CRAM) {
                Path reference = null;
                for (FileRelatedFile relatedFile : file.getRelatedFiles()) {
                    if (relatedFile.getRelation() == FileRelatedFile.Relation.REFERENCE_GENOME) {
                        reference = Paths.get(relatedFile.getFile().getUri());
                        break;
                    }
                }
                BamManager bamManager = new BamManager(Paths.get(fileUri), reference);
                return bamManager.getHeader();
            }
            return null;
        } catch (IOException e) {
            throw new CatalogIOException(e.getMessage(), e);
        }
    }



    public static FileMetadataReader get(CatalogManager catalogManager) {
        return new FileMetadataReader(catalogManager);
    }
}
