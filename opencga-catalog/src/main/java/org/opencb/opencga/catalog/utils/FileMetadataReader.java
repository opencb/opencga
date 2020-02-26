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

package org.opencb.opencga.catalog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.file.File;
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

    public static final String VARIANT_FILE_STATS = "variantFileStats";
    @Deprecated
    public static final String VARIANT_STATS = "variantStats";

    @Deprecated
    public static final String VARIANT_SOURCE = "variantSource";
    public static final String VARIANT_FILE_METADATA = "variantFileMetadata";
    public static final String VARIANT_FILE_METADATA_VARIABLE_SET = "opencga_variant_file_metadata";
    private static final QueryOptions STUDY_QUERY_OPTIONS =
            new QueryOptions("include", Arrays.asList("projects.studies.id", "projects.studies.name", "projects.studies.alias"));
    private final CatalogManager catalogManager;
    protected static Logger logger = LoggerFactory.getLogger(FileMetadataReader.class);
    public static final String CREATE_MISSING_SAMPLES = "createMissingSamples";
    private final FileUtils catalogFileUtils;


    public FileMetadataReader(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        catalogFileUtils = new FileUtils(catalogManager);
    }

    public void addMetadataInformation(String studyId, File file) throws CatalogException {
        final FileUpdateParams updateParams = extractMetadataInformation(studyId, file);
        file.setBioformat(updateParams.getBioformat() != null ? updateParams.getBioformat() : file.getBioformat());
        file.setFormat(updateParams.getFormat() != null ? updateParams.getFormat() : file.getFormat());
        file.setAttributes(updateParams.getAttributes() != null ? updateParams.getAttributes() : file.getAttributes());
        file.setSamples(updateParams.getSamples() != null
                ? updateParams.getSamples().stream().map(id -> new Sample().setId(id)).collect(Collectors.toList())
                : file.getSamples());
        file.setSize(updateParams.getSize() != null ? updateParams.getSize() : file.getSize());
    }

    public File updateMetadataInformation(String studyId, File file, String token) throws CatalogException {
        try {
            FileUpdateParams updateParams = extractMetadataInformation(studyId, file);
            ObjectMap updateMap = updateParams.getUpdateMap();

            if (!updateMap.isEmpty()) {
                if (updateParams.getSamples() != null) {
                    // TODO: Check and create missing samples
                }

                catalogManager.getFileManager().update(studyId, file.getUuid(), updateParams, QueryOptions.empty(), token);
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
                AlignmentHeader alignmentHeader = readAlignmentHeader(studyId, file, file.getUri());
                if (alignmentHeader != null) {
                    updateParams.setAttributes(file.getAttributes());
                    updateParams.getAttributes().put("alignmentHeader", alignmentHeader);
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
                        updateParams.setAttributes(file.getAttributes());
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
            updateParams.setSamples(sampleList);
        }

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(file.getUri().getScheme());
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

                    if (alignmentHeaderObj instanceof AlignmentHeader) {
                        sortedSampleNames = getSampleFromAlignmentHeader(((AlignmentHeader) alignmentHeaderObj));
                    } else if (alignmentHeaderObj instanceof Map) {
                        sortedSampleNames = getSampleFromAlignmentHeader((Map) alignmentHeaderObj);
                    } else {
                        logger.warn("Unexpected object type of AlignmentHeader ({}) in file attributes. Expected {} or {}",
                                alignmentHeaderObj.getClass(), AlignmentHeader.class, Map.class);
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
                .map((rg) -> ((Map) ((Map) rg).get("attributes")).get("SM").toString())
                .filter((s) -> s != null)
                .collect(Collectors.toSet()));
        return sampleNames;
    }

    private List<String> getSampleFromAlignmentHeader(AlignmentHeader alignmentHeader) {
        List<String> sampleNames;
        Set<String> sampleSet = alignmentHeader.getReadGroups().stream()
                .map((rg) -> rg.getAttributes().get("SM"))
                .filter((s) -> s != null)
                .collect(Collectors.toSet());
        sampleNames = new LinkedList<>(sampleSet);
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

    public static AlignmentHeader readAlignmentHeader(String studyId, File file, URI fileUri) {
        try {
            if (file.getFormat() == File.Format.SAM || file.getFormat() == File.Format.BAM
                    || FileUtils.detectFormat(fileUri) == File.Format.SAM || FileUtils.detectFormat(fileUri) == File.Format.BAM) {
                BamManager bamManager = new BamManager(Paths.get(fileUri));
                return bamManager.getHeader(studyId);
            } else if (file.getFormat() == File.Format.CRAM || FileUtils.detectFormat(fileUri) == File.Format.CRAM) {
                Path reference = null;
                for (File.RelatedFile relatedFile : file.getRelatedFiles()) {
                    if (relatedFile.getRelation() == File.RelatedFile.Relation.REFERENCE_GENOME) {
                        reference = Paths.get(relatedFile.getFile().getUri());
                        break;
                    }
                }
                BamManager bamManager = new BamManager(Paths.get(fileUri), reference);
                return bamManager.getHeader(studyId);
            }
        } catch (IOException e) {
            logger.warn("{}", e.getMessage(), e);
        }
        return null;
    }



    public static FileMetadataReader get(CatalogManager catalogManager) {
        return new FileMetadataReader(catalogManager);
    }
}
