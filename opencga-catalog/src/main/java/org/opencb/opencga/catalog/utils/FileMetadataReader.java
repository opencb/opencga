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

import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
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

    /**
     * Creates a file entry in catalog reading metadata information from the fileUri.
     * Do not upload or sync file. Created file status will be {@link File.FileStatus#STAGE}
     *
     * @param studyId     Study on where the file entry is created
     * @param fileUri     File URI to read metadata information.
     * @param path        File path, relative to the study
     * @param description File description (optional)
     * @param parents     Create parent folders or not
     * @param options     Other options
     * @param sessionId   User sessionId
     * @return The created file with status {@link File.FileStatus#STAGE}
     * @throws CatalogException  if a Catalog error occurs
     */
    public DataResult<File> create(String studyId, URI fileUri, String path, String description, boolean parents,
                                    QueryOptions options, String sessionId) throws CatalogException {

        File.Type type = fileUri.getPath().endsWith("/") ? File.Type.DIRECTORY : File.Type.FILE;
        File.Format format = FileUtils.detectFormat(fileUri);
        File.Bioformat bioformat = FileUtils.detectBioformat(fileUri);

        if (path.endsWith("/")) {
            path += Paths.get(fileUri.getPath()).getFileName().toString();
        }

        DataResult<File> fileResult = catalogManager.getFileManager().create(studyId, type, format, bioformat, path,
                description, new File.FileStatus(File.FileStatus.STAGE), (long) 0, null, (long) -1, null, null, parents,
                null, options, sessionId);

        File modifiedFile = null;

        try {
            modifiedFile = setMetadataInformation(fileResult.first(), fileUri, null, options, sessionId, false);
        } catch (CatalogException e) {
            logger.error("Fail at getting the metadata information", e);
        }
        fileResult.setResults(Collections.singletonList(modifiedFile));

        return fileResult;
    }

    public void addMetadataInformation(Study study, File file, String sessionId) throws CatalogException {
        ParamUtils.checkObj(file.getUri(), "uri");

        if (file.getType() == File.Type.DIRECTORY) {
            return;
        }

        File.Format format = FileUtils.detectFormat(file.getUri());
        File.Bioformat bioformat = FileUtils.detectBioformat(file.getUri());

        if (format != File.Format.UNKNOWN && !format.equals(file.getFormat())) {
            file.setFormat(format);
        }
        if (bioformat != File.Bioformat.NONE && !bioformat.equals(file.getBioformat())) {
            file.setBioformat(bioformat);
        }

        switch (bioformat) {
            case ALIGNMENT: {
                AlignmentHeader alignmentHeader = readAlignmentHeader(study, file, file.getUri());
                if (alignmentHeader != null) {
                    file.getAttributes().put("alignmentHeader", alignmentHeader);
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
                        file.getAttributes().put(VARIANT_FILE_METADATA, fileMetadataMap);
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

        List<String> sampleList = getFileSamples(file);
        file.setSamples(sampleList.stream().map(s -> new Sample().setId(s)).collect(Collectors.toList()));

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(file.getUri().getScheme());
        file.setSize(ioManager.getFileSize(file.getUri()));
    }

    /**
     * Reads the file and modifies the Catalog file entry with metadata information. The metadata information read is:
     *      Bioformat
     *      Format
     *      FileHeader (for known bioformats)
     *      SampleIds
     *      Disk usage (size)
     *      Checksum (if calculateChecksum == true)
     *
     * @param file          File from which read metadata
     * @param fileUri       File location. If null, ask to Catalog.
     * @param sampleIdMap   Map of sample ids (as read from the BAM or VCF file) to the actual sample id it should point to.
     * @param options       Other options
     * @param sessionId     User sessionId
     * @param simulate      Simulate the metadata modifications.
     * @return              If there are no modifications, return the same input file. Else, return the updated file
     * @throws CatalogException if a Catalog error occurs
     */
    public File setMetadataInformation(final File file, URI fileUri, Map<String, String> sampleIdMap, QueryOptions options,
                                       String sessionId, boolean simulate)
            throws CatalogException {
        Study study = catalogManager.getFileManager().getStudy(file, sessionId);
        if (fileUri == null) {
            fileUri = catalogManager.getFileManager().getUri(file);
        }
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ObjectMap modifyParams = new ObjectMap();

//        long start;
        if (file.getType() == File.Type.DIRECTORY) {
            return file;
        }

        //Get metadata information

//        start = System.currentTimeMillis();
        File.Format format = FileUtils.detectFormat(fileUri);
//        logger.trace("FormatDetector = " + (System.currentTimeMillis() - start) / 1000.0);
//        start = System.currentTimeMillis();
        File.Bioformat bioformat = FileUtils.detectBioformat(fileUri);
//        logger.trace("BioformatDetector = " + (System.currentTimeMillis() - start) / 1000.0);

        if (format != File.Format.UNKNOWN && !format.equals(file.getFormat())) {
            modifyParams.put(FileDBAdaptor.QueryParams.FORMAT.key(), format);
            file.setFormat(format);
        }
        if (bioformat != File.Bioformat.NONE && !bioformat.equals(file.getBioformat())) {
            modifyParams.put(FileDBAdaptor.QueryParams.BIOFORMAT.key(), bioformat);
            file.setBioformat(bioformat);
        }

//        start = System.currentTimeMillis();
        boolean exists = catalogManager.getCatalogIOManagerFactory().get(fileUri).exists(fileUri);
//        logger.trace("Exists = " + (System.currentTimeMillis() - start) / 1000.0);

        if (exists) {
            switch (bioformat) {
                case ALIGNMENT: {
//                    start = System.currentTimeMillis();
//                    logger.trace("getStudy = " + (System.currentTimeMillis() - start) / 1000.0);

                    AlignmentHeader alignmentHeader = readAlignmentHeader(study, file, fileUri);
                    if (alignmentHeader != null) {
                        HashMap<String, Object> attributes = new HashMap<>();
                        attributes.put("alignmentHeader", alignmentHeader);
                        modifyParams.put(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
                    }
                    break;
                }
                case VARIANT: {
//                    start = System.currentTimeMillis();
//                    logger.trace("getStudy = " + (System.currentTimeMillis() - start) / 1000.0);

                    VariantFileMetadata fileMetadata;
                    try {
                        fileMetadata = readVariantFileMetadata(file, fileUri);
                    } catch (IOException e) {
                        throw new CatalogIOException("Unable to read VariantSource", e);
                    }
                    if (fileMetadata != null) {
                        HashMap<String, Object> attributes = new HashMap<>();
                        attributes.put(VARIANT_FILE_METADATA, fileMetadata);
                        modifyParams.put(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
                    }
                    break;
                }
                default:
                    break;
            }
        }
//        start = System.currentTimeMillis();
        /*List<Sample> fileSamples = */
        getFileSamples(study, file, fileUri, modifyParams, options.getBoolean(CREATE_MISSING_SAMPLES, true), simulate, sampleIdMap, options,
                sessionId);
//        logger.trace("FileSamples = " + (System.currentTimeMillis() - start) / 1000.0);

//        start = System.currentTimeMillis();
        modifyParams.putAll(catalogFileUtils.getModifiedFileAttributes(file, fileUri, false));
//        logger.trace("FileAttributes = " + (System.currentTimeMillis() - start) / 1000.0);

        if (!modifyParams.isEmpty()) {
            catalogManager.getFileManager().update(study.getFqn(), file.getPath(), modifyParams, new QueryOptions(), sessionId);
            return catalogManager.getFileManager().get(study.getFqn(), file.getPath(), options, sessionId).first();
        }

        return file;
    }

    /**
     * Get samples from file header.
     *
     * @param file                  File from which read samples.
     * @return                      List of samples in the given file
     */
    public List<String> getFileSamples(File file) {
        //Read samples from file
        List<String> sortedSampleNames = null;
        switch (file.getBioformat()) {
            case VARIANT: {
                if (file.getAttributes().containsKey(VARIANT_FILE_METADATA)) {
                    Object variantSourceObj = file.getAttributes().get(VARIANT_FILE_METADATA);

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
                if (file.getAttributes().containsKey("alignmentHeader")) {
                    Object alignmentHeaderObj = file.getAttributes().get("alignmentHeader");

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

        return sortedSampleNames;
    }

    /**
     * Get samples from file header.
     *
     * @param study                 Study where the file is.
     * @param file                  File from which read samples.
     * @param fileUri               File location. If null, ask to Catalog.
     * @param fileModifyParams      ModifyParams to add sampleIds and other related information (like header).
     * @param createMissingSamples  Create samples from the file that where missing.
     * @param simulate              Simulate the creation of samples.
     * @param sampleIdMap           Map of sample ids (as read from the BAM or VCF file) to the actual sample id it should point to.
     * @param options               Options
     * @param sessionId             User sessionId
     * @return                      List of samples in the given file
     * @throws CatalogException if a Catalog error occurs
     */
    public List<Sample> getFileSamples(Study study, File file, URI fileUri, final ObjectMap fileModifyParams, boolean createMissingSamples,
                                       boolean simulate, Map<String, String> sampleIdMap, QueryOptions options, String sessionId)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        List<Sample> sampleList;

        Map<String, Object> attributes;
        if (!fileModifyParams.containsKey(FileDBAdaptor.QueryParams.ATTRIBUTES.key())) {
            attributes = new HashMap<>();
        } else {
            attributes = fileModifyParams.getMap(FileDBAdaptor.QueryParams.ATTRIBUTES.key());
        }

        List<String> includeSampleNameId = Arrays.asList(SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.ID.key());
        if (file.getSamples() == null || file.getSamples().isEmpty()) {
            //Read samples from file
            List<String> sortedSampleNames = null;
            switch (fileModifyParams.containsKey(FileDBAdaptor.QueryParams.BIOFORMAT.key())
                    ? (File.Bioformat) fileModifyParams.get(FileDBAdaptor.QueryParams.BIOFORMAT.key())
                    : file.getBioformat()) {
                case VARIANT: {
                    Object variantSourceObj = null;
                    if (file.getAttributes().containsKey(VARIANT_FILE_METADATA)) {
                        variantSourceObj = file.getAttributes().get(VARIANT_FILE_METADATA);
                    } else if (attributes.containsKey(VARIANT_FILE_METADATA)) {
                        variantSourceObj = fileModifyParams.getMap(FileDBAdaptor.QueryParams.ATTRIBUTES.key()).get(VARIANT_FILE_METADATA);
                    }
                    if (variantSourceObj != null) {
                        if (variantSourceObj instanceof VariantFileMetadata) {
                            sortedSampleNames = ((VariantFileMetadata) variantSourceObj).getSampleIds();
                        } else if (variantSourceObj instanceof Map) {
                            sortedSampleNames = new ObjectMap((Map) variantSourceObj).getAsStringList("sampleIds");
                        } else {
                            logger.warn("Unexpected object type of variantSource ({}) in file attributes. Expected {} or {}",
                                    variantSourceObj.getClass(), VariantFileMetadata.class, Map.class);
                        }
                    }

                    if (sortedSampleNames == null) {
                        VariantFileMetadata metadata = null;
                        try {
                            metadata = readVariantFileMetadata(file, fileUri);
                        } catch (IOException e) {
                            throw new CatalogIOException("Unable to read VariantSource", e);
                        }
                        if (metadata != null) {
                            attributes.put(VARIANT_FILE_METADATA, metadata);
                            sortedSampleNames = metadata.getSampleIds();
                        } else {
                            sortedSampleNames = new LinkedList<>();
                        }
                    }
                    break;
                }
                case ALIGNMENT: {
                    Object alignmentHeaderObj = null;
                    if (file.getAttributes().containsKey("alignmentHeader")) {
                        alignmentHeaderObj = file.getAttributes().get("alignmentHeader");
                    } else if (attributes.containsKey("alignmentHeader")) {
                        alignmentHeaderObj = fileModifyParams.getMap("attributes").get("alignmentHeader");
                    }
                    if (alignmentHeaderObj != null) {
                        if (alignmentHeaderObj instanceof AlignmentHeader) {
                            sortedSampleNames = getSampleFromAlignmentHeader(((AlignmentHeader) alignmentHeaderObj));
                        } else if (alignmentHeaderObj instanceof Map) {
                            sortedSampleNames = getSampleFromAlignmentHeader((Map) alignmentHeaderObj);
                        } else {
                            logger.warn("Unexpected object type of AlignmentHeader ({}) in file attributes. Expected {} or {}",
                                    alignmentHeaderObj.getClass(), AlignmentHeader.class, Map.class);
                        }
                    }
                    if (sortedSampleNames == null) {
                        AlignmentHeader alignmentHeader = readAlignmentHeader(study, file, fileUri);
                        if (alignmentHeader != null) {
                            attributes.put("alignmentHeader", alignmentHeader);
                            sortedSampleNames = getSampleFromAlignmentHeader(alignmentHeader);
                        } else {
                            sortedSampleNames = new LinkedList<>();
                        }
                    }
                    break;
                }
                default:
                    return new LinkedList<>();
//                    throw new CatalogException("Unknown to get samples names from bioformat " + file.getBioformat());
            }

            if (sortedSampleNames.isEmpty()) {
                return new LinkedList<>();
            }

            //Find matching samples in catalog with the sampleName from the header.
            QueryOptions sampleQueryOptions = new QueryOptions(QueryOptions.INCLUDE, includeSampleNameId);
            Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.ID.key(), sortedSampleNames);
            sampleList = catalogManager.getSampleManager().search(study.getFqn(), sampleQuery, sampleQueryOptions, sessionId).getResults();

            //check if all file samples exists in Catalog
            if (sampleList.size() != sortedSampleNames.size()) {   //Size does not match. Find the missing samples.
                //Use a LinkedHashSet to keep the order
                Set<String> set = new LinkedHashSet<>(sortedSampleNames);
                for (Sample sample : sampleList) {
                    set.remove(sample.getId());
                }
                logger.warn("Some samples from file \"{}\" were not registered in Catalog. Registering new samples: {}", file.getName(),
                        set);
                if (createMissingSamples) {
                    for (String sampleName : set) {
                        String sampleId = sampleName;
                        if (sampleIdMap != null && !sampleIdMap.isEmpty()) {
                            if (!sampleIdMap.containsKey(sampleName)) {
                                throw new CatalogException("Sample '" + sampleName + "' not found in sample map");
                            }
                            sampleId = sampleIdMap.get(sampleId);
                        }

                        if (simulate) {
                            sampleList.add(new Sample(sampleId, file.getName(), null, null, 1));
                        } else {
                            try {
                                sampleList.add(catalogManager.getSampleManager().create(study.getFqn(), new Sample().setId(sampleId)
                                        .setSource(file.getName()), null, sessionId).first());
                            } catch (CatalogException e) {
                                Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), sampleId);
                                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, includeSampleNameId);
                                if (catalogManager.getSampleManager().search(study.getFqn(), query, queryOptions, sessionId).getResults()
                                        .isEmpty()) {
                                    throw e; //Throw exception if sample does not exist.
                                } else {
                                    logger.debug("Do not create the sample \"" + sampleId + "\". It has magically appeared");
                                }
                            }
                        }
                    }
                } else {
                    throw new CatalogException("Can not find samples " + set + " in catalog");
                }
            }

            //Samples may not be sorted.
            //Sort samples as they appear in the original file.
            Map<String, Sample> sampleMap = sampleList.stream().collect(Collectors.toMap(Sample::getId, Function.identity()));
            sampleList = new ArrayList<>(sampleList.size());
            for (String sampleName : sortedSampleNames) {
                if (sampleIdMap != null && sampleIdMap.containsKey(sampleName)) {
                    sampleList.add(sampleMap.get(sampleIdMap.get(sampleName)));
                } else {
                    sampleList.add(sampleMap.get(sampleName));
                }
            }

        } else {
            //Get samples from file.sampleIds
            Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), file.getSamples().stream().map(Sample::getId)
                    .collect(Collectors.toList()));
            sampleList = catalogManager.getSampleManager().search(study.getFqn(), query, new QueryOptions(), sessionId).getResults();
        }

        List<String> sampleIdsList = sampleList.stream().map(Sample::getId).collect(Collectors.toList());
        fileModifyParams.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleIdsList);
        if (!attributes.isEmpty()) {
            fileModifyParams.put(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
        }

        return sampleList;
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

    public static AlignmentHeader readAlignmentHeader(Study study, File file, URI fileUri) {
        try {
            if (file.getFormat() == File.Format.SAM || file.getFormat() == File.Format.BAM
                    || FileUtils.detectFormat(fileUri) == File.Format.SAM || FileUtils.detectFormat(fileUri) == File.Format.BAM) {
                BamManager bamManager = new BamManager(Paths.get(fileUri));
                return bamManager.getHeader(study.getAlias());
            } else if (file.getFormat() == File.Format.CRAM || FileUtils.detectFormat(fileUri) == File.Format.CRAM) {
                Path reference = null;
                for (File.RelatedFile relatedFile : file.getRelatedFiles()) {
                    if (relatedFile.getRelation() == File.RelatedFile.Relation.REFERENCE_GENOME) {
                        reference = Paths.get(relatedFile.getFile().getUri());
                        break;
                    }
                }
                BamManager bamManager = new BamManager(Paths.get(fileUri), reference);
                return bamManager.getHeader(study.getAlias());
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
