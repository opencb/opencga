package org.opencb.opencga.catalog.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileMetadataReader {

    public static final String VARIANT_STATS = "variantStats";
    private static final QueryOptions STUDY_QUERY_OPTIONS =
            new QueryOptions("include", Arrays.asList("projects.studies.id", "projects.studies.name", "projects.studies.alias"));
    private final CatalogManager catalogManager;
    protected static Logger logger = LoggerFactory.getLogger(FileMetadataReader.class);
    public static final String CREATE_MISSING_SAMPLES = "createMissingSamples";
    private final CatalogFileUtils catalogFileUtils;


    public FileMetadataReader(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        catalogFileUtils = new CatalogFileUtils(catalogManager);
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
    public QueryResult<File> create(long studyId, URI fileUri, String path, String description, boolean parents,
                                    QueryOptions options, String sessionId) throws CatalogException {

        File.Type type = fileUri.getPath().endsWith("/") ? File.Type.DIRECTORY : File.Type.FILE;
        File.Format format = FormatDetector.detect(fileUri);
        File.Bioformat bioformat = BioformatDetector.detect(fileUri);

        if (path.endsWith("/")) {
            path += Paths.get(fileUri.getPath()).getFileName().toString();
        }

        QueryResult<File> fileResult = catalogManager.createFile(studyId, type, format, bioformat, path, null, description,
                new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, parents, options, sessionId);

        File modifiedFile = null;

        try {
            modifiedFile = setMetadataInformation(fileResult.first(), fileUri, options, sessionId, false);
        } catch (CatalogException e) {
            logger.error("Fail at getting the metadata information", e);
        }
        fileResult.setResult(Collections.singletonList(modifiedFile));

        return fileResult;
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
     * @param options       Other options
     * @param sessionId     User sessionId
     * @param simulate      Simulate the metadata modifications.
     * @return              If there are no modifications, return the same input file. Else, return the updated file
     * @throws CatalogException if a Catalog error occurs
     */
    public File setMetadataInformation(final File file, URI fileUri, QueryOptions options, String sessionId, boolean simulate)
            throws CatalogException {
        long studyId = catalogManager.getStudyIdByFileId(file.getId());
        if (fileUri == null) {
            fileUri = catalogManager.getFileUri(file);
        }
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ObjectMap modifyParams = new ObjectMap();

//        long start;
        if (file.getType() == File.Type.DIRECTORY) {
            return file;
        }

        //Get metadata information

//        start = System.currentTimeMillis();
        File.Format format = FormatDetector.detect(fileUri);
//        logger.trace("FormatDetector = " + (System.currentTimeMillis() - start) / 1000.0);
//        start = System.currentTimeMillis();
        File.Bioformat bioformat = BioformatDetector.detect(fileUri);
//        logger.trace("BioformatDetector = " + (System.currentTimeMillis() - start) / 1000.0);

        if (format != File.Format.UNKNOWN && !format.equals(file.getFormat())) {
            modifyParams.put("format", format);
            file.setFormat(format);
        }
        if (bioformat != File.Bioformat.NONE && !bioformat.equals(file.getBioformat())) {
            modifyParams.put("bioformat", bioformat);
            file.setBioformat(bioformat);
        }

        Study study = null;

//        start = System.currentTimeMillis();
        boolean exists = catalogManager.getCatalogIOManagerFactory().get(fileUri).exists(fileUri);
//        logger.trace("Exists = " + (System.currentTimeMillis() - start) / 1000.0);

        if (exists) {
            switch (bioformat) {
                case ALIGNMENT: {
//                    start = System.currentTimeMillis();
                    study = catalogManager.getStudy(studyId, STUDY_QUERY_OPTIONS, sessionId).first();
//                    logger.trace("getStudy = " + (System.currentTimeMillis() - start) / 1000.0);

                    AlignmentHeader alignmentHeader = readAlignmentHeader(study, file, fileUri);
                    if (alignmentHeader != null) {
                        HashMap<String, Object> attributes = new HashMap<>();
                        attributes.put("alignmentHeader", alignmentHeader);
                        modifyParams.put("attributes", attributes);
                    }
                    break;
                }
                case VARIANT: {
//                    start = System.currentTimeMillis();
                    study = catalogManager.getStudy(studyId, STUDY_QUERY_OPTIONS, sessionId).first();
//                    logger.trace("getStudy = " + (System.currentTimeMillis() - start) / 1000.0);

                    VariantSource variantSource = null;
                    try {
                        variantSource = readVariantSource(study, file, fileUri);
                    } catch (IOException e) {
                        throw new CatalogIOException("Unable to read VariantSource", e);
                    }
                    if (variantSource != null) {
                        HashMap<String, Object> attributes = new HashMap<>();
                        attributes.put("variantSource", variantSource);
                        modifyParams.put("attributes", attributes);
                    }
                    break;
                }
                default:
                    break;
            }
        }
//        start = System.currentTimeMillis();
        /*List<Sample> fileSamples = */
        getFileSamples(study, file, fileUri, modifyParams, options.getBoolean(CREATE_MISSING_SAMPLES, true), simulate, options, sessionId);
//        logger.trace("FileSamples = " + (System.currentTimeMillis() - start) / 1000.0);

//        start = System.currentTimeMillis();
        modifyParams.putAll(catalogFileUtils.getModifiedFileAttributes(file, fileUri, false));
//        logger.trace("FileAttributes = " + (System.currentTimeMillis() - start) / 1000.0);

        if (!modifyParams.isEmpty()) {
//            start = System.currentTimeMillis();
            catalogManager.modifyFile(file.getId(), modifyParams, sessionId);
//            logger.trace("modifyFile = " + (System.currentTimeMillis() - start) / 1000.0);

            return catalogManager.getFile(file.getId(), options, sessionId).first();
        }

        return file;
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
     * @param options               Options
     * @param sessionId             User sessionId
     * @return                      List of samples in the given file
     * @throws CatalogException if a Catalog error occurs
     */
    public List<Sample> getFileSamples(Study study, File file, URI fileUri, final ObjectMap fileModifyParams,
                                       boolean createMissingSamples, boolean simulate, QueryOptions options, String sessionId)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        List<Sample> sampleList;

        Map<String, Object> attributes;
        if (!fileModifyParams.containsKey("attributes")) {
            attributes = new HashMap<>();
        } else {
            attributes = fileModifyParams.getMap("attributes");
        }

        List<String> includeSampleNameId = Arrays.asList("projects.studies.samples.id", "projects.studies.samples.name");
        if (file.getSampleIds() == null || file.getSampleIds().isEmpty()) {
            //Read samples from file
            List<String> sortedSampleNames = null;
            switch (fileModifyParams.containsKey("bioformat") ? (File.Bioformat) fileModifyParams.get("bioformat") : file.getBioformat()) {
                case VARIANT: {
                    Object variantSourceObj = null;
                    if (file.getAttributes().containsKey("variantSource")) {
                        variantSourceObj = file.getAttributes().get("variantSource");
                    } else if (attributes.containsKey("variantSource")) {
                        variantSourceObj = fileModifyParams.getMap("attributes").get("variantSource");
                    }
                    if (variantSourceObj != null) {
                        if (variantSourceObj instanceof VariantSource) {
                            sortedSampleNames = ((VariantSource) variantSourceObj).getSamples();
                        } else if (variantSourceObj instanceof Map) {
                            sortedSampleNames = new ObjectMap((Map) variantSourceObj).getAsStringList("samples");
                        } else {
                            logger.warn("Unexpected object type of variantSource ({}) in file attributes. Expected {} or {}",
                                    variantSourceObj.getClass(), VariantSource.class, Map.class);
                        }
                    }

                    if (sortedSampleNames == null) {
                        VariantSource variantSource = null;
                        try {
                            variantSource = readVariantSource(study, file, fileUri);
                        } catch (IOException e) {
                            throw new CatalogIOException("Unable to read VariantSource", e);
                        }
                        if (variantSource != null) {
                            attributes.put("variantSource", variantSource);
                            sortedSampleNames = variantSource.getSamples();
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
            QueryOptions sampleQueryOptions = new QueryOptions("include", includeSampleNameId);
            Query sampleQuery = new Query("name", sortedSampleNames);
            sampleList = catalogManager.getAllSamples(study.getId(), sampleQuery, sampleQueryOptions, sessionId).getResult();

            //check if all file samples exists on Catalog
            if (sampleList.size() != sortedSampleNames.size()) {   //Size does not match. Find the missing samples.
                //Use a LinkedHashSet to keep the order
                Set<String> set = new LinkedHashSet<>(sortedSampleNames);
                for (Sample sample : sampleList) {
                    set.remove(sample.getName());
                }
                logger.warn("Missing samples: {}", set);
                if (createMissingSamples) {
                    for (String sampleName : set) {
                        if (simulate) {
                            sampleList.add(new Sample(-1, sampleName, file.getName(), -1, null));
                        } else {
                            try {
                                sampleList.add(catalogManager.createSample(study.getId(), sampleName, file.getName(),
                                        null, null, null, sessionId).first());
                            } catch (CatalogException e) {
                                Query query = new Query("name", sampleName);
                                QueryOptions queryOptions = new QueryOptions("include", includeSampleNameId);
                                if (catalogManager.getAllSamples(study.getId(), query, queryOptions, sessionId).getResult().isEmpty()) {
                                    throw e; //Throw exception if sample does not exist.
                                } else {
                                    logger.debug("Do not create the sample \"" + sampleName + "\". It has magically appeared");
                                }
                            }
                        }
                    }
                } else {
                    throw new CatalogException("Can not find samples " + set + " in catalog"); //FIXME: Create missing samples??
                }
            }

            //Samples may not be sorted.
            //Sort samples as they appear in the original file.
            Map<String, Sample> sampleMap = sampleList.stream().collect(Collectors.toMap(Sample::getName, Function.identity()));
            sampleList = new ArrayList<>(sampleList.size());
            for (String sampleName : sortedSampleNames) {
                sampleList.add(sampleMap.get(sampleName));
            }

        } else {
            //Get samples from file.sampleIds
            Query query = new Query("id", file.getSampleIds());
            sampleList = catalogManager.getAllSamples(study.getId(), query, options, sessionId).getResult();
        }

        List<Long> sampleIdsList = sampleList.stream().map(Sample::getId).collect(Collectors.toList());
        fileModifyParams.put("sampleIds", sampleIdsList);
        if (!attributes.isEmpty()) {
            fileModifyParams.put("attributes", attributes);
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

    public static VariantSource readVariantSource(Study study, File file, URI fileUri)
            throws IOException {
        if (file.getFormat() == File.Format.VCF || FormatDetector.detect(fileUri) == File.Format.VCF) {
            //TODO: Fix aggregate and studyType
            VariantSource source = new VariantSource(file.getName(), Long.toString(file.getId()),
                    Long.toString(study.getId()), study.getName());
            return VariantFileUtils.readVariantSource(Paths.get(fileUri.getPath()), source);
        } else {
            return null;
        }
    }

    public static AlignmentHeader readAlignmentHeader(Study study, File file, URI fileUri) {
        if (file.getFormat() == File.Format.SAM
                || file.getFormat() == File.Format.BAM
                || FormatDetector.detect(fileUri) == File.Format.SAM
                || FormatDetector.detect(fileUri) == File.Format.BAM) {
            AlignmentSamDataReader reader = new AlignmentSamDataReader(Paths.get(fileUri), study.getName());
            try {
                reader.open();
                reader.pre();
                reader.post();
                //        reader.getSamHeader().get
                return reader.getHeader();
            } finally {
                reader.close();
            }
        } else {
            return null;
        }
    }

    /**
     * Updates the file stats from a transformed variant file.
     * Reads the stats generated on the transform step.
     *
     * @param job           Job that executed successfully the transform step
     * @param sessionId     User sessionId
     * @throws CatalogException if a Catalog error occurs
     */
    @Deprecated
    public void updateVariantFileStats(Job job, String sessionId) throws CatalogException {
        long studyId = catalogManager.getStudyIdByJobId(job.getId());
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.ID.key(), job.getInput())
                .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT);
        QueryResult<File> fileQueryResult = catalogManager.getAllFiles(studyId, query, new QueryOptions(), sessionId);
        if (fileQueryResult.getResult().isEmpty()) {
            return;
        }
        File inputFile = fileQueryResult.first();
        if (inputFile.getBioformat().equals(File.Bioformat.VARIANT)) {
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.ID.key(), job.getOutput())
                    .append(FileDBAdaptor.QueryParams.NAME.key(), "~" + inputFile.getName() + ".file");
            fileQueryResult = catalogManager.getAllFiles(studyId, query, new QueryOptions(), sessionId);
            if (fileQueryResult.getResult().isEmpty()) {
                return;
            }

            File variantsFile = fileQueryResult.first();
            URI fileUri = catalogManager.getFileUri(variantsFile);
            try (InputStream is = FileUtils.newInputStream(Paths.get(fileUri.getPath()))) {
                VariantSource variantSource = new ObjectMapper().readValue(is, VariantSource.class);
                VariantGlobalStats stats = variantSource.getStats();
                catalogManager.modifyFile(inputFile.getId(), new ObjectMap("stats", new ObjectMap(VARIANT_STATS, stats)), sessionId);
            } catch (IOException e) {
                throw new CatalogException("Error reading file \"" + fileUri + "\"", e);
            }
        }
    }



    public static FileMetadataReader get(CatalogManager catalogManager) {
        return new FileMetadataReader(catalogManager);
    }
}
