package org.opencb.opencga.analysis.files;

import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Do not upload or sync file. Created file status will be <b>STAGE</b>
     *
     * @param studyId     Study on where the file entry is created
     * @param fileUri     File URI to read metadata information.
     * @param path        File path, relative to the study
     * @param description File description (optional)
     * @param parents     Create parent folders or not
     * @param options     Other options
     * @param sessionId   User sessionId
     * @return The created file with status <b>STAGE</b>
     * @throws CatalogException
     */
    public QueryResult<File> create(int studyId, URI fileUri, String path, String description, boolean parents, QueryOptions options, String sessionId) throws CatalogException {

        File.Type type = fileUri.getPath().endsWith("/") ? File.Type.FOLDER : File.Type.FILE;
        File.Format format = FormatDetector.detect(fileUri);
        File.Bioformat bioformat = BioformatDetector.detect(fileUri);

        if (path.endsWith("/")) {
            path += Paths.get(fileUri.getPath()).getFileName().toString();
        }

        QueryResult<File> fileResult = catalogManager.createFile(studyId, type, format, bioformat, path, null, null, description,
                File.Status.STAGE, 0, -1, null, -1, null, null, parents, options, sessionId);

        File modifiedFile = null;

        try {
            modifiedFile = setMetadataInformation(fileResult.first(), fileUri, options, sessionId, false);
        } catch (CatalogException | StorageManagerException e) {
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
     * @throws CatalogException
     * @throws StorageManagerException
     */
    public File setMetadataInformation(final File file, URI fileUri, QueryOptions options, String sessionId, boolean simulate)
            throws CatalogException, StorageManagerException {
        int studyId = catalogManager.getStudyIdByFileId(file.getId());
        if (fileUri == null) {
            fileUri = catalogManager.getFileUri(file);
        }
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ObjectMap modifyParams = new ObjectMap();

//        long start;
        if (file.getType() == File.Type.FOLDER) {
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
                    study = catalogManager.getStudy(studyId, sessionId, new QueryOptions("include", "projects.studies.id,projects.studies.name,projects.studies.alias")).first();
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
                    study = catalogManager.getStudy(studyId, sessionId, new QueryOptions("include", "projects.studies.id,projects.studies.name,projects.studies.alias")).first();
//                    logger.trace("getStudy = " + (System.currentTimeMillis() - start) / 1000.0);

                    VariantSource variantSource = readVariantSource(study, file, fileUri);
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
        /*List<Sample> fileSamples = */getFileSamples(study, file, fileUri, modifyParams, options.getBoolean(CREATE_MISSING_SAMPLES, true), simulate, options, sessionId);
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
     * @return
     * @throws CatalogException
     * @throws StorageManagerException
     */
    public List<Sample> getFileSamples(Study study, File file, URI fileUri, final ObjectMap fileModifyParams,
                                       boolean createMissingSamples, boolean simulate, QueryOptions options, String sessionId)
            throws CatalogException, StorageManagerException {
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
                            logger.warn("Unexpected object type of variantSource ({}) in file attributes. Expected {} or {}", variantSourceObj.getClass(), VariantSource.class, Map.class);
                        }
                    }

                    if (sortedSampleNames == null) {
                        VariantSource variantSource = readVariantSource(study, file, fileUri);
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
                            logger.warn("Unexpected object type of AlignmentHeader ({}) in file attributes. Expected {} or {}", alignmentHeaderObj.getClass(), AlignmentHeader.class, Map.class);
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
            sampleQueryOptions.add("name", sortedSampleNames);
            sampleList = catalogManager.getAllSamples(study.getId(), sampleQueryOptions, sessionId).getResult();

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
                                sampleList.add(catalogManager.createSample(study.getId(), sampleName, file.getName(), null, null, null, sessionId).first());
                            } catch (CatalogException e) {
                                QueryOptions queryOptions = new QueryOptions("name", sampleName);
                                queryOptions.add("include", includeSampleNameId);
                                if (catalogManager.getAllSamples(study.getId(), queryOptions, sessionId).getResult().isEmpty()) {
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
            QueryOptions queryOptions = new QueryOptions(options);
            queryOptions.add("id", file.getSampleIds());
            sampleList = catalogManager.getAllSamples(study.getId(), queryOptions, sessionId).getResult();
        }

        List<Integer> sampleIdsList = sampleList.stream().map(Sample::getId).collect(Collectors.toList());
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
            throws StorageManagerException {
        if (file.getFormat() == File.Format.VCF || FormatDetector.detect(fileUri) == File.Format.VCF) {
            //TODO: Fix aggregate and studyType
            VariantSource source = new VariantSource(file.getName(), Integer.toString(file.getId()), Integer.toString(study.getId()), study.getName());
            return VariantStorageManager.readVariantSource(Paths.get(fileUri.getPath()), source);
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
            reader.open();
            reader.pre();
            reader.post();
            reader.close();
    //        reader.getSamHeader().get
            return reader.getHeader();
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
     * @throws CatalogException
     */
    public void updateVariantFileStats(Job job, String sessionId) throws CatalogException {
        int studyId = catalogManager.getStudyIdByJobId(job.getId());
        QueryOptions queryOptions = new QueryOptions()
                .append(CatalogFileDBAdaptor.FileFilterOption.id.toString(), job.getInput())
                .append(CatalogFileDBAdaptor.FileFilterOption.bioformat.toString(), File.Bioformat.VARIANT);
        QueryResult<File> fileQueryResult = catalogManager.getAllFiles(studyId, queryOptions, sessionId);
        if (fileQueryResult.getResult().isEmpty()) {
            return;
        }
        File inputFile = fileQueryResult.first();
        if (inputFile.getBioformat().equals(File.Bioformat.VARIANT)) {
            queryOptions = new QueryOptions()
                    .append(CatalogFileDBAdaptor.FileFilterOption.id.toString(), job.getOutput())
                    .append(CatalogFileDBAdaptor.FileFilterOption.name.toString(), "~" + inputFile.getName() + ".variants");
            fileQueryResult = catalogManager.getAllFiles(studyId, queryOptions, sessionId);
            if (fileQueryResult.getResult().isEmpty()) {
                return;
            }

            File variantsFile = fileQueryResult.first();
            URI fileUri = catalogManager.getFileUri(variantsFile);
            try {
                VariantSource variantSource = VariantStorageManager.readVariantSource(Paths.get(fileUri.getPath()), null);
                VariantGlobalStats stats = variantSource.getStats();
                catalogManager.modifyFile(inputFile.getId(), new ObjectMap("stats", new ObjectMap(VARIANT_STATS, stats)), sessionId);
            } catch (StorageManagerException e) {
                throw new CatalogException(e);
            }
        }
    }



    public static FileMetadataReader get(CatalogManager catalogManager) {
        return new FileMetadataReader(catalogManager);
    }
}
