package org.opencb.opencga.storage.mongodb.alignment;

import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import org.opencb.biodata.formats.alignment.AlignmentConverter;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jacobo on 15/08/14.
 */
public class IndexedAlignmentDBAdaptor implements AlignmentQueryBuilder {

    //Query Options
    public static final String QO_BAM_PATH = "bam_path";
    public static final String QO_BAI_PATH = "bai_path";
    public static final String QO_VIEW_AS_PAIRS = "view_as_pairs";
    public static final String QO_PROCESS_DIFFERENCES = "process_differences";




    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(IndexedAlignmentDBAdaptor.class);

    private AlignmentConverter converter;


    public IndexedAlignmentDBAdaptor(SequenceDBAdaptor adaptor) {
        try {
            this.converter = new AlignmentConverter(adaptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public IndexedAlignmentDBAdaptor() {
        this(new CellBaseSequenceDBAdaptor());
    }

    /**
     *
     * @param region Query Region
     * @param options Query Options: Expected bam_path. Optionally bai_path and view_as_pairs
     * @return
     */
    @Override
    public QueryResult getAllAlignmentsByRegion(Region region, QueryOptions options) {


        String bam = options.getString(QO_BAM_PATH, "");
        String bai = options.getString(QO_BAI_PATH, "");
        if(bai.equals("")) {
            bai = getIndexFromBam(bam);
        }
        Path bamFile = Paths.get(bam);
        Path baiFile = Paths.get(bai);

        QueryResult<Alignment> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));

        long startTime = System.currentTimeMillis();


        if(!bam.endsWith("")){
            queryResult.setErrorMsg("Expected parameter \"" + QO_BAM_PATH + "=*.bam\"");
            logger.warn("Expected parameter \"" + QO_BAM_PATH + "=*.bam\"");
        } if(!bam.endsWith(".bam")){
            queryResult.setErrorMsg("Unsupported extension for \"" + QO_BAM_PATH + "=" + bam + "\"");
            logger.warn("Unsupported extension for \"" + QO_BAM_PATH + "=" + bam + "\"");
        } else if(!bamFile.toFile().exists()) {
            queryResult.setErrorMsg("BAM file not found");
            logger.warn("BAM file " + bamFile + " not found");
        } else if(!bai.endsWith(".bai")){
            queryResult.setErrorMsg("Can't find BAM index file. Expected parameter \"" + QO_BAI_PATH + "=*.bai\"");
            logger.warn("Can't find BAM index file. Expected parameter \"" + QO_BAI_PATH + "=*.bai\".");
        } else if (!baiFile.toFile().exists()){
            queryResult.setErrorMsg("BAM index file (.bai) not found");
            logger.warn("BAM index file (.bai) " + baiFile + " for file " + bamFile + " not found");
        } else {
            List<SAMRecord> recordList = getSamRecordsByRegion(bamFile, baiFile, region);
            List<Alignment> alignmentList = getAlignmentsFromSamRecords(recordList, options);
            queryResult.setResult(alignmentList);
            queryResult.setNumResults(alignmentList.size());
        }

        queryResult.setTime(System.currentTimeMillis() - startTime);
        return queryResult;
    }

    @Override
    public QueryResult getAllAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public QueryResult getCoverageByRegion(Region region, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        return null;
    }

    @Override
    public QueryResult getAlignmentRegionInfo(Region region, QueryOptions options) {
        return null;
    }

    /* ******************************************
     *              Auxiliary queries           *
     * ******************************************/
    public List<SAMRecord> getSamRecordsByRegion(Path bamPath, Path baiPath, Region region){
        List<SAMRecord> records = new ArrayList<>();

        SAMFileReader inputSam = new SAMFileReader(bamPath.toFile(), baiPath.toFile());
        inputSam.setValidationStringency(SAMFileReader.ValidationStringency.valueOf("LENIENT"));
        SAMRecordIterator recordsRegion = inputSam.query(region.getChromosome(), (int) region.getStart(), (int) region.getEnd(), false);

        SAMRecord record;
        while (recordsRegion.hasNext()) {
            record = recordsRegion.next();
            records.add(record);
        }

        return records;

    }



    private List<Alignment> getAlignmentsFromSamRecords(List<SAMRecord> records, QueryOptions params) {
        List<Alignment> alignments = new ArrayList<>();

        if (params.getBoolean(QO_VIEW_AS_PAIRS, false)) {
            // If must be shown as pairs, create new comparator by read name
            Collections.sort(records, new Comparator<SAMRecord>() {
                @Override
                public int compare(SAMRecord o1, SAMRecord o2) {
                    if (o1 != null && o1.getReadName() != null && o2 != null) {
                        return o1.getReadName().compareTo(o2.getReadName());
                    }
                    return -1;
                }
            });
        }

        // Create an Alignment per SAMRecord object
        for (SAMRecord record : records) {
            // Build alignment object, including differences calculation
            Alignment alignment = converter.buildAlignment(record, params.getBoolean(QO_PROCESS_DIFFERENCES, true));
            alignments.add(alignment);
        }

        return alignments;
    }


    private String getIndexFromBam(String bam) {
        String bai;
        if(Paths.get((bai = bam+".bai")).toFile().exists()){
            return bai;
        } else if(Paths.get(bai = (IOUtils.removeExtension(bam) + ".bai")).toFile().exists()){
            return bai;
        } else {
            return "";
        }
    }



}
