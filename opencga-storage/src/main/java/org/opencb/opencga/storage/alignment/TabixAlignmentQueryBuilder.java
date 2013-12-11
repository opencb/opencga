package org.opencb.opencga.storage.alignment;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.samtools.*;
import org.opencb.cellbase.core.common.Region;
import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.ObjectMap;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.CellbaseCredentials;
import org.opencb.opencga.lib.auth.SqliteCredentials;
import org.opencb.opencga.lib.auth.TabixCredentials;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.lib.common.XObject;
import org.opencb.opencga.storage.indices.SqliteManager;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class TabixAlignmentQueryBuilder implements AlignmentQueryBuilder {

    private TabixCredentials tabixCredentials;
    private CellbaseCredentials cellbaseCredentials;
    
    private SqliteCredentials sqliteCredentials;
    private SqliteManager sqliteManager;
    
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(TabixAlignmentQueryBuilder.class);

    public TabixAlignmentQueryBuilder(SqliteCredentials sqliteCredentials, 
                                         TabixCredentials tabixCredentials, 
                                         CellbaseCredentials cellbaseCredentials) {
        this.sqliteCredentials = sqliteCredentials;
        this.tabixCredentials = tabixCredentials;
        this.cellbaseCredentials = cellbaseCredentials;
        this.sqliteManager = new SqliteManager();
    }
    
    @Override
    public QueryResult<List<Alignment>> getAllAlignmentsByRegion(Region region, QueryOptions options) {
        QueryResult<List<Alignment>> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        long startTime = System.currentTimeMillis();
        try {
            List<SAMRecord> records = getSamRecordsByRegion(region);
            // TODO Start and end, long or int?
            List<Alignment> alignments = getAlignmentsFromSamRecords(region, records, options);
            queryResult.setResult(alignments);
            queryResult.setNumResults(alignments.size());
        } catch (AlignmentIndexNotExistsException | IOException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(TabixAlignmentQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }
        
        queryResult.setTime(System.currentTimeMillis() - startTime);
        return queryResult;
    }

    @Override
    public QueryResult<List<Alignment>> getAllAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public QueryResult<Map<String, short[]>> getCoverageByRegion(Region region, QueryOptions options) {
        QueryResult<Map<String, short[]>> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        long startTime = System.currentTimeMillis();
        try {
            List<SAMRecord> records = getSamRecordsByRegion(region);
            Map<String, short[]> coverage = getCoverageFromSamRecords(region, records, options);
            queryResult.setResult(coverage);
            queryResult.setNumResults(coverage.size());
        } catch (AlignmentIndexNotExistsException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(TabixAlignmentQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }
        
        queryResult.setTime(System.currentTimeMillis() - startTime);
        return queryResult;  
    }
    
    @Override
    public QueryResult<List<ObjectMap>> getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        QueryResult<List<ObjectMap>> queryResult = new QueryResult<>(String.format("%s:%d-%d", 
                region.getChromosome(), region.getStart(), region.getEnd())); // TODO Fill metadata
        List<ObjectMap> data = new ArrayList<>();
        
        long startTime = System.currentTimeMillis();
        
        Path metaDir = getMetaDir(sqliteCredentials.getPath());
        String fileName = sqliteCredentials.getPath().getFileName().toString();

        try {
            long startDbTime = System.currentTimeMillis(); 
            sqliteManager.connect(metaDir.resolve(Paths.get(fileName)), true);
            System.out.println("SQLite path: " + metaDir.resolve(Paths.get(fileName)).toString());
            String queryString = "SELECT * FROM chunk WHERE chromosome='" + region.getChromosome() + 
                    "' AND start <= " + region.getEnd() + " AND end >= " + region.getStart();
            List<XObject> queryResults = sqliteManager.query(queryString);
            sqliteManager.disconnect(true);
            queryResult.setDbTime(System.currentTimeMillis() - startDbTime);
            
            int resultSize = queryResults.size();

            if (resultSize > histogramMax) { // Need to group results to fit maximum size of the histogram
                int sumChunkSize = resultSize / histogramMax;
                int i = 0, j = 0;
                int featuresCount = 0;
                ObjectMap item = null;
                
                for (XObject result : queryResults) {
                    featuresCount += result.getInt("features_count");
                    if (i == 0) {
                        item = new ObjectMap("chromosome", result.getString("chromosome"));
                        item.put("chunk_id", result.getInt("chunk_id"));
                        item.put("start", result.getInt("start"));
                    } else if (i == sumChunkSize - 1 || j == resultSize - 1) {
                        if (histogramLogarithm) {
                            item.put("features_count", (featuresCount > 0) ? Math.log(featuresCount) : 0);
                        } else {
                            item.put("features_count", featuresCount);
                        }
                        item.put("end", result.getInt("end"));
                        data.add(item);
                        i = -1;
                        featuresCount = 0;
                    }
                    j++;
                    i++;
                }
            } else {
                for (XObject result : queryResults) {
                    ObjectMap item = new ObjectMap("chromosome", result.getString("chromosome"));
                    item.put("chunk_id", result.getInt("chunk_id"));
                    item.put("start", result.getInt("start"));
                    if (histogramLogarithm) {
                        int features_count = result.getInt("features_count");
                        result.put("features_count", (features_count > 0) ? Math.log(features_count) : 0);
                    } else {
                        item.put("features_count", result.getInt("features_count"));
                    }
                    item.put("end", result.getInt("end"));
                    data.add(item);
                }
            }
        } catch (ClassNotFoundException | SQLException ex ) {
            Logger.getLogger(TabixAlignmentQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }
        
        queryResult.setResult(data);
        queryResult.setNumResults(data.size());
        queryResult.setTime(System.currentTimeMillis() - startTime);
        
        return queryResult;
    }
    
    
    /* ******************************************
     *              Auxiliary queries           *
     * ******************************************/
    
    private List<SAMRecord> getSamRecordsByRegion(Region region) 
            throws ClassNotFoundException, SQLException, AlignmentIndexNotExistsException {
        List<SAMRecord> records = new ArrayList<>();
        
        Path filePath = sqliteCredentials.getPath();
        Path metaDir = getMetaDir(filePath);
        String fileName = filePath.getFileName().toString();

        long startDbTime = System.currentTimeMillis(); 
        sqliteManager.connect(metaDir.resolve(Paths.get(fileName)), true);
        System.out.println("SQLite path: " + metaDir.resolve(Paths.get(fileName)).toString());
        String queryString = "SELECT id, start FROM record_query_fields WHERE chromosome='" + 
                region.getChromosome() + "' AND start <= " + region.getEnd() + " AND end >= " + region.getStart();
        List<XObject> queryResults = sqliteManager.query(queryString);
        sqliteManager.disconnect(true);

        System.out.println("Query time " + (System.currentTimeMillis() - startDbTime) + "ms");

        Map<String, XObject> queryResultsMap = new HashMap<>();
        for (XObject r : queryResults) {
            queryResultsMap.put(r.getString("id") + r.getString("start"), r);
        }
        
        // Query using Picard
        File inputBamFile = new File(filePath.toString());
        File inputBamIndexFile = checkBamIndex(filePath);
        if (inputBamIndexFile == null) {
            logger.warn("Index for file " + filePath + " does not exist");
            throw new AlignmentIndexNotExistsException("Index for file " + filePath + " does not exist");
        }
        
        SAMFileReader inputSam = new SAMFileReader(inputBamFile, inputBamIndexFile);
        inputSam.setValidationStringency(SAMFileReader.ValidationStringency.valueOf("LENIENT"));
//        System.out.println("hasIndex " + inputSam.hasIndex());
        SAMRecordIterator recordsRegion = inputSam.query(region.getChromosome(), (int) region.getStart(), (int) region.getEnd(), false);

        // Filter .db and Picard lists
        long t1 = System.currentTimeMillis();
//        System.out.println(queryResults.size() + " ");

        int resultsPending = queryResults.size();

//        System.out.println("queryResultsLength " + resultsPending);

        while (recordsRegion.hasNext() && resultsPending > 0) {
            SAMRecord record = recordsRegion.next();
            if (queryResultsMap.get(record.getReadName() + record.getAlignmentStart()) != null) {
                records.add(record);
                resultsPending--;
            }
        }
//        System.out.println(records.size() + " ");
        System.out.println("Filter time " + (System.currentTimeMillis() - t1) + "ms");
        
        return records;
    }
    
    private List<Alignment> getAlignmentsFromSamRecords(Region region, List<SAMRecord> records, QueryOptions params) throws IOException {
        List<Alignment> alignments = new ArrayList<>();
        
        if (params.get("view_as_pairs") != null) {
            // If must be shown as pairs, create new comparator by read name
            if (((Boolean) params.get("view_as_pairs")).booleanValue()) {
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
        }
        
        // Get genome sequence
        String referenceSequence = getSequence(region, params);
        
        // Create an Alignment per SAMRecord object
        for (SAMRecord record : records) {
            if (!record.getReadUnmappedFlag()) {
                Map<String, String> attributes = new HashMap<>();
                for (SAMRecord.SAMTagAndValue attr : record.getAttributes()) {
                    attributes.put(attr.tag, attr.value.toString().replace("\\", "\\\\").replace("\"", "\\\""));
                }

                String referenceSubstring = referenceSequence.substring(
                        (500 + record.getUnclippedStart() - ((int)region.getStart())), 
                        (500 + record.getUnclippedEnd() - ((int)region.getStart()) + 1));
                
                // Build alignment object, including differences calculation
                Alignment alignment = new Alignment(record, attributes, referenceSubstring);
                alignments.add(alignment);
            }
        }

        return alignments;
    }
    
    private Map<String, short[]> getCoverageFromSamRecords(Region region, List<SAMRecord> records, QueryOptions params) {
        Map<String, short[]> coverage = new HashMap<>();
        int start = region.getStart();
        int end = region.getEnd();
        
        if (params.get("view_as_pairs") != null) {
            // If must be shown as pairs, create new comparator by read name
            if (((Boolean) params.get("view_as_pairs")).booleanValue()) {
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
        }

        // Arrays containing global and per-nucleotide coverage information
        short[] coverageArray = new short[end - start + 1];
        short[] aBaseArray = new short[end - start + 1];
        short[] cBaseArray = new short[end - start + 1];
        short[] gBaseArray = new short[end - start + 1];
        short[] tBaseArray = new short[end - start + 1];

        
        for (SAMRecord record : records) {
            if (!record.getReadUnmappedFlag()) {
                String readStr = record.getReadString();
                int refgenomeOffset = 0, readOffset = 0;
                
                for (int i = 0; i < record.getCigar().getCigarElements().size(); i++) {
                    if (record.getCigar().getCigarElement(i).getOperator() == CigarOperator.M) {
                        for (int j = record.getAlignmentStart() - start + refgenomeOffset, cont = 0; cont < record.getCigar().getCigarElement(i).getLength(); j++, cont++) {
                            if (j >= 0 && j < coverageArray.length) {
                                coverageArray[j]++;
                                
                                switch (readStr.charAt(cont + readOffset)) {
                                    case 'A':
                                        aBaseArray[j]++;
                                        break;
                                    case 'C':
                                        cBaseArray[j]++;
                                        break;
                                    case 'G':
                                        gBaseArray[j]++;
                                        break;
                                    case 'T':
                                        tBaseArray[j]++;
                                        break;
                                }
                            }
                        }
                    }
                    if (record.getCigar().getCigarElement(i).getOperator() == CigarOperator.I) {
                        refgenomeOffset++;
                        readOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                    } else if (record.getCigar().getCigarElement(i).getOperator() == CigarOperator.D) {
                        refgenomeOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                        readOffset++;
                    } else if (record.getCigar().getCigarElement(i).getOperator() == CigarOperator.H) {
                        // Ignore hardclipping and do not update offset pointers
                    } else {
                        refgenomeOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                        readOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                    }
//                    if (record.getCigar().getCigarElement(i).getOperator() != CigarOperator.I) {
//                    } else if(){
//                    }
                }
            }
        }

        coverage.put("all", coverageArray);
        coverage.put("a", aBaseArray);
        coverage.put("c", cBaseArray);
        coverage.put("g", gBaseArray);
        coverage.put("t", tBaseArray);
        
        return coverage;
    }

    
    /* ******************************************
     *          Path and index checking         *
     * ******************************************/
    
    private Path getMetaDir(Path file) {
        String inputName = file.getFileName().toString();
        return file.getParent().resolve(".meta_" + inputName);
    }

    private File checkBamIndex(Path inputBamPath) {
        Path metaDir = getMetaDir(inputBamPath);
        String fileName = inputBamPath.getFileName().toString();
        
        //name.bam & name.bam.bai
        Path inputBamIndexFile = metaDir.resolve(Paths.get(fileName + ".bai"));
        logger.info(inputBamIndexFile.toString());
        if (Files.exists(inputBamIndexFile)) {
            return inputBamIndexFile.toFile();
        }
        
        //name.bam & name.bai
        fileName = IOUtils.removeExtension(fileName);
        inputBamIndexFile = metaDir.resolve(Paths.get(fileName + ".bai"));
        logger.info(inputBamIndexFile.toString());
        if (Files.exists(inputBamIndexFile)) {
            return inputBamIndexFile.toFile();
        }
        
        return null;
    }
    
    private String getSequence(Region region, QueryOptions params) throws IOException {
        String cellbaseHost = params.getString("cellbasehost", "http://ws-beta.bioinfo.cipf.es/cellbase/rest/latest");
        String species = params.getString("species", "hsapiens");
        
//        if (species.equals("cclementina")) {
//            cellbasehost = "http://citrusgenn.bioinfo.cipf.es/cellbasecitrus/rest/v3";
//        }

        String urlString = cellbaseHost + "/" + species + "/genomic/region/" + region.getChromosome() + ":"
                + (region.getStart() - 500) + "-" + (region.getEnd() + 500) + "/sequence?of=json";
        System.out.println(urlString);

        URL url = new URL(urlString);
        InputStream is = url.openConnection().getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonParser jp = factory.createParser(br);
        JsonNode o = mapper.readTree(jp);

        ArrayNode response = (ArrayNode) o.get("response");
        String sequence = response.get(0).get("result").get("sequence").asText();
        br.close();
        return sequence;
    }

}
