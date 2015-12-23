/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.alignment;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import org.opencb.biodata.formats.alignment.AlignmentConverter;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.core.auth.CellbaseCredentials;
import org.opencb.opencga.core.auth.SqliteCredentials;
import org.opencb.opencga.core.auth.TabixCredentials;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.XObject;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.utils.SqliteManager;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
@Deprecated
public class TabixAlignmentDBAdaptor implements AlignmentDBAdaptor {

    private TabixCredentials tabixCredentials;
    private CellbaseCredentials cellbaseCredentials;

    private SqliteCredentials sqliteCredentials;
    private SqliteManager sqliteManager;

    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(TabixAlignmentDBAdaptor.class);

    public TabixAlignmentDBAdaptor(SqliteCredentials sqliteCredentials,
                                   TabixCredentials tabixCredentials,
                                   CellbaseCredentials cellbaseCredentials) {
        this.sqliteCredentials = sqliteCredentials;
        this.tabixCredentials = tabixCredentials;
        this.cellbaseCredentials = cellbaseCredentials;
        this.sqliteManager = new SqliteManager();
    }

    @Override
    public QueryResult<Alignment> getAllAlignmentsByRegion(List<Region> regions, QueryOptions options) {
        Region region = regions.get(0);
        QueryResult<Alignment> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        long startTime = System.currentTimeMillis();
        try {
            List<SAMRecord> records = getSamRecordsByRegion(region);
            // TODO Start and end, long or int?
            List<Alignment> alignments = getAlignmentsFromSamRecords(region, records, options);
            queryResult.setResult(alignments);
            queryResult.setNumResults(alignments.size());
        } catch (AlignmentIndexNotExistsException | IOException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(TabixAlignmentDBAdaptor.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }

        queryResult.setTime((int) (System.currentTimeMillis() - startTime));
        return queryResult;
    }


    @Override
    public QueryResult<Alignment> getAllAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public QueryResult<RegionCoverage> getCoverageByRegion(Region region, QueryOptions options) {
        QueryResult<RegionCoverage> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        long startTime = System.currentTimeMillis();
        try {
            List<SAMRecord> records = getSamRecordsByRegion(region);
            RegionCoverage coverage = getCoverageFromSamRecords(region, records, options);
            queryResult.addResult(coverage);
            queryResult.setNumResults(1);
        } catch (AlignmentIndexNotExistsException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(TabixAlignmentDBAdaptor.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }

        queryResult.setTime((int) (System.currentTimeMillis() - startTime));
        return queryResult;
    }


    @Override
    public QueryResult<ObjectMap> getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        QueryResult<ObjectMap> queryResult = new QueryResult<>(String.format("%s:%d-%d",
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
            queryResult.setDbTime((int) (System.currentTimeMillis() - startDbTime));

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
                        item.put("chunkId", result.getInt("chunk_id"));
                        item.put("start", result.getInt("start"));
                    } else if (i == sumChunkSize - 1 || j == resultSize - 1) {
                        if (histogramLogarithm) {
                            item.put("featuresCount", (featuresCount > 0) ? Math.log(featuresCount) : 0);
                        } else {
                            item.put("featuresCount", featuresCount);
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
                    item.put("chunkId", result.getInt("chunk_id"));
                    item.put("start", result.getInt("start"));
                    if (histogramLogarithm) {
                        int features_count = result.getInt("features_count");
                        result.put("featuresCount", (features_count > 0) ? Math.log(features_count) : 0);
                    } else {
                        item.put("featuresCount", result.getInt("features_count"));
                    }
                    item.put("end", result.getInt("end"));
                    data.add(item);
                }
            }
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(TabixAlignmentDBAdaptor.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }

        queryResult.setResult(data);
        queryResult.setNumResults(data.size());
        queryResult.setTime((int) (System.currentTimeMillis() - startTime));

        return queryResult;
    }

    @Override
    public QueryResult getAllIntervalFrequencies(Region region, QueryOptions options) {
        return null;
    }


    @Override
    public QueryResult getAlignmentRegionInfo(Region region, QueryOptions options) {
        AlignmentRegion alignmentRegion = new AlignmentRegion(region.getChromosome(), region.getStart(), region.getEnd());
        QueryResult<AlignmentRegion> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        long startTime = System.currentTimeMillis();

        try {
            List<SAMRecord> records = getSamRecordsByRegion(region);
            List<Alignment> alignments = getAlignmentsFromSamRecords(region, records, options);
            RegionCoverage coverage = getCoverageFromSamRecords(region, records, options);
            alignmentRegion.setAlignments(alignments);
            alignmentRegion.setCoverage(coverage);
        } catch (AlignmentIndexNotExistsException | IOException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(TabixAlignmentDBAdaptor.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }

        queryResult.setTime((int) (System.currentTimeMillis() - startTime));
        queryResult.addResult(alignmentRegion);
        queryResult.setNumResults(1);
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
        inputSam.setValidationStringency(SAMFileReader.getDefaultValidationStringency().valueOf("LENIENT"));
//        System.out.println("hasIndex " + inputSam.hasIndex());
        SAMRecordIterator recordsRegion = inputSam.query(region.getChromosome(), (int) region.getStart(), (int) region.getEnd(), false);

        // Filter .db and Picard lists
        long t1 = System.currentTimeMillis();
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
            if (record.getReadUnmappedFlag()) {
                continue;
            }

            Map<String, Object> attributes = new HashMap<>();
            for (SAMRecord.SAMTagAndValue attr : record.getAttributes()) {
                attributes.put(attr.tag, attr.value.toString().replace("\\", "\\\\").replace("\"", "\\\""));
            }

            String referenceSubstring = referenceSequence.substring(
                    (500 + record.getUnclippedStart() - ((int) region.getStart())),
                    (500 + record.getUnclippedEnd() - ((int) region.getStart()) + 1));

            // Build alignment object, including differences calculation
            Alignment alignment = AlignmentConverter.buildAlignment(record, attributes, referenceSubstring);
            alignments.add(alignment);
        }

        return alignments;
    }


    private RegionCoverage getCoverageFromSamRecords(Region region, List<SAMRecord> records, QueryOptions params) {
        RegionCoverage coverage = new RegionCoverage();
        int start = (int) region.getStart();
        int end = (int) region.getEnd();

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
        short[] allBasesArray = new short[end - start + 1];
        short[] aBaseArray = new short[end - start + 1];
        short[] cBaseArray = new short[end - start + 1];
        short[] gBaseArray = new short[end - start + 1];
        short[] tBaseArray = new short[end - start + 1];


        for (SAMRecord record : records) {
            if (record.getReadUnmappedFlag()) {
                continue;
            }

            String readStr = record.getReadString();
            int referenceOffset = 0, readOffset = 0;

            for (int i = 0; i < record.getCigar().getCigarElements().size(); i++) {
                CigarElement element = record.getCigar().getCigarElement(i);
                switch (element.getOperator()) {
                    case M:
                        for (int j = record.getAlignmentStart() - start + referenceOffset, cont = 0; cont < element.getLength(); j++,
                                cont++) {
                            if (j >= 0 && j < allBasesArray.length) {
                                allBasesArray[j]++;
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
                        break;
                    case I:
                        referenceOffset++;
                        readOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                        break;
                    case D:
                        referenceOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                        readOffset++;
                        break;
                    case H:
                        // Ignore hardclipping and do not update offset pointers
                        break;
                    default:
                        referenceOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                        readOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                }
            }
        }

        coverage.setA(aBaseArray);
        coverage.setC(cBaseArray);
        coverage.setG(gBaseArray);
        coverage.setT(tBaseArray);
        coverage.setAll(allBasesArray);

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
