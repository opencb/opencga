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
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import htsjdk.samtools.*;
import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.core.SgeManager;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.core.common.XObject;
import org.opencb.opencga.storage.core.utils.SqliteManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

@Deprecated
public class BamManager {

    protected static ObjectMapper jsonObjectMapper;
    protected static ObjectWriter jsonObjectWriter;

    private String species = "";
    private String cellbasehost = "";
    protected static Logger logger = LoggerFactory.getLogger(BamManager.class);
    private static Path indexerManagerScript = Paths.get(Config.getGcsaHome(),
            Config.getAnalysisProperties().getProperty("OPENCGA.ANALYSIS.BINARIES.PATH"), "indexer", "indexerManager.py");

    private Properties analysisProperties = Config.getAnalysisProperties();

    XObject bamDbColumns;

    public BamManager() throws IOException {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectWriter = jsonObjectMapper.writer();

        bamDbColumns = new XObject();
        bamDbColumns.put("chromosome", 0);
        bamDbColumns.put("strand", 1);
        bamDbColumns.put("start", 2);
        bamDbColumns.put("end", 3);
        bamDbColumns.put("flag", 4);
        bamDbColumns.put("mapping_quality", 5);
        bamDbColumns.put("num_errors", 6);
        bamDbColumns.put("num_indels", 7);
        bamDbColumns.put("indels_length", 8);
        bamDbColumns.put("template_length", 9);
        bamDbColumns.put("id", 10);
    }

    private static Path getMetaDir(Path file) {
        String inputName = file.getFileName().toString();
        return file.getParent().resolve(".meta_" + inputName);
    }

    public static String createIndex(Path inputBamPath) throws IOException, InterruptedException {

        Path metaDir = getMetaDir(inputBamPath);

        if (Files.exists(metaDir)) {
            IOUtils.deleteDirectory(metaDir);
        }

        String jobId = StringUtils.randomString(8);
        String commandLine = indexerManagerScript + " -t bam -i " + inputBamPath + " --outdir " + metaDir;
        try {
            SgeManager.queueJob("indexer", jobId, 0, inputBamPath.getParent().toString(), commandLine);
        } catch (Exception e) {
            logger.error(e.toString());
//            throw new AnalysisExecutionException("ERROR: sge execution failed.");
        }
        return "indexer_" + jobId;
    }

    private static File checkBamIndex(Path inputBamPath) {
        Path metaDir = getMetaDir(inputBamPath);
        String fileName = inputBamPath.getFileName().toString();
        //name.bam
        //name.bam.bai
        Path inputBamIndexFile = metaDir.resolve(Paths.get(fileName + ".bai"));
        logger.info(inputBamIndexFile.toString());
        if (Files.exists(inputBamIndexFile)) {
            return inputBamIndexFile.toFile();
        }
        //name.bam
        //name.bai
        fileName = IOUtils.removeExtension(fileName);
        inputBamIndexFile = metaDir.resolve(Paths.get(fileName + ".bai"));
        logger.info(inputBamIndexFile.toString());
        if (Files.exists(inputBamIndexFile)) {
            return inputBamIndexFile.toFile();
        }
        return null;
    }


    public static boolean checkIndex(Path filePath) {
        Path metaDir = getMetaDir(filePath);
        String fileName = filePath.getFileName().toString();
        return checkBamIndex(filePath) != null && Files.exists(metaDir.resolve(Paths.get(fileName + ".db")));
    }

    public String queryRegion(Path filePath, String regionStr, Map<String, List<String>> params) throws SQLException, IOException,
            ClassNotFoundException {

        if (params.get("cellbasehost") != null) {
            cellbasehost = params.get("cellbasehost").get(0);
            if (cellbasehost.equals("")) {
                return "{'error':'cellbase host not valid'}";
            }
        }
        if (params.get("species") != null) {
            species = params.get("species").get(0);
            if (species.equals("")) {
                return "{'error':'species not valid'}";
            }
        }

        Path metaDir = getMetaDir(filePath);
        String fileName = filePath.getFileName().toString();

        Region region = Region.parseRegion(regionStr);
        String chromosome = region.getChromosome();
        int start = region.getStart();
        int end = region.getEnd();

        //Query .db
        SqliteManager sqliteManager = new SqliteManager();
        sqliteManager.connect(metaDir.resolve(Paths.get(fileName)), true);
        System.out.println("SQLite path: " + metaDir.resolve(Paths.get(fileName)).toString());

        Boolean histogram = false;
        if (params.get("histogram") != null) {
            histogram = Boolean.parseBoolean(params.get("histogram").get(0));
        }
        Boolean histogramLogarithm = false;
        if (params.get("histogramLogarithm") != null) {
            histogramLogarithm = Boolean.parseBoolean(params.get("histogramLogarithm").get(0));
        }
        int histogramMax = 500;
        if (params.get("histogramMax") != null) {
            histogramMax = Integer.getInteger(params.get("histogramMax").get(0), 500);
        }

        if (histogram) {
            long tq = System.currentTimeMillis();
            String tableName = "chunk";
            String chrPrefix = "";
            String queryString = "SELECT * FROM " + tableName + " WHERE chromosome='" + chrPrefix + chromosome + "' AND start<=" + end +
                    " AND end>=" + start;
            List<XObject> queryResults = sqliteManager.query(queryString);
            sqliteManager.disconnect(true);
            int queryResultSize = queryResults.size();

            if (queryResultSize > histogramMax) {
                List<XObject> sumList = new ArrayList<>();
                int sumChunkSize = queryResultSize / histogramMax;
                int i = 0, j = 0;
                XObject item = null;
                int features_count = 0;
                for (XObject result : queryResults) {
                    features_count += result.getInt("features_count");
                    if (i == 0) {
                        item = new XObject("chromosome", result.getString("chromosome"));
                        item.put("start", result.getString("start"));
                    } else if (i == sumChunkSize - 1 || j == queryResultSize - 1) {
                        if (histogramLogarithm) {
                            item.put("features_count", (features_count > 0) ? Math.log(features_count) : 0);
                        } else {
                            item.put("features_count", features_count);
                        }
                        item.put("end", result.getString("end"));
                        sumList.add(item);
                        i = -1;
                        features_count = 0;
                    }
                    j++;
                    i++;
                }

                return jsonObjectWriter.writeValueAsString(sumList);
//                gson.toJson(sumList);
            }

            if (histogramLogarithm) {
                for (XObject result : queryResults) {
                    int features_count = result.getInt("features_count");
                    result.put("features_count", (features_count > 0) ? Math.log(features_count) : 0);
                }
            }

            System.out.println("Query time " + (System.currentTimeMillis() - tq) + "ms");
//            return gson.toJson(queryResults);
            return jsonObjectWriter.writeValueAsString(queryResults);
        }

//        String tableName = "global_stats";
//        String queryString = "SELECT defaultValue FROM " + tableName + " WHERE name='CHR_PREFIX'";
//        String chrPrefix = sqliteManager.query(queryString).get(0).getString("defaultValue");

        long tq = System.currentTimeMillis();
        String tableName = "record_query_fields";
        String chrPrefix = "";
        String queryString = "SELECT id, start FROM " + tableName + " WHERE chromosome='" + chrPrefix + chromosome + "' AND start<=" +
                end + " AND end>=" + start;
        List<XObject> queryResults = sqliteManager.query(queryString);
        sqliteManager.disconnect(true);
        System.out.println("Query time " + (System.currentTimeMillis() - tq) + "ms");

        HashMap<String, XObject> queryResultsMap = new HashMap<>();
        for (XObject r : queryResults) {
            queryResultsMap.put(r.getString("id") + r.getString("start"), r);
        }
        int queryResultsLength = queryResults.size();

        System.out.println("queryResultsLength " + queryResultsLength);

        //Query Picard
        File inputBamFile = new File(filePath.toString());
        File inputBamIndexFile = checkBamIndex(filePath);
        if (inputBamIndexFile == null) {
            logger.info("BamManager: " + "creating bam index for: " + filePath);
            return null;
        }
        SAMFileReader inputSam = new SAMFileReader(inputBamFile, inputBamIndexFile);
        inputSam.setValidationStringency(SAMFileReader.getDefaultValidationStringency().valueOf("LENIENT"));
        System.out.println("hasIndex " + inputSam.hasIndex());
        SAMRecordIterator recordsRegion = inputSam.query(chromosome, start, end, false);


        //Filter .db and picard lists
        long t1 = System.currentTimeMillis();
        System.out.println(queryResults.size() + " ");

        List<SAMRecord> records = new ArrayList<>();
        while (recordsRegion.hasNext()) {
            SAMRecord record = recordsRegion.next();
            if (queryResultsMap.get(record.getReadName() + record.getAlignmentStart()) != null) {
                records.add(record);
                queryResultsLength--;
            }
            if (queryResultsLength < 0) {
                break;
            }
        }
        System.out.println(records.size() + " ");
        System.out.println("Filter time " + (System.currentTimeMillis() - t1) + "ms");

        return processRecords(records, params, chromosome, start, end);
    }

    public String processRecords(List<SAMRecord> records, Map<String, List<String>> params, String chr, int start, int end) throws
            IOException {
        XObject res = new XObject();
        List<XObject> reads = new ArrayList<XObject>();
        XObject coverage = new XObject();
        res.put("reads", reads);
        res.put("coverage", coverage);
        res.put("start", start);
        res.put("end", end);

        Boolean viewAsPairs = false;
        if (params.get("view_as_pairs") != null) {
            viewAsPairs = Boolean.parseBoolean(params.get("view_as_pairs").get(0));
        }
        Boolean showSoftclipping = false;
        if (params.get("show_softclipping") != null) {
            showSoftclipping = Boolean.parseBoolean(params.get("show_softclipping").get(0));
        }
        int interval = 200000;
        if (params.get("interval") != null) {
            interval = Integer.parseInt(params.get("interval").get(0));
        }


        /**
         * GET GENOME SEQUENCE
         */
        String forwardSequence = getSequence(chr, start, end);
        String reverseSequence = revcomp(forwardSequence);
        // System.out.println(forwardSequence);
        // System.out.println(reverseSequence);
        /**
         * COVERAGE
         */
        short[] coverageArray = new short[end - start + 1];
        short[] aBaseArray = new short[end - start + 1];
        short[] cBaseArray = new short[end - start + 1];
        short[] gBaseArray = new short[end - start + 1];
        short[] tBaseArray = new short[end - start + 1];

        if (viewAsPairs) {
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

/////////////////////////
        /////////////////////////////
        ////////////////////////////
        XObject attributes;
        String readStr;
        for (SAMRecord record : records) {
//            logger.info(record.getReadName());

            Boolean condition = (!record.getReadUnmappedFlag());
            if (condition) {
                attributes = new XObject();
                for (SAMRecord.SAMTagAndValue attr : record.getAttributes()) {
                    attributes.put(attr.tag, attr.value.toString().replace("\\", "\\\\").replace("\"", "\\\""));
                }

                readStr = record.getReadString();

                /***************************************************************************/
                if (true) {// TEST
                    // if(record.getReadNegativeStrandFlag()
                    // ){
                    // if(record.getReadName().equals("SRR081241.8998181") ||
                    // record.getReadName().equals("SRR081241.645807")
                    // ){

                    // System.out.println
                    // ("#############################################################################################################################################");
                    // System.out.println
                    // ("#############################################################################################################################################");
                    // System.out.println("Unclipped Start:"+(record.getUnclippedStart()-start));
                    // System.out.println("Unclipped End:"+(record.getUnclippedEnd()-start+1));
                    // System.out.println(record.getCigarString()+"   Alig Length:"+(record.getAlignmentEnd()-record.getAlignmentStart()
                    // +1)+"   Unclipped length:"+(record.getUnclippedEnd()-record.getUnclippedStart()+1));

                    String refStr = forwardSequence.substring((500 + record.getUnclippedStart() - start),
                            (500 + record.getUnclippedEnd() - start + 1));

                    // System.out.println("refe:"+refStr+"  refe.length:"+refStr.length());
                    // System.out.println("read:"+readStr+"  readStr.length:"+readStr.length()+"   getReadLength:"+record.getReadLength());
                    StringBuilder diffStr = new StringBuilder();

                    int index = 0;
                    int indexRef = 0;
                    // System.out.println(gson.toJson(record.getCigar().getCigarElements()));

//                    logger.info("checking cigar: " + record.getCigar().toString());
                    for (int i = 0; i < record.getCigar().getCigarElements().size(); i++) {
                        CigarElement cigarEl = record.getCigar().getCigarElement(i);
                        CigarOperator cigarOp = cigarEl.getOperator();
                        int cigarLen = cigarEl.getLength();
//                        logger.info(cigarOp + " found" + " index:" + index + " indexRef:" + indexRef + " cigarLen:" + cigarLen);

                        if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.EQ || cigarOp == CigarOperator.X) {
                            String subref = refStr.substring(indexRef, indexRef + cigarLen);
                            String subread = readStr.substring(index, index + cigarLen);
                            diffStr.append(getDiff(subref, subread));
                            index = index + cigarLen;
                            indexRef = indexRef + cigarLen;
                        }
                        if (cigarOp == CigarOperator.I) {
                            diffStr.append(readStr.substring(index, index + cigarLen).toLowerCase());
                            index = index + cigarLen;
                            // TODO save insertions
                        }
                        if (cigarOp == CigarOperator.D) {
                            for (int bi = 0; bi < cigarLen; bi++) {
                                diffStr.append("d");
                            }
                            indexRef = indexRef + cigarLen;
                        }
                        if (cigarOp == CigarOperator.N) {
                            for (int bi = 0; bi < cigarLen; bi++) {
                                diffStr.append("n");
                            }
                            indexRef = indexRef + cigarLen;
                        }
                        if (cigarOp == CigarOperator.S) {
                            if (showSoftclipping) {
                                String subread = readStr.substring(index, index + cigarLen);
                                diffStr.append(subread);
                                index = index + cigarLen;
                                indexRef = indexRef + cigarLen;
                            } else {
                                for (int bi = 0; bi < cigarLen; bi++) {
                                    diffStr.append(" ");
                                }
                                index = index + cigarLen;
                                indexRef = indexRef + cigarLen;
                            }
                        }
                        if (cigarOp == CigarOperator.H) {
                            for (int bi = 0; bi < cigarLen; bi++) {
                                diffStr.append("h");
                            }
                            indexRef = indexRef + cigarLen;
                        }
                        if (cigarOp == CigarOperator.P) {
                            for (int bi = 0; bi < cigarLen; bi++) {
                                diffStr.append("p");
                            }
                            indexRef = indexRef + cigarLen;
                        }
                        // if(cigarOp == CigarOperator.EQ) {
                        //
                        // }
                        // if(cigarOp == CigarOperator.X) {
                        //
                        // }
                    }
                    // System.out.println("diff:"+diffStr);
                    String empty = diffStr.toString().replace(" ", "");
                    // System.out.println("diff:"+diffStr);
                    /*************************************************************************/

                    XObject read = new XObject();
                    read.put("start", record.getAlignmentStart());
                    read.put("end", record.getAlignmentEnd());
                    read.put("unclippedStart", record.getUnclippedStart());
                    read.put("unclippedEnd", record.getUnclippedEnd());
                    read.put("chromosome", chr);
                    read.put("flags", record.getFlags());

                    read.put("cigar", record.getCigarString());
                    read.put("name", record.getReadName());
                    read.put("blocks", record.getAlignmentBlocks().get(0).getLength());
                    read.put("attributes", attributes);

                    read.put("referenceName", record.getReferenceName());
                    read.put("referenceName", "");
                    // the " char must be scaped for
                    read.put("baseQualityString", record.getBaseQualityString().replace("\\", "\\\\").replace("\"", "\\\""));

//                    reads.put("readGroupId",record.getReadGroup().getId());
//                    reads.put("readGroupPlatform",record.getReadGroup().getPlatform());
//                    reads.put("readGroupLibrary",record.getReadGroup().getLibrary());

                    read.put("header", record.getHeader().toString());
                    read.put("readLength", record.getReadLength());
                    read.put("mappingQuality", record.getMappingQuality());

                    read.put("mateReferenceName", record.getMateReferenceName());
                    read.put("mateAlignmentStart", record.getMateAlignmentStart());
                    read.put("inferredInsertSize", record.getInferredInsertSize());

                    if (!empty.isEmpty()) {
                        read.put("diff", diffStr);
                    }

                    read.put("read", readStr);
                    reads.add(read);

//                    reads.put("",);

                }// IF TEST BY READ NAME


//                logger.info("Creating coverage array");
                // TODO cigar check for correct coverage calculation and
                int refgenomeOffset = 0;
                int readOffset = 0;
                int offset = record.getAlignmentStart() - start;
                for (int i = 0; i < record.getCigar().getCigarElements().size(); i++) {
                    if (record.getCigar().getCigarElement(i).getOperator() == CigarOperator.M) {
//                        logger.info("start: "+start);
//                        logger.info("r a start: "+record.getAlignmentStart());
//                        logger.info("refgenomeOffset: "+refgenomeOffset);
//                        logger.info("r c lenght: "+record.getCigar().getCigarElement(i).getLength());
//                        logger.info(record.getAlignmentStart() - start + refgenomeOffset);
//                        logger.info("readStr: "+readStr.length());
//                        logger.info("readStr: "+readStr.length());


                        for (int j = record.getAlignmentStart() - start + refgenomeOffset, cont = 0; cont < record.getCigar()
                                .getCigarElement(i).getLength(); j++, cont++) {
                            if (j >= 0 && j < coverageArray.length) {
                                coverageArray[j]++;
//   /*check unused*/             readPos = j - offset;
                                // if(record.getAlignmentStart() == 32877696){
                                // System.out.println(i-(record.getAlignmentStart()-start));
                                // System.out.println(record.getAlignmentStart()-start);
                                // }
                                // System.out.print(" - "+(cont+readOffset));
                                // System.out.print("|"+readStr.length());
                                int total = cont + readOffset;
                                // if(total < readStr.length()){
//                                logger.info(readStr.length());
                                switch (readStr.charAt(total)) {
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
                                // }
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
                        //Ignored Hardclipping and do not update offset pointers
                    } else {
                        refgenomeOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                        readOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                    }
//                    if (record.getCigar().getCigarElement(i).getOperator() != CigarOperator.I) {
//                    } else if(){
//                    }
                }
//                logger.info("coverage array created");
            }
//            logger.info(" ");
        }

        coverage.put("all", coverageArray);
        coverage.put("a", aBaseArray);
        coverage.put("c", cBaseArray);
        coverage.put("g", gBaseArray);
        coverage.put("t", tBaseArray);

        return jsonObjectWriter.writeValueAsString(res);
//        return gson.toJson(res);
    }

    @Deprecated
    public String getByRegion(Path fullFilePath, String regionStr, Map<String, List<String>> params) throws IOException {
        long totalTime = System.currentTimeMillis();

        Region region = Region.parseRegion(regionStr);
        String chr = region.getChromosome();
        int start = region.getStart();
        int end = region.getEnd();

        logger.info("chr: " + chr + " start: " + start + " end: " + end);

        if (params.get("cellbasehost") != null) {
            cellbasehost = params.get("cellbasehost").get(0);
            if (cellbasehost.equals("")) {
                return "{'error':'cellbase host not valid'}";
            }
        }
        if (params.get("species") != null) {
            species = params.get("species").get(0);
            if (species.equals("")) {
                return "{'error':'species not valid'}";
            }
        }

        Boolean viewAsPairs = false;
        if (params.get("view_as_pairs") != null) {
            viewAsPairs = Boolean.parseBoolean(params.get("view_as_pairs").get(0));
        }
        Boolean showSoftclipping = false;
        if (params.get("show_softclipping") != null) {
            showSoftclipping = Boolean.parseBoolean(params.get("show_softclipping").get(0));
        }
        Boolean histogram = false;
        if (params.get("histogram") != null) {
            histogram = Boolean.parseBoolean(params.get("histogram").get(0));
        }
        int interval = 200000;
        if (params.get("interval") != null) {
            interval = Integer.parseInt(params.get("interval").get(0));
        }

        File inputBamFile = new File(fullFilePath.toString());
        File inputBamIndexFile = new File(fullFilePath + ".bai");

        if (inputBamIndexFile == null) {
            logger.info("BamManager: " + "creating bam index for: " + fullFilePath);
            // createIndex(inputBamFile, inputBamIndexFile);
            return "{error:'no index found'}";
        }

        long t = System.currentTimeMillis();
        SAMFileReader inputSam = new SAMFileReader(inputBamFile, inputBamIndexFile);
        System.out.println("new SamFileReader in " + (System.currentTimeMillis() - t) + "ms");
        System.out.println("hasIndex " + inputSam.hasIndex());

        t = System.currentTimeMillis();
        SAMRecordIterator recordsFound = inputSam.query(chr, start, end, false);
        System.out.println("query SamFileReader in " + (System.currentTimeMillis() - t) + "ms");

        /**
         * ARRAY LIST
         */
        ArrayList<SAMRecord> records = new ArrayList<SAMRecord>();
        t = System.currentTimeMillis();
        while (recordsFound.hasNext()) {
            SAMRecord record = recordsFound.next();
            records.add(record);
        }
        System.out.println(records.size() + " elements added in: " + (System.currentTimeMillis() - t) + "ms");

        /**
         * Check histogram
         */
        if (histogram) {
            int numIntervals = (region.getEnd() - region.getStart()) / interval + 1;
            System.out.println("numIntervals :" + numIntervals);
            int[] intervalCount = new int[numIntervals];

            System.out.println(region.getChromosome());
            System.out.println(region.getStart());
            System.out.println(region.getEnd());
            for (SAMRecord record : records) {
//				System.out.println("---*-*-*-*-" + numIntervals);
//				System.out.println("---*-*-*-*-" + record.getAlignmentStart());
//				System.out.println("---*-*-*-*-" + interval);
                if (record.getAlignmentStart() >= region.getStart() && record.getAlignmentStart() <= region.getEnd()) {
                    int intervalIndex = (record.getAlignmentStart() - region.getStart()) / interval; // truncate
//					System.out.print(intervalIndex + " ");
                    intervalCount[intervalIndex]++;
                }
            }

            int intervalStart = region.getStart();
            int intervalEnd = intervalStart + interval - 1;
            BasicDBList intervalList = new BasicDBList();
            for (int i = 0; i < numIntervals; i++) {
                BasicDBObject intervalObj = new BasicDBObject();
                intervalObj.put("start", intervalStart);
                intervalObj.put("end", intervalEnd);
                intervalObj.put("interval", i);
                intervalObj.put("defaultValue", intervalCount[i]);
                intervalList.add(intervalObj);
                intervalStart = intervalEnd + 1;
                intervalEnd = intervalStart + interval - 1;
            }

            System.out.println(region.getChromosome());
            System.out.println(region.getStart());
            System.out.println(region.getEnd());
            return intervalList.toString();
        }

        /**
         * GET GENOME SEQUENCE
         */
        t = System.currentTimeMillis();
        String forwardSequence = getSequence(chr, start, end);
        String reverseSequence = revcomp(forwardSequence);
        // System.out.println(forwardSequence);
        // System.out.println(reverseSequence);
        System.out.println("Get genome sequence in " + (System.currentTimeMillis() - t) + "ms");
        /**
         * COVERAGE
         */
        short[] coverageArray = new short[end - start + 1];
        short[] aBaseArray = new short[end - start + 1];
        short[] cBaseArray = new short[end - start + 1];
        short[] gBaseArray = new short[end - start + 1];
        short[] tBaseArray = new short[end - start + 1];

        if (viewAsPairs) {
            t = System.currentTimeMillis();
            Collections.sort(records, new Comparator<SAMRecord>() {
                @Override
                public int compare(SAMRecord o1, SAMRecord o2) {
                    if (o1 != null && o1.getReadName() != null && o2 != null) {
                        return o1.getReadName().compareTo(o2.getReadName());
                    }
                    return -1;
                }
            });
            System.out.println(records.size() + " elements sorted in: " + (System.currentTimeMillis() - t) + "ms");
        }

        t = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"reads\":[");
        StringBuilder attrString;
        String readStr;
        int readPos;
//        logger.info("Processing SAM records");
        for (SAMRecord record : records) {
//            logger.info(record.getReadName());

            Boolean condition = (!record.getReadUnmappedFlag());
            if (condition) {
                attrString = new StringBuilder();
                attrString.append("{");
                for (SAMRecord.SAMTagAndValue attr : record.getAttributes()) {
                    attrString.append("\"" + attr.tag + "\":\""
                            + attr.value.toString().replace("\\", "\\\\").replace("\"", "\\\"") + "\",");
                }
                // Remove last comma
                if (attrString.length() > 1) {
                    attrString.replace(attrString.length() - 1, attrString.length(), "");
                }
                attrString.append("}");

                readStr = record.getReadString();

                /***************************************************************************/
                if (true) {// TEST
                    // if(record.getReadNegativeStrandFlag()
                    // ){
                    // if(record.getReadName().equals("SRR081241.8998181") ||
                    // record.getReadName().equals("SRR081241.645807")
                    // ){

                    // System.out.println
                    // ("#############################################################################################################################################");
                    // System.out.println
                    // ("#############################################################################################################################################");
                    // System.out.println("Unclipped Start:"+(record.getUnclippedStart()-start));
                    // System.out.println("Unclipped End:"+(record.getUnclippedEnd()-start+1));
                    // System.out.println(record.getCigarString()+"   Alig Length:"+(record.getAlignmentEnd()-record.getAlignmentStart()
                    // +1)+"   Unclipped length:"+(record.getUnclippedEnd()-record.getUnclippedStart()+1));

                    String refStr = forwardSequence.substring((500 + record.getUnclippedStart() - start),
                            (500 + record.getUnclippedEnd() - start + 1));

                    // System.out.println("refe:"+refStr+"  refe.length:"+refStr.length());
                    // System.out.println("read:"+readStr+"  readStr.length:"+readStr.length()+"   getReadLength:"+record.getReadLength());
                    StringBuilder diffStr = new StringBuilder();

                    int index = 0;
                    int indexRef = 0;
                    // System.out.println(jsonObjectMapper.toJson(record.getCigar().getCigarElements()));

//                    logger.info("checking cigar: " + record.getCigar().toString());
                    for (int i = 0; i < record.getCigar().getCigarElements().size(); i++) {
                        CigarElement cigarEl = record.getCigar().getCigarElement(i);
                        CigarOperator cigarOp = cigarEl.getOperator();
                        int cigarLen = cigarEl.getLength();
//                        logger.info(cigarOp + " found" + " index:" + index + " indexRef:" + indexRef + " cigarLen:" + cigarLen);

                        if (cigarOp == CigarOperator.M || cigarOp == CigarOperator.EQ || cigarOp == CigarOperator.X) {
                            String subref = refStr.substring(indexRef, indexRef + cigarLen);
                            String subread = readStr.substring(index, index + cigarLen);
                            diffStr.append(getDiff(subref, subread));
                            index = index + cigarLen;
                            indexRef = indexRef + cigarLen;
                        }
                        if (cigarOp == CigarOperator.I) {
                            diffStr.append(readStr.substring(index, index + cigarLen).toLowerCase());
                            index = index + cigarLen;
                            // TODO save insertions
                        }
                        if (cigarOp == CigarOperator.D) {
                            for (int bi = 0; bi < cigarLen; bi++) {
                                diffStr.append("d");
                            }
                            indexRef = indexRef + cigarLen;
                        }
                        if (cigarOp == CigarOperator.N) {
                            for (int bi = 0; bi < cigarLen; bi++) {
                                diffStr.append("n");
                            }
                            indexRef = indexRef + cigarLen;
                        }
                        if (cigarOp == CigarOperator.S) {
                            if (showSoftclipping) {
                                String subread = readStr.substring(index, index + cigarLen);
                                diffStr.append(subread);
                                index = index + cigarLen;
                                indexRef = indexRef + cigarLen;
                            } else {
                                for (int bi = 0; bi < cigarLen; bi++) {
                                    diffStr.append(" ");
                                }
                                index = index + cigarLen;
                                indexRef = indexRef + cigarLen;
                            }
                        }
                        if (cigarOp == CigarOperator.H) {
                            for (int bi = 0; bi < cigarLen; bi++) {
                                diffStr.append("h");
                            }
                            indexRef = indexRef + cigarLen;
                        }
                        if (cigarOp == CigarOperator.P) {
                            for (int bi = 0; bi < cigarLen; bi++) {
                                diffStr.append("p");
                            }
                            indexRef = indexRef + cigarLen;
                        }
                        // if(cigarOp == CigarOperator.EQ) {
                        //
                        // }
                        // if(cigarOp == CigarOperator.X) {
                        //
                        // }
                    }
                    // System.out.println("diff:"+diffStr);
                    String empty = diffStr.toString().replace(" ", "");
                    // System.out.println("diff:"+diffStr);
                    /*************************************************************************/

                    sb.append("{");
                    sb.append("\"start\":" + record.getAlignmentStart() + ",");
                    sb.append("\"end\":" + record.getAlignmentEnd() + ",");
                    sb.append("\"unclippedStart\":" + record.getUnclippedStart() + ",");
                    sb.append("\"unclippedEnd\":" + record.getUnclippedEnd() + ",");
                    sb.append("\"chromosome\":\"" + chr + "\",");
                    sb.append("\"flags\":\"" + record.getFlags() + "\",");// with
                    // flags
                    // the
                    // strand
                    // will
                    // be
                    // calculated
                    sb.append("\"cigar\":\"" + record.getCigarString() + "\",");
                    sb.append("\"name\":\"" + record.getReadName() + "\",");
                    sb.append("\"blocks\":\"" + record.getAlignmentBlocks().get(0).getLength() + "\",");

                    sb.append("\"attributes\":" + attrString.toString() + ",");

                    // sb.append("\"readGroupId\":\""+record.getReadGroup().getId()+"\",");
                    // sb.append("\"readGroupPlatform\":\""+record.getReadGroup().getPlatform()+"\",");
                    // sb.append("\"readGroupLibrary\":\""+record.getReadGroup().getLibrary()+"\",");
                    sb.append("\"referenceName\":\"" + record.getReferenceName() + "\",");
                    sb.append("\"baseQualityString\":\""
                            + record.getBaseQualityString().replace("\\", "\\\\").replace("\"", "\\\"") + "\",");// the
                    // "
                    // char
                    // unables
                    // parse
                    // from
                    // javascript
                    // sb.append("\"baseQualityString\":\""+jsonObjectMapper.toJson(baseQualityArray)+"\",");//
                    // the " char unables parse from javascript
                    sb.append("\"header\":\"" + record.getHeader().toString() + "\",");
                    sb.append("\"readLength\":" + record.getReadLength() + ",");
                    sb.append("\"mappingQuality\":" + record.getMappingQuality() + ",");

                    sb.append("\"mateReferenceName\":\"" + record.getMateReferenceName() + "\",");
                    sb.append("\"mateAlignmentStart\":" + record.getMateAlignmentStart() + ",");
                    sb.append("\"inferredInsertSize\":" + record.getInferredInsertSize() + ",");

                    if (!empty.isEmpty()) {
                        sb.append("\"diff\":\"" + diffStr + "\",");
                    }

                    sb.append("\"read\":\"" + readStr + "\"");
                    sb.append("},");

                }// IF TEST BY READ NAME


//                logger.info("Creating coverage array");
                // TODO cigar check for correct coverage calculation and
                int refgenomeOffset = 0;
                int readOffset = 0;
                int offset = record.getAlignmentStart() - start;
                for (int i = 0; i < record.getCigar().getCigarElements().size(); i++) {
                    if (record.getCigar().getCigarElement(i).getOperator() == CigarOperator.M) {
//                        logger.info("start: "+start);
//                        logger.info("r a start: "+record.getAlignmentStart());
//                        logger.info("refgenomeOffset: "+refgenomeOffset);
//                        logger.info("r c lenght: "+record.getCigar().getCigarElement(i).getLength());
//                        logger.info(record.getAlignmentStart() - start + refgenomeOffset);
//                        logger.info("readStr: "+readStr.length());
//                        logger.info("readStr: "+readStr.length());

                        for (int j = record.getAlignmentStart() - start + refgenomeOffset, cont = 0; cont < record.getCigar()
                                .getCigarElement(i).getLength(); j++, cont++) {
                            if (j >= 0 && j < coverageArray.length) {
                                coverageArray[j]++;
                                readPos = j - offset;
                                // if(record.getAlignmentStart() == 32877696){
                                // System.out.println(i-(record.getAlignmentStart()-start));
                                // System.out.println(record.getAlignmentStart()-start);
                                // }
                                // System.out.print(" - "+(cont+readOffset));
                                // System.out.print("|"+readStr.length());
                                int total = cont + readOffset;
                                // if(total < readStr.length()){
//                                logger.info(readStr.length());
                                switch (readStr.charAt(total)) {
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
                                // }
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
                        //Ignored Hardclipping and do not update offset pointers
                    } else {
                        refgenomeOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                        readOffset += record.getCigar().getCigarElement(i).getLength() - 1;
                    }
//                    if (record.getCigar().getCigarElement(i).getOperator() != CigarOperator.I) {
//                    } else if(){
//                    }
                }
//                logger.info("coverage array created");
            }
//            logger.info(" ");
        }

        // Remove last comma
        int sbLength = sb.length();
        int sbLastPos = sbLength - 1;
        if (sbLength > 1 && sb.charAt(sbLastPos) == ',') {
            sb.replace(sbLastPos, sbLength, "");
        }

        // //FIXME
        // sb.append("]");
        // sb.append(",\"coverage\":"+jsonObjectMapper.toJson(coverageArray));
        // sb.append("}");

        // FIXME
        sb.append("]");
        sb.append(",\"coverage\":{\"all\":" + jsonObjectWriter.writeValueAsString(coverageArray));
        sb.append(",\"a\":" + jsonObjectWriter.writeValueAsString(aBaseArray));
        sb.append(",\"c\":" + jsonObjectWriter.writeValueAsString(cBaseArray));
        sb.append(",\"g\":" + jsonObjectWriter.writeValueAsString(gBaseArray));
        sb.append(",\"t\":" + jsonObjectWriter.writeValueAsString(tBaseArray));
        sb.append("}");
        sb.append("}");

        String json = sb.toString();
        System.out.println("Result String created in " + (System.currentTimeMillis() - t) + "ms");

        // IOUtils.write("/tmp/dqslastgetByRegionCall", json);

        inputSam.close();

        System.out.println("TOTAL " + (System.currentTimeMillis() - totalTime) + "ms");
        return json;
    }

    private String getDiff(String refStr, String readStr) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < refStr.length(); i++) {
            if (refStr.charAt(i) != readStr.charAt(i)) {
                sb.append(readStr.charAt(i));
            } else {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    private String getSequence(final String chr, final int start, final int end) throws IOException {

        if (species.equals("cclementina")) {
            cellbasehost = "http://citrusgenn.bioinfo.cipf.es/cellbasecitrus/rest/v3";
        }

        String urlString = cellbasehost + "/" + species + "/genomic/region/" + chr + ":"
                + (start - 500) + "-" + (end + 500) + "/sequence?of=json";
        System.out.println(urlString);

        URL url = new URL(urlString);
        InputStream is = url.openConnection().getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
//        String line = null;
//        StringBuilder sb = new StringBuilder();
//        while ((line = br.readLine()) != null) {
//            sb.append(line.trim());
//        }


        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonParser jp = factory.createParser(br);
        JsonNode o = mapper.readTree(jp);

        ArrayNode response = (ArrayNode) o.get("response");
        String sequence = response.get(0).get("result").get("sequence").asText();
//        JsonElement json = new JsonParser().parse(sb.toString());
//        String sequence = json.getAsJsonObject().get("response").getAsJsonArray().get(0).getAsJsonObject().get("result")
// .getAsJsonObject().get("sequence").getAsString();
        br.close();
        return sequence;
    }

    private String revcomp(String seq) {
        StringBuilder sb = new StringBuilder(seq.length());
        char c;
        for (int i = seq.length() - 1; i > 0; i--) {
            c = seq.charAt(i);
            switch (c) {
                case 'A':
                    c = 'T';
                    break;
                case 'T':
                    c = 'A';
                    break;
                case 'G':
                    c = 'C';
                    break;
                case 'C':
                    c = 'G';
                    break;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    public String getFileList(final String filePath) {
//        File bamDir = new File(filePath + "/bam");
        Path bamDirPath = Paths.get(filePath + "/bam");
        try {
//            FileUtils.checkDirectory(bamDir);

//            File[] files = FileUtils.listFiles(bamDir, ".+.bam");

            List<Path> files = new ArrayList<>();
            DirectoryStream<Path> stream = Files.newDirectoryStream(bamDirPath);
            for (Path p : stream) {
                files.add(p);
            }

            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (int i = 0; i < files.size(); i++) {
                if (!Files.isDirectory(files.get(i))) {
                    Path bai = Paths.get(files.get(i).toAbsolutePath() + ".bai");
                    if (Files.exists(bai)) {
                        sb.append("\"" + files.get(i).getFileName() + "\",");
                    } else {
                        logger.info(files.get(i).getFileName() + " was not added because " + files.get(i).getFileName()
                                + ".bai was not found.");
                    }
                }
            }
            // Remove last comma
            if (sb.length() > 1) {
                sb.replace(sb.length() - 1, sb.length(), "");
            }
            sb.append("]");

            logger.info(sb.toString());
            return sb.toString();

        } catch (IOException e1) {
            return bamDirPath.toAbsolutePath() + "not exists.";
        }

    }
}
