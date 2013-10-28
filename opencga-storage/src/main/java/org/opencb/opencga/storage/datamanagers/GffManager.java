package org.opencb.opencga.storage.datamanagers;

import org.opencb.opencga.lib.common.XObject;
import org.opencb.opencga.storage.indices.DefaultParser;
import org.opencb.opencga.storage.indices.SqliteManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class GffManager {
    private int CHUNKSIZE = 2000;

    String recordTableName;
    XObject recordColumns;
    String recordIndexName;
    XObject recordIndices;
    DefaultParser recordDefaultParser;

    String chunkTableName;
    XObject chunkColumns;
    String chunkIndexName;
    XObject chunkIndices;

    String statsTableName;
    XObject statsColumns;

    XObject gffColumns;

    public GffManager() {
        //record_query_fields
        recordTableName = "record_query_fields";
        recordColumns = new XObject();
        recordColumns.put("chromosome", "TEXT");
        recordColumns.put("start", "INT");
        recordColumns.put("end", "INT");
        recordColumns.put("offset", "BIGINT");

        recordIndexName = "chromosome_start_end";
        recordIndices = new XObject();
        recordIndices.put("chromosome", 0);
        recordIndices.put("start", 3);
        recordIndices.put("end", 4);
        recordDefaultParser = new DefaultParser(recordIndices);

        //chunk
        chunkTableName = "chunk";
        chunkColumns = new XObject();
        chunkColumns.put("chromosome", "TEXT");
        chunkColumns.put("chunk_id", "TEXT");
        chunkColumns.put("start", "INT");
        chunkColumns.put("end", "INT");
        chunkColumns.put("features_count", "INT");

        chunkIndexName = "chromosome_chunk_id";
        chunkIndices = new XObject();
        chunkIndices.put("chromosome", 0);
        chunkIndices.put("chunk_id", -1);

        //stats
        statsTableName = "global_stats";
        statsColumns = new XObject();
        statsColumns.put("name", "TEXT");
        statsColumns.put("title", "TEXT");
        statsColumns.put("value", "TEXT");


        gffColumns = new XObject();
        gffColumns.put("seqname", 0);
        gffColumns.put("source", 1);
        gffColumns.put("feature", 2);
        gffColumns.put("start", 3);
        gffColumns.put("end", 4);
        gffColumns.put("score", 5);
        gffColumns.put("strand", 6);
        gffColumns.put("frame", 7);
        gffColumns.put("group", 8);
    }

    public void createIndex(Path filePath) throws SQLException, IOException, ClassNotFoundException {

        SqliteManager sqliteManager = new SqliteManager();
        sqliteManager.connect(filePath, false);

        //record_query_fields
        sqliteManager.createTable(recordTableName, recordColumns);

        //chunk
        sqliteManager.createTable(chunkTableName, chunkColumns);

        //stats
        sqliteManager.createTable(statsTableName, statsColumns);


        //chunk visited hash
        HashMap<Integer, XObject> visitedChunks = new HashMap<>();
        HashMap<String, XObject> visitedChromosomes = new HashMap<>();

        //Read file
        BufferedReader br;
        Boolean gzip = false;
        if (gzip) {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(filePath))));
        } else {
            br = Files.newBufferedReader(filePath, Charset.defaultCharset());
        }
        String line = null;
        long offsetPos = 0;
        int numberLines = 0;
        while ((line = br.readLine()) != null) {
            numberLines++;
            XObject offsetXO = recordDefaultParser.parse(line);
            offsetXO.put("offset", offsetPos);
            //offset table
            sqliteManager.insert(offsetXO, recordTableName);
            offsetPos += line.length() + 1;

            //calculate chromosome stats
            XObject chrXo = visitedChromosomes.get(offsetXO.get("chromosome"));
            if (chrXo == null) {
                chrXo = new XObject();
                chrXo.put("start", offsetXO.getInt("start"));
                chrXo.put("end", offsetXO.getInt("end"));
                visitedChromosomes.put(offsetXO.getString("chromosome"), chrXo);
            }
            chrXo.put("start", Math.min(chrXo.getInt("start"), offsetXO.getInt("start")));
            chrXo.put("end", Math.max(chrXo.getInt("end"), offsetXO.getInt("end")));


            //chunk table
            int firstChunkId = getChunkId(offsetXO.getInt("start"));
            int lastChunkId = getChunkId(offsetXO.getInt("end"));
            for (int i = firstChunkId; i <= lastChunkId; i++) {
                if (visitedChunks.get(i) == null) {
                    XObject xoChunk = new XObject();
                    int chunkStart = getChunkStart(i);
                    int chunkEnd = getChunkEnd(i);
                    xoChunk.put("chunk_id", i);
                    xoChunk.put("chromosome", offsetXO.getString("chr"));
                    xoChunk.put("start", chunkStart);
                    xoChunk.put("end", chunkEnd);
                    xoChunk.put("features_count", 0);
                    visitedChunks.put(i, xoChunk);
                }
                XObject xoUpdate = visitedChunks.get(i);
                xoUpdate.put("features_count", xoUpdate.getInt("features_count") + 1);
            }

        }
        br.close();

        //table record_query_fields
        sqliteManager.commit(recordTableName);
        sqliteManager.createIndex(recordTableName, recordIndexName, recordIndices);

        //table chunk
        for (Integer key : visitedChunks.keySet()) {
            sqliteManager.insert(visitedChunks.get(key), chunkTableName);
        }
        sqliteManager.commit(chunkTableName);
        sqliteManager.createIndex(chunkTableName, chunkIndexName, chunkIndices);

        //table stats
        XObject values = new XObject();
        values.put("title", "File number lines");
        values.put("name", "NUM_LINES");
        values.put("value", numberLines);
        sqliteManager.insert(values, statsTableName);

        values = new XObject();
        values.put("title", "Number of chromosomes (or sequences)");
        values.put("name", "NUM_CHR");
        values.put("value", visitedChromosomes.keySet().size());
        sqliteManager.insert(values, statsTableName);

        values = new XObject();
        String chrStr = visitedChromosomes.keySet().toString();
        values.put("title", "Chromosomes (or sequences)");
        values.put("name", "CHR_LIST");
        values.put("value", chrStr.substring(1, chrStr.length() - 1));
        sqliteManager.insert(values, statsTableName);

        for (String key : visitedChromosomes.keySet()) {
            XObject chrXo = visitedChromosomes.get(key);
            String chromosomePrefix = "";

            String chrkey = key;
            if (key.contains("chr")) {
                chromosomePrefix = "chr";
                chrkey = key.replace("chr", "");
            }

            //check chromosome prefix
            values = new XObject();
            values.put("title", "Chromsome " + chrkey + " prefix");
            values.put("name", "CHR_" + chrkey + "_PREFIX");
            values.put("value", chromosomePrefix);
            sqliteManager.insert(values, statsTableName);

            values = new XObject();
            values.put("title", "Chromosome");
            values.put("name", "CHR_" + chrkey + "_NAME");
            values.put("value", key);
            sqliteManager.insert(values, statsTableName);

            values = new XObject();
            values.put("title", "Length");
            values.put("name", "CHR_" + chrkey + "_LENGTH");
            values.put("value", chrXo.getInt("end") - chrXo.getInt("start") + 1);
            sqliteManager.insert(values, statsTableName);
        }

        sqliteManager.commit(statsTableName);

        //disconnect
        sqliteManager.disconnect(true);
    }

    public List<XObject> queryRegion(Path filePath, String chromosome, int start, int end) throws SQLException, IOException, ClassNotFoundException {

        SqliteManager sqliteManager = new SqliteManager();
        sqliteManager.connect(filePath, true);

        String tableName = "global_stats";
        String queryString = "SELECT value FROM " + tableName + " WHERE name='CHR_" + chromosome + "_PREFIX'";
        String chrPrefix = sqliteManager.query(queryString).get(0).getString("value");

        tableName = "record_query_fields";
        queryString = "SELECT offset FROM " + tableName + " WHERE chromosome='" + chrPrefix + chromosome + "' AND start<=" + end + " AND end>=" + start;
        List<XObject> queryResults = sqliteManager.query(queryString);
        //disconnect
        sqliteManager.disconnect(true);

        //access file
        List<XObject> results = new ArrayList<>();
        DefaultParser GFFParser = new DefaultParser(gffColumns);
        RandomAccessFile raf = new RandomAccessFile(filePath.toString(), "r");
        for (XObject queryResult : queryResults) {
            raf.seek(queryResult.getInt("offset"));
            results.add(GFFParser.parse(raf.readLine()));
        }
        return results;
    }

    private int getChunkId(int position) {
        return position / CHUNKSIZE;
    }

    private int getChunkStart(int id) {
        return (id == 0) ? 1 : id * CHUNKSIZE;
    }

    private int getChunkEnd(int id) {
        return (id * CHUNKSIZE) + CHUNKSIZE - 1;
    }
}
