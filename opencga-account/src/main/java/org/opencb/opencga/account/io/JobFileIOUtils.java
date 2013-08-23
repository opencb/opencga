package org.opencb.opencga.account.io;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.opencb.opencga.common.IOUtils;
import org.opencb.opencga.common.ListUtils;
import org.opencb.opencga.common.ArrayUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class JobFileIOUtils implements IOManager {

    private static Logger logger = Logger.getLogger(JobFileIOUtils.class);

    public static String getSenchaTable(Path jobFile, String filename, String start, String limit, String colNames,
                                        String colVisibility, String callback, String sort) throws IOManagementException, IOException {

        int first = Integer.parseInt(start);
        int end = first + Integer.parseInt(limit);
        String[] colnamesArray = colNames.split(",");
        String[] colvisibilityArray = colVisibility.split(",");

        if (!Files.exists(jobFile)) {
            throw new IOManagementException("getFileTableFromJob(): the file '" + jobFile.toAbsolutePath()
                    + "' not exists");
        }

        String name = filename.replace("..", "").replace("/", "");
        List<String> avoidingFiles = getAvoidingFiles();
        if (avoidingFiles.contains(name)) {
            throw new IOManagementException("getFileTableFromJob(): No permission to use the file '"
                    + jobFile.toAbsolutePath() + "'");
        }

        StringBuilder stringBuilder = new StringBuilder();

        int totalCount = -1;
        List<String> headLines;
        try {
            headLines = IOUtils.head(jobFile, 30);
        } catch (IOException e) {
            throw new IOManagementException("getFileTableFromJob(): could not head the file '"
                    + jobFile.toAbsolutePath() + "'");
        }

        Iterator<String> headIterator = headLines.iterator();
        while (headIterator.hasNext()) {
            String line = headIterator.next();
            if (line.startsWith("#NUMBER_FEATURES")) {
                totalCount = Integer.parseInt(line.split("\t")[1]);
                break;
            }
        }

        logger.debug("totalCount ---after read head lines ---------> " + totalCount);

        if (totalCount == -1) {
            logger.debug("totalCount ---need to count all lines and prepend it---------> " + totalCount);

            int numFeatures = 0;
            BufferedReader br = Files.newBufferedReader(jobFile, Charset.defaultCharset());
            String line = null;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("#")) {
                    numFeatures++;
                }
            }
            br.close();
            totalCount = numFeatures;
            String text = "#NUMBER_FEATURES	" + numFeatures;
            IOUtils.prependString(jobFile, text);
        }

        if (!sort.equals("false")) {
            // Para obtener el numero de la columna a partir del
            // nombre que viene del store de Sencha
            Map<String, Integer> indexColumn = new HashMap<String, Integer>();
            for (int i = 0; i < colnamesArray.length; i++) {
                indexColumn.put(colnamesArray[i], i);
            }

            // Parsear y obtener el nombre de la columna que envia
            // Sencha
            // logger.info("PAKO::SORT1: "+ sort);
            Gson gson = new Gson();
            TableSort[] datos = gson.fromJson(sort, TableSort[].class);
            logger.info("PAKO:SORT: " + Arrays.toString(datos));
            int numColumn = indexColumn.get(datos[0].getProperty());
            String direction = datos[0].getDirection();
            logger.info("PAKO:SORT:NUMCOLUMN " + numColumn);

            boolean decreasing = false;
            if (direction.equals("DESC")) {
                decreasing = true;
            }

            List<String> dataFile = IOUtils.grep(jobFile, "^[^#].*");

            double[] numbers = ListUtils.toDoubleArray(IOUtils.column(jobFile, numColumn, "\t", "^[^#].*"));

            int[] orderedRowIndices = ArrayUtils.order(numbers, decreasing);

            String[] fields;
            stringBuilder.append(callback + "({\"total\":\"" + totalCount + "\",\"items\":[");
            for (int j = 0; j < orderedRowIndices.length; j++) {
                if (j >= first && j < end) {
                    fields = dataFile.get(orderedRowIndices[j]).split("\t");
                    stringBuilder.append("{");
                    for (int i = 0; i < fields.length; i++) {
                        if (Integer.parseInt(colvisibilityArray[i].toString()) == 1) {
                            stringBuilder.append("\"" + colnamesArray[i] + "\":\"" + fields[i] + "\",");
                        }
                    }
                    stringBuilder.append("}");
                    stringBuilder.append(",");
                } else {
                    if (j >= end) {
                        break;
                    }
                }
            }
            stringBuilder.append("]});");

        } else {// END SORT

            int numLine = 0;
            String line = null;
            String[] fields;
            BufferedReader br = Files.newBufferedReader(jobFile, Charset.defaultCharset());
            stringBuilder.append(callback + "({\"total\":\"" + totalCount + "\",\"items\":[");
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("#")) {
                    if (numLine >= first && numLine < end) {
                        fields = line.split("\t");
                        stringBuilder.append("{");
                        logger.info("PAKO::length: " + fields.length);
                        for (int i = 0; i < fields.length; i++) {
                            if (Integer.parseInt(colvisibilityArray[i].toString()) == 1) {
                                stringBuilder.append("\"" + colnamesArray[i] + "\":\"" + fields[i] + "\",");
                            }
                        }
                        stringBuilder.append("}");
                        stringBuilder.append(",");
                    } else {
                        if (numLine >= end) {
                            break;
                        }
                    }
                    numLine++;
                }
            }
            br.close();
            stringBuilder.append("]});");
        }
        return stringBuilder.toString();
    }

    private static List<String> getAvoidingFiles() {
        List<String> avoidingFiles = new ArrayList<String>();
        avoidingFiles.add("result.xml");
        avoidingFiles.add("cli.txt");
        avoidingFiles.add("form.txt");
        avoidingFiles.add("input_params.txt");
        avoidingFiles.add("job.log");
        avoidingFiles.add("jobzip.zip");
        return avoidingFiles;
    }

    private class TableSort {
        private String property;
        private String direction;

        public TableSort() {
        }

        public TableSort(String prop, String direct) {
            this.setProperty(prop);
            this.setDirection(direct);
        }

        @Override
        public String toString() {
            return property + "::" + direction;
        }

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }

        public String getDirection() {
            return direction;
        }

        public void setDirection(String direction) {
            this.direction = direction;
        }

    }
}
