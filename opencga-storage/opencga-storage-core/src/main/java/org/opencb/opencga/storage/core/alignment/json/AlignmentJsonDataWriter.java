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

package org.opencb.opencga.storage.core.alignment.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentDataWriter;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentHeader;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Date: 8/06/14.
 *
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 *         <p>
 *         AlignmentsFileName     : <name>.alignments.json.gz
 *         HeaderFileName         : <name>.header.json.gz
 */
public class AlignmentJsonDataWriter implements AlignmentDataWriter {

    private final String alignmentFilename;
    private final String headerFilename;
    private final boolean gzip;
    private boolean append = false;

    private AlignmentDataReader reader;

    private final JsonFactory factory;
    private final ObjectMapper jsonObjectMapper;
    private OutputStream alignmentOutputStream;
    private OutputStream headerOutputStream;

    private JsonGenerator alignmentsGenerator;
    private JsonGenerator headerGenerator;
    //private ObjectMap objectMap;
    //private int alignmentsCount = 0;
    //private int alignmentsPerLine = 1;


    public AlignmentJsonDataWriter(AlignmentDataReader reader, String alignmentFilename, String headerFilename) {
        this.alignmentFilename = alignmentFilename;
        this.headerFilename = headerFilename;
        this.reader = reader;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
        this.gzip = alignmentFilename.endsWith(".gz");
    }

    public AlignmentJsonDataWriter(AlignmentDataReader reader, String baseFilename, boolean gzip) {
        this.alignmentFilename = baseFilename + ".alignments" + (gzip ? ".json.gz" : ".json");
        this.headerFilename = baseFilename + ".header" + (gzip ? ".json.gz" : ".json");
        this.reader = reader;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
        this.gzip = gzip;
    }


    @Override
    public boolean writeHeader(AlignmentHeader header) {
        //objectMap.put("header", header);
        try {
            //alignmentOutputStream.write((objectMap.toJson() + "\n").getBytes());
            //objectMap.clear();
            headerGenerator.writeObject(header);
            headerGenerator.flush();
            headerOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
        return true;
    }

    @Override
    public boolean open() {
        try {

            alignmentOutputStream = new FileOutputStream(alignmentFilename, append);
            headerOutputStream = new FileOutputStream(headerFilename, append);

            if (gzip) {
                alignmentOutputStream = new GZIPOutputStream(alignmentOutputStream);
                headerOutputStream = new GZIPOutputStream(headerOutputStream);
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.addMixInAnnotations(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);
        //jsonObjectMapper.addMixInAnnotations(Alignment.class, AlignmentJsonMixin.class); //Not needed
        try {
            alignmentsGenerator = factory.createGenerator(alignmentOutputStream);
            headerGenerator = factory.createGenerator(headerOutputStream);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            close();
            return false;
        }

        return true;
    }

    @Override
    public boolean write(Alignment elem) {

        try {
            alignmentsGenerator.writeObject(elem);
            alignmentsGenerator.writeRaw('\n');
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }


        //objectMap.put(String.valueOf(alignmentsCount), elem);

//        alignmentsCount++;
//        if(alignmentsCount == alignmentsPerLine){
//            alignmentsCount = 0;
//            try {
//                alignmentsGenerator.writeRaw('\n');
//                //alignmentOutputStream.write((objectMap.toJson() + "\n").getBytes());
//                //objectMap.clear();
//            } catch (IOException e) {
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                return false;
//            }
//        }
        return true;
    }

    @Override
    public boolean write(List<Alignment> batch) {
        for (Alignment elem : batch) {
            if (!write(elem)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean post() {
        //TODO: Write Summary
        AlignmentHeader header = reader.getHeader();
        writeHeader(header);

        try {
            alignmentOutputStream.flush();
            alignmentsGenerator.flush();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            alignmentOutputStream.close();
            headerOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
        return true;
    }


    public boolean isAppend() {
        return append;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }


    public boolean isGzip() {
        return gzip;
    }

    public String getHeaderFilename() {
        return headerFilename;
    }

    public String getAlignmentFilename() {
        return alignmentFilename;
    }
}
