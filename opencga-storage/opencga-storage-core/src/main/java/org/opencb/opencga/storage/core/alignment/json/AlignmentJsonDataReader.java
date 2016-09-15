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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentHeader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Date: 18/06/14.
 *
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 *         <p>
 *         AlignmentsFileName     : <name>.alignments.json.gz
 *         HeaderFileName         : <name>.header.json.gz
 */
public class AlignmentJsonDataReader implements AlignmentDataReader {

    private final String alignmentFilename;
    private final String headerFilename;
    private final boolean gzip;
    private final JsonFactory factory;
    private final ObjectMapper jsonObjectMapper;
    private JsonParser alignmentsParser;
    private JsonParser headerParser;

    private InputStream alignmentsStream;
    private InputStream headerStream;

    private AlignmentHeader alignmentHeader;


    public AlignmentJsonDataReader(String baseFilename, boolean gzip) {
        this(baseFilename + (gzip ? ".alignments.json.gz" : ".alignments.json"), baseFilename
                + (gzip ? ".header.json.gz" : ".header" + ".json"));
    }

    public AlignmentJsonDataReader(String alignmentFilename, String headerFilename) {
        this.alignmentFilename = alignmentFilename;
        this.headerFilename = headerFilename;
        this.gzip = alignmentFilename.endsWith(".gz");
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
    }

    @Override
    public AlignmentHeader getHeader() {
        return alignmentHeader;
    }

    @Override
    public boolean open() {

        try {

            alignmentsStream = new FileInputStream(alignmentFilename);
            headerStream = new FileInputStream(headerFilename);

            if (gzip) {
                alignmentsStream = new GZIPInputStream(alignmentsStream);
                headerStream = new GZIPInputStream(headerStream);
            }

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
            alignmentsParser = factory.createParser(alignmentsStream);
            headerParser = factory.createParser(headerStream);

            alignmentHeader = headerParser.readValueAs(AlignmentHeader.class);
        } catch (IOException e) {
            e.printStackTrace();
            close();
            return false;
        }

        return true;
    }

    @Override
    public List<Alignment> read() {
        Alignment elem = readElem();
        return elem != null ? Arrays.asList(elem) : null;
    }

    public Alignment readElem() {
        try {
            if (alignmentsParser.nextToken() != null) {
                Alignment alignment = alignmentsParser.readValueAs(Alignment.class);
                return alignment;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Alignment> read(int batchSize) {
        //List<Alignment> listRecords = new ArrayList<>(batchSize);
        List<Alignment> listRecords = new LinkedList<>();
        Alignment elem;
        for (int i = 0; i < batchSize; i++) {
            elem = readElem();
            if (elem == null) {
                break;
            }
            listRecords.add(elem);
        }
        return listRecords;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public boolean close() {
        try {
            alignmentsParser.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return true;
    }
}
