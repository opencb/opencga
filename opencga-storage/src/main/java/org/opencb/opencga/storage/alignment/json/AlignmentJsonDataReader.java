package org.opencb.opencga.storage.alignment.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentHeader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created with IntelliJ IDEA.
 * User: jacobo
 * Date: 18/06/14
 * Time: 13:03
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentJsonDataReader implements AlignmentDataReader<Alignment>{

    private String alignmentFilename;
    private String globalFilename;

    private Path dir;
    //private Path globalPath;

    protected JsonFactory factory;
    protected ObjectMapper jsonObjectMapper;
    private JsonParser alignmentsParser;
 //   private JsonParser globalParser;

    private InputStream alignmentsStream;
 //   private InputStream globalStream;

    private AlignmentHeader alignmentHeader;

    public AlignmentJsonDataReader(String alignmentFilename) {
        this(alignmentFilename, null);
    }

    public AlignmentJsonDataReader(String alignmentFilename, Path dir) {
        this.alignmentFilename = alignmentFilename;
        this.dir = (dir != null) ? dir : Paths.get("").toAbsolutePath(); ;
    }

    @Override
    public AlignmentHeader getHeader() {
        return alignmentHeader;
    }

    @Override
    public boolean open() {

        try {
            new FileInputStream(dir.toFile());

            alignmentsStream = new FileInputStream(Paths.get(dir.toString(), alignmentFilename).toAbsolutePath().toString());

            if (alignmentFilename.endsWith(".gz")) {
                alignmentsStream = new GZIPInputStream(alignmentsStream);
            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        return true;
    }

    @Override
    public boolean pre() {

        try {
            alignmentsParser = factory.createParser(alignmentsStream);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            close();
            return false;
        }

        return true;
    }

    @Override
    public List<Alignment> read() {
        Alignment elem = readElem();
        return elem != null? Arrays.asList(elem) : null;
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
        for (int i = 0; i < batchSize ; i++) {
            elem = readElem();
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
