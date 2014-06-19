package org.opencb.opencga.storage.alignment.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.alignment.io.AlignmentDataReader;
import org.opencb.biodata.formats.alignment.io.AlignmentDataWriter;
import org.opencb.biodata.formats.alignment.sam.io.AlignmentSamDataReader;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.commons.containers.map.ObjectMap;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Created with IntelliJ IDEA.
 * User: jacobo
 * Date: 8/06/14
 * Time: 10:17
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentJsonDataWriter implements AlignmentDataWriter<Alignment, AlignmentHeader> {

    private int alignmentsPerLine = 10;
    private String filename;
    private Path outdir;
    private boolean append = false;

    private AlignmentDataReader<Alignment> reader;

    private final JsonFactory factory;
    private final ObjectMapper jsonObjectMapper;
    private OutputStream alignmentOutputStream;
    private OutputStream headerOutputStream;

    protected JsonGenerator alignmentsGenerator;
    protected JsonGenerator headerGenerator;
   // private ObjectMap objectMap;
    private int alignmentsCount = 0;
    private boolean gzip = true;


    public AlignmentJsonDataWriter(AlignmentDataReader<Alignment> reader, String filename) {
        this(reader, filename, null);
    }

    public AlignmentJsonDataWriter(AlignmentDataReader<Alignment> reader, String filename, Path outdir) {
        this.filename = filename;
        this.outdir = (outdir != null) ? outdir : Paths.get("").toAbsolutePath();
        this.reader = reader;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
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

            String extension = gzip? ".json.gz" : ".json";

            alignmentOutputStream = new FileOutputStream(
                            Paths.get(outdir.toString(), filename).toAbsolutePath().toString() + ".alignments" + extension , append);
            headerOutputStream    = new FileOutputStream(
                            Paths.get(outdir.toString(), filename).toAbsolutePath().toString() + ".header" + extension , append);


            if(gzip){
                alignmentOutputStream   = new GZIPOutputStream(alignmentOutputStream);
                headerOutputStream      = new GZIPOutputStream(headerOutputStream);
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
        try {
            alignmentsGenerator = factory.createGenerator(alignmentOutputStream);
            headerGenerator = factory.createGenerator(headerOutputStream);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            close();
            return false;
        }

        AlignmentHeader header = reader.getHeader();
        return writeHeader(header);
    }

    @Override
    public boolean write(Alignment elem) {

        try {
            alignmentsGenerator.writeObject(elem);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }


        //objectMap.put(String.valueOf(alignmentsCount), elem);

        alignmentsCount++;

        if(alignmentsCount == alignmentsPerLine){
            alignmentsCount = 0;
            try {

                alignmentsGenerator.writeRaw('\n');
                //alignmentOutputStream.write((objectMap.toJson() + "\n").getBytes());
                //objectMap.clear();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean write(List<Alignment> batch) {
        for (Alignment elem : batch) {
            if(!write(elem)){
                return false;
            }
        }
        return true;
    }

    @Override
     public boolean post() {
        //TODO: Write Summary
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
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
        return true;
    }






    public int getAlignmentsPerLine() {
        return alignmentsPerLine;
    }

    public void setAlignmentsPerLine(int alignmentsPerLine) {
        this.alignmentsPerLine = alignmentsPerLine;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
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

    public void setGzip(boolean gzip) {
        this.gzip = gzip;
    }
}
