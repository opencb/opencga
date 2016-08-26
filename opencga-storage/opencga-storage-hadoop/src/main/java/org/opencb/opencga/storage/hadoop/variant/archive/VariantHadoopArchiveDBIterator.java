package org.opencb.opencga.storage.hadoop.variant.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converter.VcfRecordToVariantConverter;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created on 04/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopArchiveDBIterator extends VariantDBIterator implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(VariantHadoopArchiveDBIterator.class);
    private final VcfRecordToVariantConverter converter;
    private long limit;
    private long count = 0;
    private Iterator<VcfSliceProtos.VcfRecord> vcfRecordIterator = Collections.emptyIterator();
    private VcfSliceProtos.VcfSlice vcfSlice;
    private final Iterator<Result> iterator;
    private final byte[] columnFamily;
    private final byte[] fileIdBytes;
    private ResultScanner resultScanner;
    private int startPosition = 0;
    private int endPosition = Integer.MAX_VALUE;
    private VcfSliceProtos.VcfRecord nextVcfRecord = null;

    public VariantHadoopArchiveDBIterator(ResultScanner resultScanner, ArchiveHelper archiveHelper, QueryOptions options) {
        this.resultScanner = resultScanner;
        this.iterator = this.resultScanner.iterator();
        this.columnFamily = archiveHelper.getColumnFamily();
        this.fileIdBytes = archiveHelper.getColumn();
        VariantSource variantSource = archiveHelper.getMeta().getVariantSource();
        converter = new VcfRecordToVariantConverter(StudyEntry.sortSamplesPositionMap(variantSource.getSamplesPosition()),
                variantSource.getStudyId(), variantSource.getFileId());
        setLimit(options.getLong("limit"));
    }

    public VariantHadoopArchiveDBIterator(ResultScanner resultScanner, byte[] columnFamily, byte[] fileIdBytes, VcfMeta meta) {
        this.resultScanner = resultScanner;
        this.iterator = this.resultScanner.iterator();
        this.columnFamily = columnFamily;
        this.fileIdBytes = fileIdBytes;
        VariantSource variantSource = meta.getVariantSource();
        converter = new VcfRecordToVariantConverter(StudyEntry.sortSamplesPositionMap(variantSource.getSamplesPosition()),
                variantSource.getStudyId(), variantSource.getFileId());
    }

    @Override
    public boolean hasNext() {
        if (nextVcfRecord != null) {
            return true;
        } else {
            nextVcfRecord = nextVcfRecord();
            return nextVcfRecord != null;
        }
    }

    @Override
    public Variant next() {
        if (!(count < limit)) {
            throw new NoSuchElementException("Limit reached");
        }

        final VcfSliceProtos.VcfRecord vcfRecord;

        if (nextVcfRecord != null) {
            vcfRecord = nextVcfRecord;
            nextVcfRecord = null;
        } else {
            vcfRecord = nextVcfRecord();
        }

        if (vcfRecord == null) {
            throw new NoSuchElementException("Limit reached");
        }

        Variant variant;
        try {
            count++;
            variant = convert(() -> converter.convert(vcfRecord, vcfSlice.getChromosome(), vcfSlice.getPosition()));
        } catch (IllegalArgumentException e) {
            e.printStackTrace(System.err);
            System.err.println("vcfSlice.getPosition() = " + vcfSlice.getPosition());
            System.err.println("vcfRecord.getRelativeStart() = " + vcfRecord.getRelativeStart());
            System.err.println("vcfRecord.getRelativeEnd() = " + vcfRecord.getRelativeEnd());
            variant = new Variant(vcfSlice.getChromosome(), vcfRecord.getRelativeStart() + vcfSlice.getPosition(),
                    vcfRecord.getReference(), vcfRecord.getAlternate());
            logger.debug("variant: {}", variant.toString());
        }
        return variant;
    }

    private VcfSliceProtos.VcfRecord nextVcfRecord() {
        VcfSliceProtos.VcfRecord vcfRecord;
        int variantStart;
        do {
            if (!vcfRecordIterator.hasNext()) {
                if (!iterator.hasNext()) {
                    return null;
                }
                Result result = fetch(iterator::next);
                byte[] rid = result.getRow();
                try {
                    byte[] value = result.getValue(columnFamily, fileIdBytes);
                    vcfSlice = convert(() -> VcfSliceProtos.VcfSlice.parseFrom(value));
                    vcfRecordIterator = vcfSlice.getRecordsList().iterator();
                    converter.setFields(vcfSlice.getFields());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            vcfRecord = vcfRecordIterator.next();
            variantStart = vcfSlice.getPosition() + vcfRecord.getRelativeStart();
        } while (vcfRecord.getRelativeStart() < 0 || variantStart < this.startPosition || variantStart > this.endPosition);
        //Skip duplicated variant!
        return vcfRecord;
    }

    @Override
    public void close() {
        logger.debug("Close variant iterator. Fetch = {}ms, Convert = {}ms", getTimeFetching() / 1000000.0,
                getTimeConverting() / 1000000.0);
        resultScanner.close();
    }

    protected VariantHadoopArchiveDBIterator setLimit(long limit) {
        this.limit = limit <= 0 ? Long.MAX_VALUE : limit;
        return this;
    }

    public VariantHadoopArchiveDBIterator setRegion(Region region) {
        if (region == null) {
            return this;
        }
        this.startPosition = region.getStart();
        this.endPosition = region.getEnd();
        return this;
    }

    public ResultScanner getResultScanner() {
        return resultScanner;
    }
}
