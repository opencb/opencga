/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * Created on 04/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopArchiveDBIterator extends VariantDBIterator implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(VariantHadoopArchiveDBIterator.class);
    private final VcfRecordProtoToVariantConverter nonRefConverter;
    private final VcfRecordProtoToVariantConverter refConverter;
    private long limit;
    private int count = 0;
    private ListIterator<VcfSliceProtos.VcfRecord> refVcfRecordIterator = Collections.emptyListIterator();
    private ListIterator<VcfSliceProtos.VcfRecord> nonRefVcfRecordIterator = Collections.emptyListIterator();
    private VcfSliceProtos.VcfSlice refVcfSlice;
    private VcfSliceProtos.VcfSlice nonRefVcfSlice;
    private final Iterator<Result> iterator;
    private final byte[] columnFamily;
    private final byte[] refColumnBytes;
    private final byte[] nonRefColumnBytes;
    private ResultScanner resultScanner;
    private int startPosition = 0;
    private int endPosition = Integer.MAX_VALUE;
    private Variant nextVariant = null;

    public VariantHadoopArchiveDBIterator(ResultScanner resultScanner, ArchiveTableHelper archiveHelper, QueryOptions options) {
        this.resultScanner = resultScanner;
        this.iterator = this.resultScanner.iterator();
        this.columnFamily = archiveHelper.getColumnFamily();
        this.refColumnBytes = archiveHelper.getRefColumnName();
        this.nonRefColumnBytes = archiveHelper.getNonRefColumnName();
        VariantFileMetadata fileMetadata = archiveHelper.getFileMetadata();
        nonRefConverter = new VcfRecordProtoToVariantConverter(StudyEntry.sortSamplesPositionMap(fileMetadata.getSamplesPosition()),
                String.valueOf(archiveHelper.getStudyId()), fileMetadata.getId());
        refConverter = new VcfRecordProtoToVariantConverter(StudyEntry.sortSamplesPositionMap(fileMetadata.getSamplesPosition()),
                String.valueOf(archiveHelper.getStudyId()), fileMetadata.getId());
        setLimit(options.getLong(QueryOptions.LIMIT));
    }


    @Override
    public boolean hasNext() {
        if (nextVariant != null) {
            return true;
        } else {
            nextVariant = nextVariant();
            return nextVariant != null;
        }
    }

    @Override
    public Variant next() {
        if (!(count < limit)) {
            throw new NoSuchElementException("Limit reached");
        }

        final Variant variant;

        if (nextVariant != null) {
            variant = nextVariant;
            nextVariant = null;
        } else {
            variant = nextVariant();
        }


        if (variant == null) {
            throw new NoSuchElementException("Limit reached");
        }

        count++;

        return variant;
    }

    private Variant nextVariant() {
        VcfSliceProtos.VcfRecord vcfRecord;
        VcfSliceProtos.VcfSlice vcfSlice;
        VcfRecordProtoToVariantConverter converter;
        int variantStart;
        do {
            if (!nonRefVcfRecordIterator.hasNext() && !refVcfRecordIterator.hasNext()) {
                if (!iterator.hasNext()) {
                    return null;
                }
                Result result = fetch(iterator::next);
                byte[] rid = result.getRow();
                try {
                    byte[] nonRefValue = result.getValue(columnFamily, nonRefColumnBytes);
                    if (nonRefValue != null && nonRefValue.length > 0) {
                        nonRefVcfSlice = convert(() -> VcfSliceProtos.VcfSlice.parseFrom(nonRefValue));
                        nonRefVcfRecordIterator = nonRefVcfSlice.getRecordsList().listIterator();
                        nonRefConverter.setFields(nonRefVcfSlice.getFields());
                    }
                    byte[] refValue = result.getValue(columnFamily, refColumnBytes);
                    if (refValue != null && refValue.length > 0) {
                        refVcfSlice = convert(() -> VcfSliceProtos.VcfSlice.parseFrom(refValue));
                        refVcfRecordIterator = refVcfSlice.getRecordsList().listIterator();
                        refConverter.setFields(refVcfSlice.getFields());
                    }
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            if (nonRefVcfRecordIterator.hasNext() && refVcfRecordIterator.hasNext()) {
                VcfSliceProtos.VcfRecord nonRefVcfRecord = nonRefVcfRecordIterator.next();
                VcfSliceProtos.VcfRecord refVcfRecord = refVcfRecordIterator.next();
                if (nonRefVcfRecord.getRelativeStart() > refVcfRecord.getRelativeStart()) {
                    vcfRecord = refVcfRecord;
                    vcfSlice = refVcfSlice;
                    converter = refConverter;
                    nonRefVcfRecordIterator.previous();
                } else {
                    vcfRecord = nonRefVcfRecord;
                    vcfSlice = nonRefVcfSlice;
                    converter = nonRefConverter;
                    refVcfRecordIterator.previous();
                }
            } else if (nonRefVcfRecordIterator.hasNext()) {
                vcfRecord = nonRefVcfRecordIterator.next();
                vcfSlice = nonRefVcfSlice;
                converter = nonRefConverter;
            } else {
                vcfRecord = refVcfRecordIterator.next();
                vcfSlice = refVcfSlice;
                converter = refConverter;
            }
            variantStart = nonRefVcfSlice.getPosition() + vcfRecord.getRelativeStart();
        } while (vcfRecord.getRelativeStart() < 0 || variantStart < this.startPosition || variantStart > this.endPosition);
        //Skip duplicated variant!

        Variant variant;
        try {
            VcfRecordProtoToVariantConverter finalConverter = converter;
            VcfSliceProtos.VcfRecord finalVcfRecord = vcfRecord;
            VcfSliceProtos.VcfSlice finalSlice = vcfSlice;
            variant = convert(() -> finalConverter.convert(finalVcfRecord, finalSlice.getChromosome(), finalSlice.getPosition()));
        } catch (IllegalArgumentException e) {
            e.printStackTrace(System.err);
            System.err.println("vcfSlice.getPosition() = " + nonRefVcfSlice.getPosition());
            System.err.println("vcfRecord.getRelativeStart() = " + vcfRecord.getRelativeStart());
            System.err.println("vcfRecord.getRelativeEnd() = " + vcfRecord.getRelativeEnd());
            variant = new Variant(nonRefVcfSlice.getChromosome(), vcfRecord.getRelativeStart() + nonRefVcfSlice.getPosition(),
                    vcfRecord.getReference(), vcfRecord.getAlternate());
            logger.debug("variant: {}", variant.toString());
        }

        return variant;
    }

    @Override
    public void close() throws Exception {
        super.close();
        logger.debug("Close variant iterator. Fetch = {}ms, Convert = {}ms", getTimeFetching() / 1000000.0,
                getTimeConverting() / 1000000.0);
        resultScanner.close();
    }

    @Override
    public int getCount() {
        return count;
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
