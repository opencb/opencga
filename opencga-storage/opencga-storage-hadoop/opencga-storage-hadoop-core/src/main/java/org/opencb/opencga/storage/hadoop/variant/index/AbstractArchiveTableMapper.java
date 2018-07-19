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

package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveResultToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.AbstractHBaseVariantMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Abstract variant table map reduce.
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public abstract class AbstractArchiveTableMapper extends AbstractHBaseVariantMapper<ImmutableBytesWritable, Mutation> {

    private Logger logger = LoggerFactory.getLogger(AbstractArchiveTableMapper.class);

    protected ArchiveResultToVariantConverter resultConverter;
    private ArchiveRowKeyFactory rowKeyFactory;

    protected ArchiveResultToVariantConverter getResultConverter() {
        return resultConverter;
    }

    /**
     * Extracts file Ids from column names - ignoring _V columns.
     * @param value
     * @return Set of file IDs
     */
    private Set<Integer> extractFileIds(Result value) {
        Set<Integer> set = new HashSet<>();
        for (Cell c : value.rawCells()) {
            if (Bytes.equals(CellUtil.cloneFamily(c), getHelper().getColumnFamily())) {
                byte[] column = CellUtil.cloneQualifier(c);
                if (ArchiveTableHelper.isNonRefColumn(column)) {
                    set.add(ArchiveTableHelper.getFileIdFromNonRefColumnName(column));
                } else if (ArchiveTableHelper.isRefColumn(column)) {
                    set.add(ArchiveTableHelper.getFileIdFromRefColumnName(column));
                }
            }
        }
        return set;
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);

        // Load VCF meta data for columns
        int studyId = getStudyConfiguration().getStudyId();
        resultConverter = new ArchiveResultToVariantConverter(studyId, getHelper().getColumnFamily(), this.getStudyConfiguration());

        rowKeyFactory = new ArchiveRowKeyFactory(getHelper().getConf());
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        super.cleanup(context);
    }

    @Override
    public final void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        logger.info("Start mapping key: " + Bytes.toString(key.get()));
        startStep();
        if (value.isEmpty()) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "VCF_RESULT_EMPTY").increment(1);
            return; // TODO search backwards?
        }

        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "VCF_BLOCK_READ").increment(1);


        // Calculate various positions
        byte[] currRowKey = key.get();
        String sliceKey = Bytes.toString(currRowKey);
        String chr = rowKeyFactory.extractChromosomeFromBlockId(sliceKey);
        Long sliceReg = rowKeyFactory.extractSliceFromBlockId(sliceKey);
        long startPos = rowKeyFactory.getStartPositionFromSlice(sliceReg);
        long nextStartPos = rowKeyFactory.getStartPositionFromSlice(sliceReg + 1);

        Set<Integer> fileIds = extractFileIds(value);
        if (logger.isDebugEnabled()) {
            logger.debug("Results contain file IDs : " + StringUtils.join(fileIds, ','));
        }
        Set<Integer> sampleIds = new HashSet<>();
        for (Integer fid : fileIds) {
            LinkedHashSet<Integer> sids = getStudyConfiguration().getSamplesInFiles().get(fid);
            sampleIds.addAll(sids);
        }

        logger.debug("Processing slice {}", sliceKey);


        VariantMapReduceContext ctx = new VariantMapReduceContext(currRowKey, context, value, fileIds,
                sampleIds, chr, startPos, nextStartPos);

        endStep("1 Prepare slice");

        /* *********************************** */
        /* ********* CALL concrete class ***** */
        map(ctx);
        /* *********************************** */

        // Clean up of this slice
        this.getMrHelper().addTimesAsCounters();

        logger.info("Finished mapping key: " + Bytes.toString(key.get()));
    }

    protected abstract void map(VariantMapReduceContext ctx) throws IOException, InterruptedException;

    protected static class VariantMapReduceContext {
        public VariantMapReduceContext(byte[] currRowKey, Context context, Result value, Set<Integer> fileIdsInResult,
                                       Set<Integer> sampleIds, String chr, long startPos, long nextStartPos) {
            this.currRowKey = currRowKey;
            this.context = context;
            this.value = value;
            this.fileIdsInResult = fileIdsInResult;
            this.sampleIds = sampleIds;
            this.chr = chr;
            this.startPos = startPos;
            this.nextStartPos = nextStartPos;
        }

        protected final byte[] currRowKey;
        protected final Context context;
        protected final Result value;
        private final Set<Integer> fileIdsInResult;
        protected final Set<Integer> sampleIds;
        private final String chr;
        protected final long startPos;
        protected final long nextStartPos;


        public byte[] getCurrRowKey() {
            return currRowKey;
        }

        public Context getContext() {
            return context;
        }

        public Result getValue() {
            return value;
        }

        public Set<Integer> getSampleIds() {
            return sampleIds;
        }

        public String getChromosome() {
            return chr;
        }

        public Set<Integer> getFileIdsInResult() {
            return fileIdsInResult;
        }

        public long getStartPos() {
            return startPos;
        }

        public long getNextStartPos() {
            return nextStartPos;
        }
    }
}
