/*
 * Copyright 2015-2016 OpenCB
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

/**
 * Converter to convert Archive Result to Variants
 */
package org.opencb.opencga.storage.hadoop.variant.archive;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converter.VcfSliceToVariantListConverter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VariantLocalConflictResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class ArchiveResultToVariantConverter {
    private final Logger LOG = LoggerFactory.getLogger(ArchiveResultToVariantConverter.class);
    private final int studyId;
    private final StudyConfiguration sc;
    private byte[] columnFamily;
    private Map<Integer, VcfSliceToVariantListConverter> fileidToConverter = new HashMap<Integer, VcfSliceToVariantListConverter>();

    public ArchiveResultToVariantConverter(int studyId, byte[] columnFamily, StudyConfiguration sc) {
        this.studyId = studyId;
        this.columnFamily = columnFamily;
        this.sc = sc;
    }

    public List<Variant> convert(Result value, Long start, Long end, boolean resolveConflict) throws InvalidProtocolBufferException {
        return filter(convert(value, resolveConflict), start, end);
    }

    private List<Variant> filter(List<Variant> variantList, Long start, Long end) {
        return variantList.stream() // only keep variants in
                // overlapping this region
                .filter(v -> variantCoveringRegion(v, start, end, true)).collect(Collectors.toList());
    }

    public static boolean variantCoveringRegion(Variant v, Long start, Long end, boolean inclusive) {
        int iStart = start.intValue();
        int iEnd = end.intValue();
        if (inclusive) {
            return iEnd >= v.getStart() && iStart <= v.getEnd();
        } else {
            return iEnd > v.getStart() && iStart < v.getEnd();
        }
    }

    public List<Variant> convert(Result value, boolean resolveConflict) throws InvalidProtocolBufferException {
        return Arrays.stream(value.rawCells()).filter(c -> Bytes.equals(CellUtil.cloneFamily(c), columnFamily))
                .filter(c -> !Bytes.startsWith(CellUtil.cloneQualifier(c), GenomeHelper.VARIANT_COLUMN_B_PREFIX))
                .flatMap(c -> {
                    try {
                        List<Variant> variants = archiveCellToVariants(
                                CellUtil.cloneQualifier(c),
                                CellUtil.cloneValue(c));
                        if (resolveConflict) {
                            variants = resolveConflicts(variants);
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("For Column %s found %s entries",
                                    ArchiveHelper.getFileIdFromColumnName(CellUtil.cloneQualifier(c)),
                                    c.getValueLength()));
                        }
                        return variants.stream();
                    } catch (InvalidProtocolBufferException e) {
                        throw new IllegalStateException(e);
                    }
                }).collect(Collectors.toList());
    }

    private List<Variant> archiveCellToVariants(byte[] key, byte[] value) throws InvalidProtocolBufferException {
        int fileId = ArchiveHelper.getFileIdFromColumnName(key);
        VcfSliceToVariantListConverter converter = loadConverter(fileId);
        VcfSlice vcfSlice = VcfSlice.parseFrom(value);
        return converter.convert(vcfSlice);
    }

    private VcfSliceToVariantListConverter loadConverter(int fileId) {
        VcfSliceToVariantListConverter converter = fileidToConverter.get(fileId);
        if (converter == null) {
            LinkedHashSet<Integer> sampleIds = sc.getSamplesInFiles().get(fileId);
            Map<String, Integer> thisFileSamplePositions = new LinkedHashMap<>();
            for (Integer sampleId : sampleIds) {
                String sampleName = sc.getSampleIds().inverse().get(sampleId);
                thisFileSamplePositions.put(sampleName, thisFileSamplePositions.size());
            }
            converter = new VcfSliceToVariantListConverter(thisFileSamplePositions, Integer.toString(fileId), Integer.toString(studyId));
            fileidToConverter.put(fileId, converter);
        }
        return converter;
    }

    /**
     * Resolve Conflict per file.
     * @param variants sorted list of variants
     * @return Valid set of variants without conflicts (each position only represented once)
     */
    public List<Variant> resolveConflicts(List<Variant> variants) {
        return new VariantLocalConflictResolver().resolveConflicts(variants);
    }

}
