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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converters.proto.VcfSliceToVariantListConverter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VariantLocalConflictResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class ArchiveResultToVariantConverter {
    private final Logger LOG = LoggerFactory.getLogger(ArchiveResultToVariantConverter.class);
    private final int studyId;
    private final AtomicReference<StudyConfiguration> sc = new AtomicReference<>();
    private byte[] columnFamily;
    private volatile ConcurrentHashMap<Integer, VcfSliceToVariantListConverter> fileidToConverter = new ConcurrentHashMap<>();
    private final AtomicBoolean parallel = new AtomicBoolean(false);

    public ArchiveResultToVariantConverter(int studyId, byte[] columnFamily, StudyConfiguration sc) {
        this.studyId = studyId;
        this.columnFamily = columnFamily;
        this.sc.set(sc);
    }

    public void setParallel(boolean parallel) {
        this.parallel.set(parallel);
    }

    public boolean isParallel() {
        return parallel.get();
    }

    public StudyConfiguration getSc() {
        return sc.get();
    }

    public List<Variant> convert(Result value, Long start, Long end, boolean resolveConflict) throws IllegalStateException {
        return convert(value, resolveConflict, var -> variantCoveringRegion(var, start, end, true));
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

    public List<Variant> convert(Result value, boolean resolveConflict) throws IllegalStateException {
        return convert(value, resolveConflict, Variant -> true); // Default -> use all
    }

    public List<Variant> convert(Result value, boolean resolveConflict, Predicate<Variant> positionFilter) throws IllegalStateException {
        Stream<Cell> cellStream =
                Arrays.stream(value.rawCells()).filter(c -> Bytes.equals(CellUtil.cloneFamily(c), columnFamily))
                        .filter(c -> !Bytes.startsWith(CellUtil.cloneQualifier(c), GenomeHelper.VARIANT_COLUMN_B_PREFIX));

        Function<Cell, Stream<? extends Variant>> cellStreamFunction = c -> {
            try {
                final List<Variant> variants = archiveCellToVariants(
                        CellUtil.cloneQualifier(c),
                        CellUtil.cloneValue(c));
                if (resolveConflict) {
                   return resolveConflicts(variants).stream().filter(positionFilter);
                }
                return variants.stream().filter(positionFilter);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
        };
        Collector<Variant, ?, List<Variant>> toList = Collectors.toCollection(CopyOnWriteArrayList::new);
        if (this.isParallel()) { // if parallel
            return cellStream.parallel().flatMap(cellStreamFunction).collect(toList);
        }
        return cellStream.flatMap(cellStreamFunction).collect(toList);
    }

    private List<Variant> archiveCellToVariants(byte[] key, byte[] value) throws InvalidProtocolBufferException {
        int fileId = ArchiveTableHelper.getFileIdFromColumnName(key);
        VcfSliceToVariantListConverter converter = loadConverter(fileId);
        VcfSlice vcfSlice = VcfSlice.parseFrom(value);
        return converter.convert(vcfSlice);
    }

    private VcfSliceToVariantListConverter loadConverter(int fileId) {
        return fileidToConverter.computeIfAbsent(fileId, key -> {
            LinkedHashSet<Integer> sampleIds = getSc().getSamplesInFiles().get(fileId);
            Map<String, Integer> thisFileSamplePositions = new LinkedHashMap<>();
            for (Integer sampleId : sampleIds) {
                String sampleName = getSc().getSampleIds().inverse().get(sampleId);
                thisFileSamplePositions.put(sampleName, thisFileSamplePositions.size());
            }
            VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(
                    thisFileSamplePositions, Integer.toString(fileId), Integer.toString(studyId));
            return converter;
        });
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
