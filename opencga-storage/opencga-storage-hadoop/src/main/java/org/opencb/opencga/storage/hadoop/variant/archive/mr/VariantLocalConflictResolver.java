package org.opencb.opencga.storage.hadoop.variant.archive.mr;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantVcfFactory;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;

import java.util.*;
import java.util.stream.Collectors;

import static htsjdk.variant.vcf.VCFConstants.GENOTYPE_FILTER_KEY;
import static htsjdk.variant.vcf.VCFConstants.GENOTYPE_KEY;
import static org.opencb.biodata.models.variant.VariantVcfFactory.FILTER;
import static org.opencb.biodata.models.variant.VariantVcfFactory.QUAL;
import static org.opencb.biodata.models.variant.avro.VariantType.NO_VARIATION;

/**
 * Created by mh719 on 10/06/2016.
 */
public class VariantLocalConflictResolver {


    private VariantPositionRefAltComparator POS_REF_ALT_COMP = new VariantPositionRefAltComparator();
    public List<Variant> resolveConflicts(List<Variant> variants) {
        // sorted by position assumed
        List<Variant> varSorted = new ArrayList<>(variants);
        Collections.sort(varSorted, POS_REF_ALT_COMP);
        List<Variant> resolved = new ArrayList<>(variants.size());
        List<Variant> currVariants = new ArrayList<>();
        for (Variant var : varSorted) {
            if (currVariants.isEmpty()) { // init
                currVariants.add(var);
            } else if (!VariantLocalConflictResolver.hasAnyConflictOverlapInclSecAlt(currVariants, var)) { // no overlap
                resolved.addAll(resolve(currVariants));
                currVariants.clear();
                currVariants.add(var);
            } else { // partial or full overlap
                currVariants.add(var);
            }
        }
        resolved.addAll(resolve(currVariants));
        return resolved;
    }

    /**
     * Returns a nonredudant set of variants, where each position of the genome is only covered once.<br>
     * Conflicting regions are converted as NO_VARIATION.
     *
     * @param conflicts List of Variants with conflicts.
     * @return List of Variants
     */
    public List<Variant> resolve(List<Variant> conflicts) {
        if (conflicts.size() < 2) {
            return conflicts;
        }
        // <start,end> pair stream
        List<Pair<Integer, Integer>> secAltPairs = buildRegions(conflicts);

        int min = secAltPairs.stream().mapToInt(p -> p.getLeft()).min().getAsInt();
        int max = secAltPairs.stream().mapToInt(p -> p.getRight()).max().getAsInt();

        List<Variant> sorted = new ArrayList<>(conflicts);
        sorted.sort(VARIANT_COMP);
        List<Variant> resolved = new ArrayList<>();
        List<Variant> misfit = new ArrayList<>();
        for (Variant q : sorted) {
            if (resolved.isEmpty()) {
                resolved.add(q);
            } else {
                if (!hasAnyConflictOverlapInclSecAlt(resolved, q)) {
                    resolved.add(q);
                } else if (allSameTypeAndGT(resolved, q, VariantType.NO_VARIATION)) {
                    List<Variant> collect = resolved.stream().filter(r -> r.overlapWith(q, true))
                            .collect(Collectors.toList());
                    if (collect.size() != 1) {
                        throw new IllegalStateException("Found " + collect.size() + " overlapping variants for " + q);
                    }
                    Variant variant = collect.get(0);
                    variant.setStart(Math.min(variant.getStart(), q.getStart()));
                    variant.setEnd(Math.max(variant.getEnd(), q.getEnd()));
                    variant.setLength((variant.getEnd() - variant.getStart()) + 1);
                } else {
                    // does not fit
                    misfit.add(q);
                }
            }
        }
        if (!misfit.isEmpty()) {
            // fit into place (before and after)
            fillNoCall(resolved, misfit.get(0), min, max);
        }
        return resolved;
    }

    private boolean allSameTypeAndGT(List<Variant> resolved, Variant q, VariantType type) {
        if (!q.getType().equals(type)) {
            return false;
        }
        StudyEntry studyEntry = q.getStudies().get(0);
        String sample = studyEntry.getSamplesName().stream().findFirst().get();

        String gt = studyEntry.getSampleData(sample, GENOTYPE_KEY);
        long count = resolved.stream().filter(v -> v.getType().equals(type)
                && StringUtils.equals(gt, v.getStudies().get(0).getSampleData(sample, GENOTYPE_KEY))).count();
        return ((int) count) == resolved.size();
    }

    private static VariantPositionComparator varPositionOrder = new VariantPositionComparator();
    private static PositionComparator positionOrder = new PositionComparator();

    private void fillNoCall(List<Variant> resolved, Variant q, int start, int end) {

        // find missing pieces
        List<Pair<Integer, Integer>> holes = getMissingRegions(resolved, start, end);

        // create NO_VARIANT fillers for holes
        if (holes.size() == 1) { // only one hole - use query variant
            Pair<Integer, Integer> h = holes.get(0);
            changeVariantToNoCall(q, h.getKey(), h.getValue());
            resolved.add(q);
        } else {
            for (Pair<Integer, Integer> h : holes) { // > 1 hole -> need to make copies of variant object
                Variant v = deepCopy(q);
                changeVariantToNoCall(v, h.getKey(), h.getValue());
                resolved.add(v);
            }
        }
    }
    public static List<Pair<Integer, Integer>> getMissingRegions(List<Variant> target, Variant query) {
        return getMissingRegions(target, query.getStart(), query.getEnd());
    }

    public static List<Pair<Integer, Integer>> getMissingRegions(List<Variant> target, int start, int end) {
        List<Pair<Integer, Integer>> targetReg = new ArrayList<>(new HashSet<>(buildRegions(target)));
        targetReg.sort(positionOrder);
//        target.sort(varPositionOrder);

        int min = start;
        int max = end;
        if (max < min) {
            // Insertion -> no need for holes
            return Collections.emptyList();
        }
        int minTarget = targetReg.stream().mapToInt(p -> p.getLeft()).min().getAsInt();
        int maxTarget = targetReg.stream().mapToInt(p -> p.getRight()).max().getAsInt();
        if (maxTarget < minTarget) {
            // Insertion -> split query region into two
            return Arrays.asList(new ImmutablePair<>(min, maxTarget), new ImmutablePair<>(minTarget, max));
        }
        // find missing pieces
        List<Pair<Integer, Integer>> holes = new ArrayList<>();
        for (Pair<Integer, Integer> pair : targetReg) {
            if (min > max) {
                break; // All holes closed
            }
            if (max < pair.getLeft()) { // Region ends before or at start of this target
                holes.add(new ImmutablePair<>(min, max));
                break; // finish
            } else if (min > pair.getRight()) {
                // No overlap
                min = Math.max(min, pair.getRight() + 1);
            } else if (min >= pair.getLeft() && max <= pair.getRight()) {
                // Full overlap
                min = Math.max(min, pair.getRight() + 1); // Reset min to current target end +1
            } else if (min < pair.getLeft() && max >= pair.getLeft()) {
                // Query overlaps with target start
                holes.add(new ImmutablePair<>(min, pair.getLeft() - 1));
                min = Math.max(min, pair.getRight() + 1);
            } else if (min <= pair.getRight() && max >= pair.getRight()) {
                // Query overlaps with target end
                min = Math.max(min, pair.getRight() + 1); // Reset min to current target end +1
            }
        }
        // Fill in holes at the end
        if (min > maxTarget && min <= max) {
            holes.add(new ImmutablePair<>(min, max));
        }
        return holes;
    }

    private static List<Pair<Integer, Integer>> buildRegions(List<Variant> target) {
        return target.stream().map(v -> buildRegions(v)).flatMap(l -> l.stream()).collect(Collectors.toList());
    }

    private static List<Pair<Integer, Integer>> buildRegions(Variant target) {
        List<Variant> vlst = new ArrayList<Variant>();
        vlst.add(target);
        vlst.addAll(expandToVariants(target));
        return vlst.stream().map(v -> new ImmutablePair<>(v.getStart(), v.getEnd())).collect(Collectors.toList());
    }

    public static boolean hasAnyConflictOverlapInclSecAlt(List<Variant> target, Variant query) {
        return target.stream().filter(v -> hasAnyConflictOverlapInclSecAlt(v, query)).findAny().isPresent();
    }

    public static boolean hasAnyConflictOverlapInclSecAlt(Variant a, Variant b) {
        // Check Direct overlap
        if (hasConflictOverlap(a, b)) {
            return true;
        }
        // Check AltCoords as well
        List<Variant> aList = expandToVariants(a);
        List<Variant> bList = expandToVariants(b);

        if (aList.size() == 1 && bList.size() == 1) {
            return false; // No Secondary alternates in the list.
        }
        // Search for any overlap between both lists
        boolean overlapExist = aList.stream().filter(av -> bList.stream().filter(bv -> hasConflictOverlap(av, bv)).findAny()
                .isPresent()).findAny().isPresent();

        return overlapExist;
    }

    /**
     * Creates a list with the provided Variant and all secondary alternates {@link AlternateCoordinate} converted to
     * Variants.
     * @param v {@link Variant}
     * @return List of Variant positions.
     */
    public static List<Variant> expandToVariants(Variant v) {
        if (v.getStudies().isEmpty()) {
            return Collections.singletonList(v);
        }
        StudyEntry studyEntry = v.getStudies().get(0);
        List<AlternateCoordinate> secAlt = studyEntry.getSecondaryAlternates();
        if (secAlt.isEmpty()) {
            return Collections.singletonList(v);
        }
        // Check AltCoords as well
        List<Variant> list = new ArrayList<>(Collections.singletonList(v));
        secAlt.forEach(alt -> list.add(asVariant(v, alt)));
        return list;
    }

    private static boolean hasConflictOverlap(Variant a, Variant b) {
        boolean conflict = a.overlapWith(b, true);
        if (conflict && (isInsertion(a) || isInsertion(b))) {
            // in case of insertions
            if (isInsertion(a) != isInsertion(b)) { // one of them insertion
                conflict = isInsertion(a) ? isInsertionCovered(a, b) : isInsertionCovered(b, a);
            }
        }
        return conflict;
    }

    private static boolean isInsertion(Variant variant) {
        return variant.getStart() > variant.getEnd();
    }

    private static boolean isInsertionCovered(Variant insertion, Variant notInsertion) {
        if (!isInsertion(insertion)) {
            throw new IllegalStateException("Variable insertion is not an Insertion:" + insertion);
        }
        if (isInsertion(notInsertion)) {
            throw new IllegalStateException("Variable notInsertion is an Insertion:" + notInsertion);
        }
        Integer start = notInsertion.getStart();
        Integer end = notInsertion.getEnd();
        return start <= insertion.getStart() && insertion.getStart() <= end
                && start <= insertion.getEnd() && insertion.getEnd() <= end;
    }

    public static Variant asVariant(Variant a, AlternateCoordinate altA) {
        String chr = ObjectUtils.firstNonNull(altA.getChromosome(), a.getChromosome());
        Integer start  = ObjectUtils.firstNonNull(altA.getStart(), a.getStart());
        Integer end  = ObjectUtils.firstNonNull(altA.getEnd(), a.getEnd());
        String ref  = ObjectUtils.firstNonNull(altA.getReference(), a.getReference());
        String alt  = ObjectUtils.firstNonNull(altA.getAlternate(), a.getAlternate());
        try {
            return new Variant(chr, start, end, ref, alt);
        } catch (IllegalArgumentException e) {
            String msg = altA + "\n" + a.toJson() + "\n";
            throw new IllegalStateException(msg, e);
        }
    }

    public static int min(List<Variant> target) {
        return target.stream().mapToInt(v -> v.getStart().intValue()).min().getAsInt();
    }

    public static int max(List<Variant> target) {
        return target.stream().mapToInt(v -> v.getEnd().intValue()).max().getAsInt();
    }

    public static String extractGenotypeFilter(Variant a) {
        List<StudyEntry> studies = a.getStudies();
        if (studies.isEmpty()) {
            return null;
        }
        StudyEntry studyEntry = studies.get(0);
        List<List<String>> samplesData = studyEntry.getSamplesData();
        if (samplesData == null || samplesData.isEmpty()) {
            return null;
        }
        Integer keyPos = studyEntry.getFormatPositions().get(GENOTYPE_FILTER_KEY);
        if (null == keyPos) {
            return null;
        }
        List<String> sample = samplesData.get(0);
        if (sample.isEmpty()) {
            return null;
        }
        String af = sample.get(keyPos);
        return af;
    }

    public static String extractFileAttribute(Variant a, String key) {
        List<StudyEntry> studies = a.getStudies();
        if (studies.isEmpty()) {
            return null;
        }
        StudyEntry studyEntry = studies.get(0);
        List<FileEntry> files = studyEntry.getFiles();
        if (files == null || files.isEmpty()) {
            return null;
        }
        return files.get(0).getAttributes().get(key);
    }

    /**
     * Checks if SiteConflict is part of the Filter flag.
     *
     * @param a Variant
     * @param b Variant
     * @return -1 if in a, 1 if in b, 0 if in both or non.
     */
    public static int checkSiteConflict(Variant a, Variant b) {
        String af = extractFileAttribute(a, FILTER);
        String bf = extractFileAttribute(b, FILTER);
        return checkStringConflict(af, bf, "SiteConflict");
    }

    /**
     * Checks PASS is the filter flag.
     *
     * @param a Variant
     * @param b Variant
     * @return -1 if in a, 1 if in b, 0 if in both or non.
     */
    public static int checkPassConflict(Variant a, Variant b) {
        String af = ObjectUtils.firstNonNull(extractFileAttribute(a, FILTER), extractGenotypeFilter(a));
        String bf = ObjectUtils.firstNonNull(extractFileAttribute(b, FILTER), extractGenotypeFilter(b));
        return checkStringConflict(af, bf, "PASS");
    }

    /**
     * Checks Quality score.
     *
     * @param a Variant
     * @param b Variant
     * @return -1 if lower in a, 1 if lower in b, 0 if in both are the same or dot.
     */
    public static int checkQualityScore(Variant a, Variant b) {
        String af = extractFileAttribute(a, QUAL);
        String bf = extractFileAttribute(b, QUAL);
        Double va = StringUtils.isBlank(af) || StringUtils.equals(af, ".") ? 0d : Double.valueOf(af);
        Double vb = StringUtils.isBlank(bf) || StringUtils.equals(bf, ".") ? 0d : Double.valueOf(bf);
        return va.compareTo(vb);
    }

    public static int checkStringConflict(String a, String b, String query) {
        if (!(StringUtils.contains(a, query) && StringUtils.contains(b, query))) {
            if (StringUtils.contains(a, query)) {
                return -1;
            }
            if (StringUtils.contains(b, query)) {
                return 1;
            }
        }
        return 0;
    }

    private static final VariantComparator VARIANT_COMP = new VariantComparator();

    private static class VariantComparator implements Comparator<Variant> {

        /**
         * Sorts variants by.
         * <ul>
         * <li>PASS gets preference</li>
         * <li>SiteConflict less priority</li>
         * <li>Higher quality score first</li>
         * <li>Variant before reference block</li>
         * <li>Earlier start</li>
         * <li>Earlier end</li>
         * <li>Hash code</li>
         * </ul>
         *
         * @param o1 Variant
         * @param o2 Variant
         * @return int
         */
        @Override
        public int compare(Variant o1, Variant o2) {
            // Variant before reference block
            if (o1.getType().equals(NO_VARIATION)) {
                if (!o2.getType().equals(NO_VARIATION)) {
                    return 1;
                }
            } else if (o2.getType().equals(NO_VARIATION)) {
                return -1;
            }
            // Check for PASS
            int c = checkPassConflict(o1, o2);
            if (c != 0) {
                return c;
            }
            // Check SiteConflict
            c = checkSiteConflict(o1, o2) * -1; // invert result
            if (c != 0) {
                return c;
            }
            // Check Quality scores
            c = checkQualityScore(o1, o2) * -1; // invert result - higher score better
            if (c != 0) {
                return c;
            }
            c = o1.getStart().compareTo(o2.getStart());
            if (c != 0) {
                return c;
            }
            c = o1.getEnd().compareTo(o2.getEnd());
            if (c != 0) {
                return c;
            }
            return Integer.compare(o1.hashCode(), o2.hashCode());
        }
    }

    private static class VariantPositionComparator implements Comparator<Variant> {

        @Override
        public int compare(Variant o1, Variant o2) {
            int c = o1.getStart().compareTo(o2.getStart());
            if (c != 0) {
                return c;
            }
            c = o1.getEnd().compareTo(o2.getEnd());
            if (c != 0) {
                return c;
            }
            return Integer.compare(o1.hashCode(), o2.hashCode());
        }
    }

    private static class VariantPositionRefAltComparator implements Comparator<Variant> {

        @Override
        public int compare(Variant o1, Variant o2) {
            int c = o1.getStart().compareTo(o2.getStart());
            if (c != 0) {
                return c;
            }
            c = o1.getEnd().compareTo(o2.getEnd());
            if (c != 0) {
                return c;
            }
            c = o1.getReference().compareTo(o2.getReference());
            if (c != 0) {
                return c;
            }
            c = o1.getAlternate().compareTo(o2.getAlternate());
            if (c != 0) {
                return c;
            }
            return Integer.compare(o1.hashCode(), o2.hashCode());
        }
    }

    private static class PositionComparator implements Comparator<Pair<Integer, Integer>> {

        @Override
        public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
            int c = o1.getLeft().compareTo(o2.getLeft());
            if (c != 0) {
                return c;
            }
            c = o1.getRight().compareTo(o2.getRight());
            if (c != 0) {
                return c;
            }
            return Integer.compare(o1.hashCode(), o2.hashCode());
        }
    }

    public static Variant deepCopy(Variant var) {

        Variant v = new Variant(var.getChromosome(), var.getStart(), var.getEnd(), var.getReference(), var
                .getAlternate());
        v.setIds(var.getIds());
        v.setStrand(var.getStrand());
        v.setAnnotation(var.getAnnotation());

        for (StudyEntry vse : var.getStudies()) {
            StudyEntry se = new StudyEntry();
            se.setStudyId(vse.getStudyId());
            if (null != vse.getSamplesPosition()) {
                se.setSamplesPosition(new HashMap<>(vse.getSamplesPosition()));
            } else {
                se.setSamplesPosition(new HashMap<>());
            }
            if (null != vse.getFormat()) {
                se.setFormat(new ArrayList<>(vse.getFormat()));
            } else {
                se.setFormat(new ArrayList<>());
            }

            List<FileEntry> files = new ArrayList<>(vse.getFiles().size());
            for (FileEntry file : vse.getFiles()) {
                HashMap<String, String> attributes = new HashMap<>(file.getAttributes()); //TODO: Check file attributes
                files.add(new FileEntry(file.getFileId(), file.getCall(), attributes));
            }
            se.setFiles(files);

            int samplesSize = vse.getSamplesData().size();
            List<List<String>> newSampleData = new ArrayList<>(samplesSize);
            for (int i = 0; i < samplesSize; i++) {
                List<String> sd = vse.getSamplesData().get(i);
                newSampleData.add(new ArrayList<>(sd));
            }
            se.setSamplesData(newSampleData);

            v.addStudyEntry(se);
        }
        return v;
    }

    public static void changeVariantToNoCall(Variant var, Integer start, Integer end) {
        var.setReference("");
        var.setAlternate("");
        var.setStart(start);
        var.setEnd(end);
        String genotype = VariantTableStudyRow.NOCALL;
        var.setType(NO_VARIATION);
        StudyEntry se = var.getStudies().get(0);
        Map<String, Integer> formatPositions = se.getFormatPositions();
        int gtpos = formatPositions.get(GENOTYPE_KEY);
        int filterPos = formatPositions.containsKey(GENOTYPE_FILTER_KEY)
                ? formatPositions.get(GENOTYPE_FILTER_KEY) : -1;
        List<List<String>> sdLst = se.getSamplesData();
        List<List<String>> oLst = new ArrayList<>(sdLst.size());
        for (List<String> sd : sdLst) {
            List<String> o = new ArrayList<>(sd);
            o.set(gtpos, genotype);
            if (filterPos != -1) {
                o.set(filterPos, "SiteConflict");
            }
            oLst.add(o);
        }
        se.setSamplesData(oLst);
        se.setSecondaryAlternates(new ArrayList<>());
        for (FileEntry fe : se.getFiles()) {
            Map<String, String> feAttr = fe.getAttributes();
            if (null == feAttr) {
                feAttr = new HashMap<>();
            } else {
                feAttr = new HashMap<>(feAttr);
            }
            feAttr.put(VariantVcfFactory.FILTER, "SiteConflict");
            fe.setAttributes(feAttr);
        }
    }
}
