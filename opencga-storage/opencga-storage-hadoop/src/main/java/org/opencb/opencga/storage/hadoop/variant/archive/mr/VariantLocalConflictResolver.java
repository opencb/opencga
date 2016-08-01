package org.opencb.opencga.storage.hadoop.variant.archive.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;

import java.util.*;

import static org.opencb.biodata.models.variant.VariantVcfFactory.FILTER;
import static org.opencb.biodata.models.variant.VariantVcfFactory.QUAL;

/**
 * Created by mh719 on 10/06/2016.
 */
public class VariantLocalConflictResolver {


    public List<Variant> resolveConflicts(List<Variant> variants) {
        // sorted by position assumed
        List<Variant> resolved = new ArrayList<>(variants.size());
        List<Variant> currVariants = new ArrayList<>();
        for (Variant var : variants) {
            if (currVariants.isEmpty()) { // init
                currVariants.add(var);
            } else if (!VariantLocalConflictResolver.hasOverlap(currVariants, var)) { // no overlap
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
        List<Variant> sorted = new ArrayList<>(conflicts);
        sorted.sort(VARIANT_COMP);
        int min = min(conflicts);
        int max = max(conflicts);
        List<Variant> resolved = new ArrayList<>();
        for (Variant q : sorted) {
            if (resolved.isEmpty()) {
                resolved.add(q);
            } else {
                if (!hasOverlap(resolved, q)) {
                    resolved.add(q);
                } else {
                    // fit into place (before and after)
                    fillNoCall(resolved, q);
                }
            }
        }
        return resolved;
    }

    private static VariantPositionComparator varPositionOrder = new VariantPositionComparator();

    private void fillNoCall(List<Variant> resolved, Variant q) {

        // find missing pieces
        List<Pair<Integer, Integer>> holes = getMissingRegions(resolved, q);

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
        target.sort(varPositionOrder);
        int min = query.getStart();
        int max = query.getEnd();
        int minTarget = min(target);
        int maxTarget = max(target);

        if (max < minTarget || min > maxTarget) {
            throw new IllegalStateException(String.format("Region is outside of targets: %s %s, %s %s", min, max,
                    minTarget, maxTarget));
        }

        // find missing pieces
        List<Pair<Integer, Integer>> holes = new ArrayList<>();
        for (Variant v : target) {
            if (min > max) {
                break; // All holes closed
            }
            if (max < v.getStart()) { // Region ends before or at start of this target
                holes.add(new ImmutablePair<>(min, max));
                break; // finish
            } else if (min < v.getStart() && max >= v.getStart()) {
                // Query overlaps with target start
                holes.add(new ImmutablePair<>(min, v.getStart() - 1));
                min = v.getEnd() + 1;
            } else if (min <= v.getEnd() && max >= v.getEnd()) {
                // Query overlaps with target end
                min = v.getEnd() + 1; // Reset min to current target end +1
            }
        }
        // Fill in holes at the end
        if (min > maxTarget && min <= max) {
            holes.add(new ImmutablePair<>(min, max));
        }
        return holes;
    }

    public static boolean hasOverlap(List<Variant> target, Variant query) {
        return target.stream().filter(
                v -> v.getEnd() >= query.getStart() && v.getStart() <= query.getEnd()
        ).findAny().isPresent();
    }

    public static int min(List<Variant> target) {
        return target.stream().mapToInt(v -> v.getStart().intValue()).min().getAsInt();
    }

    public static int max(List<Variant> target) {
        return target.stream().mapToInt(v -> v.getEnd().intValue()).max().getAsInt();
    }

    /**
     * Checks if SiteConflict is part of the Filter flag.
     *
     * @param a Variant
     * @param b Variant
     * @return -1 if in a, 1 if in b, 0 if in both or non.
     */
    public static int checkSiteConflict(Variant a, Variant b) {
        String af = a.getStudies().get(0).getFiles().get(0).getAttributes().get(FILTER);
        String bf = b.getStudies().get(0).getFiles().get(0).getAttributes().get(FILTER);
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
        String af = a.getStudies().get(0).getFiles().get(0).getAttributes().get(FILTER);
        String bf = b.getStudies().get(0).getFiles().get(0).getAttributes().get(FILTER);
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
        String af = a.getStudies().get(0).getFiles().get(0).getAttributes().get(QUAL);
        String bf = b.getStudies().get(0).getFiles().get(0).getAttributes().get(QUAL);
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
            // Variant before reference block
            if (o1.getType().equals(VariantType.NO_VARIATION)) {
                if (!o2.getType().equals(VariantType.NO_VARIATION)) {
                    return 1;
                }
            } else if (o2.getType().equals(VariantType.NO_VARIATION)) {
                return -1;
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

    public static void changeVariantToNoCall(Variant var) {
        changeVariantToNoCall(var, var.getStart(), var.getEnd());
    }

    public static void changeVariantToNoCall(Variant var, Integer start, Integer end) {
        var.setReference("");
        var.setAlternate("");
        var.setStart(start);
        var.setEnd(end);
        String genotype = VariantTableStudyRow.NOCALL;
        var.setType(VariantType.NO_VARIATION);
        StudyEntry se = var.getStudies().get(0);
        Map<String, Integer> formatPositions = se.getFormatPositions();
        int gtpos = formatPositions.get("GT");
        List<List<String>> sdLst = se.getSamplesData();
        List<List<String>> oLst = new ArrayList<>(sdLst.size());
        for (List<String> sd : sdLst) {
            List<String> o = new ArrayList<>(sd);
            o.set(gtpos, genotype);
            oLst.add(o);
        }
        se.setSamplesData(oLst);
    }
}
