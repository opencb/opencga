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


    private static VariantPositionRefAltComparator variantPositionRefAltComparator = new VariantPositionRefAltComparator();
    public List<Variant> resolveConflicts(List<Variant> variants) {
        Map<AlternateWrapper, Variant> altToVar = removeDuplicatedAlts(variants);

        // reindex the other way
        Map<Variant, List<Variant>> varToAlt = altToVar.entrySet().stream().collect(
                Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(e -> e.getKey().getVariant(), Collectors.toList())));
        // sort alts by position
        NavigableSet<Variant> altSorted = new TreeSet<>(variantPositionRefAltComparator);
        altSorted.addAll(altToVar.keySet().stream().map(v -> v.getVariant()).collect(Collectors.toList()));

        List<Variant> resolved = new ArrayList<>();
        while (!altSorted.isEmpty()) {
            Variant altQuery = altSorted.first();
            Variant varQuery = altToVar.get(new AlternateWrapper(altQuery));
            Set<Variant> altConflictSet = findConflictAlternates(varQuery, altSorted, altToVar, varToAlt);

            Set<Variant> varConflicts = new HashSet<>();
            altConflictSet.forEach(a -> varConflicts.add(altToVar.get(new AlternateWrapper(a))));

            if (varConflicts.isEmpty()) {
                throw new IllegalStateException("Variant didn't find itself: " + altQuery);
            } else if (varConflicts.size() == 1) {
                if (!varConflicts.contains(varQuery)) {
                    throw new IllegalStateException("Variant didn't find itself, but others: " + varQuery);
                }
                resolved.add(varQuery);
            } else {
                Collection<Variant> varResolved = resolve(altConflictSet, varConflicts, altToVar, varToAlt);
                resolved.addAll(varResolved);
            }
            altSorted.removeAll(altConflictSet);
        }
        return resolved;
    }

    /**
     * Remove a variant, if the same pos:ref:alt is already represented in a SecAlt variant.
     *
     * @param variants Collection of Alts as Variant objects.
     * @return Map Alternate (SecAlt and Alt as variant) to originating Variant.
     */
    protected Map<AlternateWrapper, Variant> removeDuplicatedAlts(Collection<Variant> variants) {
        // Remove redundant variants (variant with one alt already represented in a SecAlt)
        Map<AlternateWrapper, List<Variant>> altToVarList = variants.stream()
                .flatMap(v -> expandToVariants(v).stream().map(e -> new ImmutablePair<>(new AlternateWrapper(e), v)))
                .collect(Collectors.groupingBy(Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toList())));
        // reindex the other way
        Map<Variant, Set<AlternateWrapper>> varToAlt =
                altToVarList.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(v -> new ImmutablePair<>(v, e.getKey())))
                .collect(
                Collectors.groupingBy(
                        Pair::getKey,
                        Collectors.mapping(Pair::getValue, Collectors.toSet())));

        Map<AlternateWrapper, Variant> resMap = new HashMap<>();
        Set<AlternateWrapper> altBlackList = new HashSet<>();

         altToVarList.entrySet().forEach(e -> {
             AlternateWrapper key = e.getKey();
             if (altBlackList.contains(key)) {
                 return; // ignore
             }
             List<Variant> lst = e.getValue();
             if (lst.size() > 1) {
                 // remove exact duplicated call
                 Set<Set<AlternateWrapper>> duplicatedCheck = new HashSet<>();
                 List<Variant> uniqCalls = new ArrayList<>();
                 lst.forEach(v -> {
                     if (duplicatedCheck.add(varToAlt.get(v))) {
                         uniqCalls.add(v);
                     }
                 });
                 lst = uniqCalls;
             }
             // conflict
             while (lst.size() > 1) {
                 // remove calls with no sec-alt
                 Optional<Variant> any = lst.stream().filter(
                         v -> isSamePosRefAlt(v, key.variant)
                                 && v.getStudies().get(0).getSecondaryAlternates().isEmpty()).findAny();
                 if (any.isPresent()) {
                     lst.remove(any.get());
                     continue; // removed one variant with no sec-alt
                 }
                 // >1 variant with sec-alt ...
                 // chose 'best' call, remove other.
                 Collections.sort(lst, VARIANT_COMP);
                 lst.forEach(v -> altBlackList.addAll(varToAlt.get(v)));
                 lst = Collections.singletonList(lst.get(0));
                 altBlackList.removeAll(varToAlt.get(lst.get(0)));
             }
             resMap.put(key, lst.get(0));
         });
        return resMap;
    }


    protected boolean isSamePosRefAlt(Variant query, Variant v) {
        return v.onSameRegion(query)
                && v.getReference().equals(query.getReference())
                && v.getAlternate().equals(query.getAlternate());
    }

    /**
     * Find all Alts overlapping each Alt of found Variants.
     * @param varQuery Variant object as in Vcf.
     * @param altSorted Alternates (as Variant objects) from all Variants (Alt and SecAlt) sorted by position.
     * @param altToVar Alternates to originating Variant object.
     * @param varToAlt Variant with a list of Alternates (Alt and SecAlts).
     * @return Set of conflicting Alts.
     */
    public static Set<Variant> findConflictAlternates(Variant varQuery, NavigableSet<Variant> altSorted,
                             Map<AlternateWrapper, Variant> altToVar, Map<Variant, List<Variant>> varToAlt) {

        // Get ALTs for Variant
        List<Variant> altQueryLst = varToAlt.get(varQuery);
        Collections.sort(altQueryLst, variantPositionRefAltComparator);

        altSorted.headSet(altQueryLst.get(altQueryLst.size() - 1), true);

        NavigableSet<Variant> altConflicts = new TreeSet<>(variantPositionRefAltComparator);
        altConflicts.addAll(altSorted.headSet(altQueryLst.get(altQueryLst.size() - 1), true));

        // While there are items in the sorted ALT list
        // OR there are no overlaps anymore
        NavigableSet<Variant> remaining = new TreeSet<>(variantPositionRefAltComparator);
        remaining.addAll(altSorted.tailSet(altConflicts.last(), false));
        while (!remaining.isEmpty()) {
            Variant q = remaining.first();
            boolean hasOverlap = altConflicts.stream().filter(a -> hasConflictOverlap(a, q)).findAny().isPresent();
            if (!hasOverlap) {
                break; // END -> no overlaps.
            }
            altConflicts.add(q);
            // Get all ALTs from variant of ALT
            List<Variant> qAlts =
                    varToAlt.get(
                            altToVar.get(new AlternateWrapper(q)));
            Set<Variant> altResolveList = new HashSet<>(qAlts);

            // Add everything which are lower (in sorting order) and their possible ALTs from same variant
            while (!altResolveList.isEmpty()) {
                List<Variant> tmplst = new ArrayList<>(altResolveList);
                altResolveList.clear();
                for (Variant toResolve : tmplst) {
                    if (altConflicts.contains(toResolve)) {
                        continue; //already in list
                    }
                    altConflicts.add(toResolve);

                    // Add all ALTs to the toResolve list, which are not conlicting yet.
                    altResolveList.addAll(remaining.headSet(toResolve, false).stream()
                            .flatMap(v -> varToAlt.get(altToVar.get(new AlternateWrapper(v))).stream())
                            .filter(v -> !altConflicts.contains(v)).collect(Collectors.toSet()));
                }
            }
            altConflicts.addAll(qAlts);
            remaining.clear();
            remaining.addAll(altSorted.tailSet(altConflicts.last(), false));
        }
        return altConflicts;
    }

    /**
     * Returns a nonredudant set of variants, where each position of the genome is only covered once.<br>
     * Conflicting regions are converted as NO_VARIATION.
     *
     * @param altConf Collection of Variants with conflicts.
     * @return List of Variants
     */
    private Collection<Variant> resolve(Set<Variant> altConf, Set<Variant> varConf, Map<AlternateWrapper, Variant> altToVar,
                                        Map<Variant, List<Variant>> varToAlt) {
        if (varConf.size() < 2) {
            return varConf;
        }
        // <start,end> pair stream
        List<Pair<Integer, Integer>> secAltPairs = buildRegions(altConf);

        int min = secAltPairs.stream().mapToInt(p -> p.getLeft()).min().getAsInt();
        int max = secAltPairs.stream().mapToInt(p -> p.getRight()).max().getAsInt();

        List<Variant> sorted = new ArrayList<>(varConf);
        sorted.sort(VARIANT_COMP);

        List<Variant> resolved = new ArrayList<>();
        List<Variant> misfit = new ArrayList<>();
        for (Variant query : sorted) {
            if (resolved.isEmpty()) {
                resolved.add(query);
            } else {
                if (!hasAnyConflictOverlapInclSecAlt(resolved, query, varToAlt)) {
                    resolved.add(query);
                } else if (allSameTypeAndGT(resolved, VariantType.NO_VARIATION)) {
                    List<Variant> collect = resolved.stream().filter(r -> r.overlapWith(query, true))
                            .collect(Collectors.toList());
                    collect.add(query);
                    List<Pair<Integer, Integer>> pairs = buildRegions(collect);
                    collect.sort(varPositionOrder);
                    Variant variant = deepCopy(collect.get(0));
                    int minPos = pairs.stream().mapToInt(p -> p.getLeft()).min().getAsInt();
                    if (!variant.getStart().equals(minPos)) {
                        throw new IllegalStateException("Sorting and merging of NO_VARIATOIN regions went wrong: " + query);
                    }
                    variant.setEnd(pairs.stream().mapToInt(p -> p.getRight()).max().getAsInt());
                    variant.setLength((variant.getEnd() - variant.getStart()) + 1);
                    resolved.clear();
                    resolved.add(variant);
                } else {
                    // does not fit
                    misfit.add(query);
                }
            }
        }
        if (!misfit.isEmpty()) {
            // fit into place (before and after)
            fillNoCall(resolved, misfit.get(0), min, max);
        }
        return resolved;
    }

    private boolean allSameTypeAndGT(List<Variant> conflicts, Variant query, VariantType type) {
        List<Variant> tmp = new ArrayList<>(conflicts.size() + 1);
        tmp.addAll(conflicts);
        tmp.add(query);
        return allSameTypeAndGT(tmp, type);
    }
    private boolean allSameTypeAndGT(Collection<Variant> conflicts, VariantType type) {
        boolean differentType = conflicts.stream().filter(v -> !v.getType().equals(type)).findAny().isPresent();
        if (differentType) {
            return false;
        }

        StudyEntry studyEntry = conflicts.stream().findAny().get().getStudies().get(0);
        String sample = studyEntry.getSamplesName().stream().findFirst().get();

        String gt = studyEntry.getSampleData(sample, GENOTYPE_KEY);
        long count = conflicts.stream().filter(v -> v.getType().equals(type)
                && StringUtils.equals(gt, v.getStudies().get(0).getSampleData(sample, GENOTYPE_KEY))).count();
        return ((int) count) == conflicts.size();
    }

    private static VariantPositionComparator varPositionOrder = new VariantPositionComparator();
    private static PositionComparator positionOrder = new PositionComparator();

    private void fillNoCall(List<Variant> resolved, Variant q, int start, int end) {

        // find missing pieces
        List<Pair<Integer, Integer>> holes = getMissingRegions(resolved, start, end);

        for (Pair<Integer, Integer> h : holes) { // > 1 hole -> need to make copies of variant object
            Variant v = deepCopy(q);
            changeVariantToNoCall(v, h.getKey(), h.getValue());
            resolved.add(v);
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
            return Arrays.asList(
                    new ImmutablePair<>(Math.min(min, maxTarget), Math.max(min, maxTarget)),
                    new ImmutablePair<>(Math.min(minTarget, max), Math.max(minTarget, max)));
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

    private static List<Pair<Integer, Integer>> buildRegions(Collection<Variant> target) {
        return target.stream().map(v -> buildRegions(v)).collect(Collectors.toList());
    }

    private static Pair<Integer, Integer> buildRegions(Variant v) {
        return new ImmutablePair<>(v.getStart(), v.getEnd());
    }

    public static boolean hasAnyConflictOverlapInclSecAlt(List<Variant> target, Variant query, Map<Variant, List<Variant>> varToAlt) {
        return target.stream().filter(v -> hasAnyConflictOverlapInclSecAlt(v, query, varToAlt)).findAny().isPresent();
    }

    public static boolean hasAnyConflictOverlapInclSecAlt(Variant a, Variant b, Map<Variant, List<Variant>> varToAlt) {
        // Check Direct overlap
        if (hasConflictOverlap(a, b)) {
            return true;
        }
        // Check AltCoords as well
        List<Variant> aList = varToAlt.get(a);
        List<Variant> bList = varToAlt.get(b);

        if (aList.size() == 1 && bList.size() == 1) {
            return false; // No Secondary alternates in the list -> no possible overlaps.
        }
        if (aList.size() == 1) {
            Variant av = aList.get(0);
            return bList.stream().filter(bv -> hasConflictOverlap(av, bv)).findAny().isPresent();
        }
        if (bList.size() == 1) {
            Variant bv = bList.get(0);
            return aList.stream().filter(av -> hasConflictOverlap(av, bv)).findAny().isPresent();
        }
        // Search for any overlap between both lists
        return aList.stream().filter(av -> bList.stream().filter(bv -> hasConflictOverlap(av, bv)).findAny()
                .isPresent()).findAny().isPresent();
    }

    /**
     * Creates a list with the provided Variant and all secondary alternates {@link AlternateCoordinate} converted to
     * Variants.
     * @param v {@link Variant}
     * @return List of Variant positions.
     */
    public static List<Variant> expandToVariants(Variant v) {
        Variant nv = asVariant(v);
        if (v.getStudies().isEmpty()) {
            return Collections.singletonList(nv);
        }
        StudyEntry studyEntry = v.getStudies().get(0);
        List<AlternateCoordinate> secAlt = studyEntry.getSecondaryAlternates();
        if (secAlt.isEmpty()) {
            return Collections.singletonList(nv);
        }
        // Check AltCoords as well
        List<Variant> list = new ArrayList<>(Collections.singletonList(nv));
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

    public static Variant asVariant(Variant v) {
        Variant variant =
                new Variant(v.getChromosome(), v.getStart(), v.getEnd(), v.getReference(), v.getAlternate(), v.getStrand());
        variant.setType(v.getType());
        return variant;
    }

    public static Variant asVariant(Variant a, AlternateCoordinate altA) {
        String chr = ObjectUtils.firstNonNull(altA.getChromosome(), a.getChromosome());
        Integer start  = ObjectUtils.firstNonNull(altA.getStart(), a.getStart());
        Integer end  = ObjectUtils.firstNonNull(altA.getEnd(), a.getEnd());
        String ref  = ObjectUtils.firstNonNull(altA.getReference(), a.getReference());
        String alt  = ObjectUtils.firstNonNull(altA.getAlternate(), a.getAlternate());
        VariantType type = ObjectUtils.firstNonNull(altA.getType(), a.getType());
        try {
            Variant variant = new Variant(chr, start, end, ref, alt);
            variant.setType(type);
            return variant;
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
            c = o1.getType().compareTo(o2.getType());
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
        v.setType(var.getType());
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

    public static class AlternateWrapper {
        private final Variant variant;

        public AlternateWrapper(Variant variant) {
            this.variant = variant;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AlternateWrapper)) {
                return false;
            }
            AlternateWrapper that = (AlternateWrapper) o;
            if (variant == null) {
                if (that.variant == null) {
                    return true;
                }
                return false;
            }
            return variantPositionRefAltComparator.compare(variant, that.variant) == 0;
        }

        @Override
        public int hashCode() {
            return variant != null ? variant.toString().hashCode() : 0;
        }

        public Variant getVariant() {
            return variant;
        }
    }
}
