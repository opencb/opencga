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

package org.opencb.opencga.storage.hadoop.variant.archive.mr;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static htsjdk.variant.vcf.VCFConstants.GENOTYPE_FILTER_KEY;
import static htsjdk.variant.vcf.VCFConstants.GENOTYPE_KEY;
import static org.opencb.biodata.models.variant.StudyEntry.FILTER;
import static org.opencb.biodata.models.variant.StudyEntry.QUAL;
import static org.opencb.biodata.models.variant.avro.VariantType.NO_VARIATION;

/**
 * Variant conflict resolver.
 *
 * Identifies and fixes overlapping variants from the same set of samples that may be inconsistent.
 *
 * 1. Remove duplicated
 * 2. Identify conflicts
 * 3. Resolve conflicts
 *
 * Created by mh719 on 10/06/2016.
 */
public class VariantLocalConflictResolver {

    private final Logger logger = LoggerFactory.getLogger(VariantLocalConflictResolver.class);

    private static final VariantComparator VARIANT_COMP = new VariantComparator();
    private static final VariantPositionComparator VARIANT_POSITION_COMPARATOR = new VariantPositionComparator();
    private static final PositionComparator POSITION_COMPARATOR = new PositionComparator();
    public static final String NOCALL = ".";

    public List<Variant> resolveConflicts(List<Variant> variants) {
        Map<Alternate, Variant> altToVar = removeDuplicatedAlts(variants);

        // reindex the other way
        IdentityHashMap<Variant, List<Alternate>> varToAlt = altToVar.entrySet().stream().collect(
                Collectors.groupingBy(
                        Map.Entry::getValue,
                        IdentityHashMap::new,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        // sort alts by position
        NavigableSet<Alternate> altSorted = new TreeSet<>();
//        altSorted.addAll(altToVar.keySet().stream().map(v -> v.getVariant()).collect(Collectors.toList()));
        altSorted.addAll(altToVar.keySet());

        List<Variant> resolved = new ArrayList<>();
        while (!altSorted.isEmpty()) {
            Alternate alternate = altSorted.first();
            Variant variant = altToVar.get(alternate);
            Set<Alternate> altConflictSet = findConflictAlternates(variant, altSorted, altToVar, varToAlt);

            Set<Variant> varConflicts;
            if (altConflictSet.size() == 1) {
                varConflicts = Collections.singleton(variant);
            } else {
                varConflicts = new HashSet<>();
                altConflictSet.forEach(a -> varConflicts.add(altToVar.get(a)));
            }

            if (varConflicts.isEmpty()) {
                throw new IllegalStateException("Variant didn't find itself: " + alternate);
            } else if (varConflicts.size() == 1) {
                if (!varConflicts.contains(variant)) {
                    throw new IllegalStateException("Variant didn't find itself, but others: " + variant);
                }
                resolved.add(variant);
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
    protected Map<Alternate, Variant> removeDuplicatedAlts(Collection<Variant> variants) {

        List<Variant> notFromSameCall = new ArrayList<>(variants.size());

        // Get all variants with multiple calls
        Multimap<String, Variant> callVariantsMap = ArrayListMultimap.create();
        for (Variant variant : variants) {
            String call = variant.getStudies().get(0).getFiles().get(0).getCall();
            if (variant.getType().equals(NO_VARIATION) || StringUtils.isEmpty(call)) {
                notFromSameCall.add(variant);
            } else {
                call = call.substring(0, call.lastIndexOf(':'));
                callVariantsMap.put(call, variant);
            }
        }

        // Remove all variants from the same call but one
        for (Map.Entry<String, Collection<Variant>> entry : callVariantsMap.asMap().entrySet()) {
            String call = entry.getKey();
            Collection<Variant> variantsFromSameCall = entry.getValue();
            if (!call.isEmpty() && variantsFromSameCall.size() > 1) {
                // Select the one with the lowest allele index
                List<Variant> sorted = new ArrayList<>(variantsFromSameCall);
                sorted.sort((v1, v2) -> {
                    String alleleIdx1 = v1.getStudies().get(0).getFiles().get(0).getCall().substring(call.length() + 1);
                    String alleleIdx2 = v2.getStudies().get(0).getFiles().get(0).getCall().substring(call.length() + 1);
                    return Integer.valueOf(alleleIdx1).compareTo(Integer.valueOf(alleleIdx2));
                });
                notFromSameCall.add(sorted.get(0));
                for (int i = 1; i < sorted.size(); i++) {
                    Variant discardedVariant = sorted.get(i);
                    logger.debug("Discarding variant {} ({}). Duplicated call {}", discardedVariant,
                            System.identityHashCode(discardedVariant), call);
                }
            } else {
                notFromSameCall.addAll(variantsFromSameCall);
            }
        }

        // Add all the alternates to the deDupMap. If already existing, add to the duplicated alternates map.
        Map<Alternate, Variant> deDupMap = new HashMap<>();
        Map<Alternate, List<Variant>> duplicated = new HashMap<>();
        for (Variant variant : notFromSameCall) {
            for (Alternate alternate : expandToVariants(variant)) {
                Variant old = deDupMap.put(alternate, variant);
                if (old != null) {
                    List<Variant> duplicatedVariants;
                    if (duplicated.containsKey(alternate)) {
                        duplicatedVariants = duplicated.get(alternate);
                    } else {
                        duplicatedVariants = new ArrayList<>();
                        duplicated.put(alternate, duplicatedVariants);
                        duplicatedVariants.add(old);
                    }
                    duplicatedVariants.add(variant);
                }
            }
        }

        // Get the best variant for duplicated variants
        for (Map.Entry<Alternate, List<Variant>> entry : duplicated.entrySet()) {
//            deDupMap.remove(entry.getKey());
            List<Variant> duplicatedVariants = entry.getValue();
            for (Variant duplicatedVariant : duplicatedVariants) {
                // Remove all the variants associated with this duplicated variant.
                // This loop should remove only one variant
                for (Alternate alternate : expandToVariants(duplicatedVariant)) {
                    Variant removed = deDupMap.remove(alternate);
                    if (removed != null) {
                        logger.debug("Discarded {}, ({})", removed, System.identityHashCode(removed));
                    }
                }
            }
            duplicatedVariants.sort(VARIANT_COMP);
            Variant variant = duplicatedVariants.get(0);
            deDupMap.put(entry.getKey(), variant);
            logger.debug("Replaced by {}, ({})", variant, System.identityHashCode(variant));
        }
        return deDupMap;
    }


    protected boolean isSamePosRefAlt(Variant query, Variant v) {
        return v.onSameRegion(query)
                && v.getReference().equals(query.getReference())
                && v.getAlternate().equals(query.getAlternate());
    }

    /**
     * Find all Alts overlapping each Alt of found Variants.
     * @param variant Variant object as in Vcf.
     * @param altSorted Alternates (as Variant objects) from all Variants (Alt and SecAlt) sorted by position.
     * @param altToVar Alternates to originating Variant object.
     * @param varToAlt Variant with a list of Alternates (Alt and SecAlts).
     * @return Set of conflicting Alts.
     */
    private Set<Alternate> findConflictAlternates(Variant variant, NavigableSet<Alternate> altSorted,
                                                  Map<Alternate, Variant> altToVar, Map<Variant, List<Alternate>> varToAlt) {

        // Get ALTs for Variant
        List<Alternate> alternates = varToAlt.get(variant);
        Collections.sort(alternates);

        altSorted.headSet(alternates.get(alternates.size() - 1), true);

        NavigableSet<Alternate> altConflicts = new TreeSet<>();
        altConflicts.addAll(altSorted.headSet(alternates.get(alternates.size() - 1), true));

        // While there are items in the sorted ALT list
        // OR there are no overlaps anymore
        NavigableSet<Alternate> remaining = new TreeSet<>();
        remaining.addAll(altSorted.tailSet(altConflicts.last(), false));
        while (!remaining.isEmpty()) {
            Alternate q = remaining.first();
            boolean hasOverlap = altConflicts.stream().filter(a -> hasConflictOverlap(a, q)).findAny().isPresent();
            if (!hasOverlap) {
                break; // END -> no overlaps.
            }
            altConflicts.add(q);
            // Get all ALTs from variant of ALT
            List<Alternate> qAlts = varToAlt.get(altToVar.get(q));
            Set<Alternate> altResolveList = new HashSet<>(qAlts);

            // Add everything which are lower (in sorting order) and their possible ALTs from same variant
            while (!altResolveList.isEmpty()) {
                List<Alternate> tmplst = new ArrayList<>(altResolveList);
                altResolveList.clear();
                for (Alternate toResolve : tmplst) {
                    if (altConflicts.contains(toResolve)) {
                        continue; //already in list
                    }
                    altConflicts.add(toResolve);

                    // Add all ALTs to the toResolve list, which are not conlicting yet.
                    altResolveList.addAll(remaining.headSet(toResolve, false).stream()
                            .flatMap(v -> varToAlt.get(altToVar.get(v)).stream())
                            .filter(v -> !altConflicts.contains(v)).collect(Collectors.toSet()));
                }
            }
            qAlts.forEach(altConflicts::add);
            remaining.clear();
            remaining.addAll(altSorted.tailSet(altConflicts.last(), false));
        }
        return altConflicts;
    }

    /**
     * Returns a nonredudant set of variants, where each position of the genome is only covered once.<br>
     * Conflicting regions are converted as NO_VARIATION.
     *
     * @param altConflicts Collection of Variants with conflicts.
     * @return List of Variants
     */
    private Collection<Variant> resolve(Set<Alternate> altConflicts, Collection<Variant> varConflicts, Map<Alternate, Variant> altToVar,
                                        IdentityHashMap<Variant, List<Alternate>> varToAlt) {
        if (varConflicts.size() < 2) {
            return varConflicts;
        }
        // <start,end> pair stream
        List<Pair<Integer, Integer>> secAltPairs = buildRegionsFromAlts(altConflicts);

        int min = secAltPairs.stream().mapToInt(Pair::getLeft).min().getAsInt();
        int max = secAltPairs.stream().mapToInt(Pair::getRight).max().getAsInt();

        List<Variant> sorted = new ArrayList<>(varConflicts);
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
                    List<Variant> collect = resolved.stream()
                            .filter(r -> r.overlapWith(query, true))
                            .collect(Collectors.toList());
                    collect.add(query);
                    List<Pair<Integer, Integer>> pairs = buildRegions(collect);
                    collect.sort(VARIANT_POSITION_COMPARATOR);
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
        targetReg.sort(POSITION_COMPARATOR);
//        target.sort(varPositionOrder);

        int min = start;
        int max = end;
        if (max < min) {
            // Insertion -> no need for holes
            return Collections.emptyList();
        }
        int minTarget = targetReg.stream().mapToInt(Pair::getLeft).min().getAsInt();
        int maxTarget = targetReg.stream().mapToInt(Pair::getRight).max().getAsInt();
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

    private static List<Pair<Integer, Integer>> buildRegionsFromAlts(Collection<Alternate> target) {
        return target.stream().map(a -> buildRegions(a.getVariant())).collect(Collectors.toList());
    }

    private static List<Pair<Integer, Integer>> buildRegions(Collection<Variant> target) {
        return target.stream().map(VariantLocalConflictResolver::buildRegions).collect(Collectors.toList());
    }

    private static Pair<Integer, Integer> buildRegions(Variant v) {
        return new ImmutablePair<>(v.getStart(), v.getEnd());
    }

    public static boolean hasAnyConflictOverlapInclSecAlt(List<Variant> target, Variant query, Map<Variant, List<Alternate>> varToAlt) {
        return target.stream()
                .filter(v -> hasAnyConflictOverlapInclSecAlt(v, query, varToAlt))
                .findAny()
                .isPresent();
    }

    public static boolean hasAnyConflictOverlapInclSecAlt(Variant a, Variant b, Map<Variant, List<Alternate>> varToAlt) {
        // Check Direct overlap
        if (hasConflictOverlap(a, b)) {
            return true;
        }
        // Check AltCoords as well
        List<Alternate> aList = varToAlt.get(a);
        List<Alternate> bList = varToAlt.get(b);

        if (aList.size() == 1 && bList.size() == 1) {
            return false; // No Secondary alternates in the list -> no possible overlaps.
        }
        if (aList.size() == 1) {
            Alternate av = aList.get(0);
            return bList.stream().filter(bv -> hasConflictOverlap(av, bv)).findAny().isPresent();
        }
        if (bList.size() == 1) {
            Alternate bv = bList.get(0);
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
    public static List<Alternate> expandToVariants(Variant v) {
        Alternate mainVariant = new Alternate(asVariant(v));
        if (v.getStudies().isEmpty()) {
            return Collections.singletonList(mainVariant);
        }
        List<AlternateCoordinate> secondaryAlternates = v.getStudies().get(0).getSecondaryAlternates();
        if (secondaryAlternates.isEmpty()) {
            return Collections.singletonList(mainVariant);
        }
        // Check AltCoords as well
        List<Alternate> list = new ArrayList<>(1 + secondaryAlternates.size());
        list.add(mainVariant);
        secondaryAlternates.forEach(alt -> list.add(new Alternate(asVariant(v, alt))));
        return list;
    }

    private static boolean hasConflictOverlap(Alternate a, Alternate b) {
        return hasConflictOverlap(a.getVariant(), b.getVariant());
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
        Variant variant = new Variant(v.getChromosome(), v.getStart(), v.getEnd(), v.getReference(), v.getAlternate(), v.getStrand());
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
        return target.stream().mapToInt(Variant::getStart).min().getAsInt();
    }

    public static int max(List<Variant> target) {
        return target.stream().mapToInt(Variant::getEnd).max().getAsInt();
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

        Variant v = new Variant(var.getChromosome(), var.getStart(), var.getEnd(), var.getReference(), var.getAlternate());
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
        String genotype = NOCALL;
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

    // Alternate variant. Just a plain variant
    public static class Alternate implements Comparable<Alternate> {
        private final Variant variant;

        public Alternate(Variant variant) {
            this.variant = variant;
//            assert variant.getStudies().isEmpty();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Alternate)) {
                return false;
            }
            Alternate that = (Alternate) o;
            if (variant == null) {
                if (that.variant == null) {
                    return true;
                }
                return false;
            }
            return compare(variant, that.variant) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(variant.getStart(), variant.getEnd(), variant.getReference(), variant.getAlternate(), variant.getType());
//            return variant != null ? variant.toString().hashCode() : 0;
        }

        @Override
        public String toString() {
            return variant.toString();
        }

        public Variant getVariant() {
            return variant;
        }

        @Override
        public int compareTo(Alternate that) {
            return compare(variant, that.variant);
        }

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
            return 0;
//            return Integer.compare(o1.hashCode(), o2.hashCode());
        }
    }
}
