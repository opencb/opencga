package org.opencb.opencga.storage.core.variant.adaptors;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.core.Every;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.QueryResult;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;

/**
 * Created on 05/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMatchers {

    public static <T> FeatureMatcher<QueryResult<T>, List<T>> hasResult(Matcher<? super List<T>> subMatcher) {
        return new FeatureMatcher<QueryResult<T>, List<T>>(subMatcher, "a query result where", "QueryResult") {
            @Override
            protected List<T> featureValueOf(QueryResult<T> actual) {
                return actual.getResult();
            }
        };
    }

    public static <T> Matcher<QueryResult<T>> everyResult(QueryResult<T> allValues, Matcher<T> subMatcher) {
        return everyResult(allValues.getResult(), subMatcher);
    }

    public static <T> Matcher<QueryResult<T>> everyResult(List<T> allValues, Matcher<T> subMatcher) {
        long count = count(allValues, subMatcher);
        return allOf(everyResult(subMatcher), numResults(is((int) count)));
    }

    public static <T> Matcher<QueryResult<T>> everyResult(Matcher<T> subMatcher) {
        return hasResult(Every.everyItem(subMatcher));
    }

    public static Matcher<QueryResult<?>> numTotalResults(Matcher<Long> subMatcher) {
        return new FeatureMatcher<QueryResult<?>, Long>(subMatcher, "a queryResult with numTotalResults", "NumTotalResults") {
            @Override
            protected Long featureValueOf(QueryResult<?> actual) {
                return actual.getNumTotalResults();
            }
        };
    }
    public static Matcher<QueryResult<?>> numResults(Matcher<Integer> subMatcher) {
        return new FeatureMatcher<QueryResult<?>, Integer>(subMatcher, "a queryResult with numResults", "NumResults") {
            @Override
            protected Integer featureValueOf(QueryResult<?> actual) {
                return actual.getNumResults();
            }
        };
    }

    public static Matcher<Variant> overlaps(Region region) {
        return overlaps(region, true);
    }

    public static Matcher<Variant> overlaps(final Region region, final boolean inclusive) {
        return new TypeSafeDiagnosingMatcher<Variant>() {
            @Override
            protected boolean matchesSafely(Variant item, Description mismatchDescription) {
                return item.overlapWith(region.getChromosome(), region.getStart(), region.getEnd(), inclusive);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("overlaps with " + region + (inclusive? "inclusively" : "non inclusively"));
            }
        };
    }

    public static Matcher<Variant> hasAnnotation(Matcher<? super VariantAnnotation> subMatcher) {
        return new FeatureMatcher<Variant, VariantAnnotation>(subMatcher, "with variant annotation", "VariantAnnotation") {
            @Override
            protected VariantAnnotation featureValueOf(Variant actual) {
                return actual.getAnnotation();
            }
        };
    }

    public static Matcher<VariantAnnotation> hasGenes(Matcher<? super Collection<String>> subMatcher) {
        return new FeatureMatcher<VariantAnnotation, Collection<String>>(subMatcher, "with genes", "annotation") {
            @Override
            protected Collection<String> featureValueOf(VariantAnnotation actual) {
                return actual.getConsequenceTypes().stream().map(ConsequenceType::getGeneName).collect(Collectors.toList());
            }
        };
    }

    public static Matcher<VariantAnnotation> hasGenes(Collection<String> genes) {
        return hasGenes(hasItems(genes.toArray(new String[genes.size()])));
    }

    public static Matcher<VariantAnnotation> hasAnyGeneOf(Collection<String> genes) {
        LinkedList<Matcher<? super Collection<String>>> any = new LinkedList<>();
        for (String gene : genes) {
            any.add(hasItem(gene));
        }
        return hasGenes(anyOf(any));
    }

    public static Matcher<VariantAnnotation> hasSO(Matcher<? super Collection<String>> subMatcher) {
        return new FeatureMatcher<VariantAnnotation, Collection<String>>(subMatcher, "with Sequence Ontology Terms", "SOTerms") {
            @Override
            protected Collection<String> featureValueOf(VariantAnnotation actual) {
                return actual.getConsequenceTypes()
                        .stream()
                        .map(ConsequenceType::getSequenceOntologyTerms)
                        .flatMap(Collection::stream)
                        .map(SequenceOntologyTerm::getAccession)
                        .collect(Collectors.toSet());
            }
        };
    }

    public static Matcher<VariantAnnotation> hasPopAltFreq(String study, String population, Matcher<? super Float> subMatcher) {
        return new FeatureMatcher<VariantAnnotation, Float>(subMatcher, "with Population alternate allele Frequency (" + study + ", " + population + ")", "PopulationAltFreq") {
            @Override
            protected Float featureValueOf(VariantAnnotation actual) {
                if (actual.getPopulationFrequencies() != null) {
                    for (PopulationFrequency populationFrequency : actual.getPopulationFrequencies()) {
                        if (populationFrequency.getStudy().equalsIgnoreCase(study)
                                && populationFrequency.getPopulation().equalsIgnoreCase(population)) {
                            return populationFrequency.getAltAlleleFreq();
                        }
                    }
                }
                return 0F;
            }
        };
    }

    public static Matcher<VariantAnnotation> hasPopRefFreq(String study, String population, Matcher<? super Float> subMatcher) {
        return new FeatureMatcher<VariantAnnotation, Float>(subMatcher, "with Population reference allele Frequency (" + study + ", " + population + ")", "PopulationRefFreq") {
            @Override
            protected Float featureValueOf(VariantAnnotation actual) {
                if (actual.getPopulationFrequencies() != null) {
                    for (PopulationFrequency populationFrequency : actual.getPopulationFrequencies()) {
                        if (populationFrequency.getStudy().equalsIgnoreCase(study)
                                && populationFrequency.getPopulation().equalsIgnoreCase(population)) {
                            return populationFrequency.getRefAlleleFreq();
                        }
                    }
                }
                return 0F;
            }
        };
    }

    public static Matcher<Variant> firstStudy(Matcher<? super StudyEntry> subMatcher) {
        return new FeatureMatcher<Variant, StudyEntry>(subMatcher, "with first study", "Study") {
            @Override
            protected StudyEntry featureValueOf(Variant actual) {
                return actual.getStudies().get(0);
            }
        };
    }

    public static Matcher<Variant> withStudy(final String study, Matcher<? super StudyEntry> subMatcher) {
        return new FeatureMatcher<Variant, StudyEntry>(subMatcher, "with study " + study, "Study") {
            @Override
            protected StudyEntry featureValueOf(Variant actual) {
                return actual.getStudy(study);
            }
        };
    }

    public static Matcher<StudyEntry> withStats(final String cohortName, Matcher<? super VariantStats> subMatcher) {
        return new FeatureMatcher<StudyEntry, VariantStats>(subMatcher, "with stats " + cohortName, "Stats") {
            @Override
            protected VariantStats featureValueOf(StudyEntry actual) {
                return actual.getStats(cohortName);
            }
        };
    }

    public static Matcher<VariantStats> withMaf(Matcher<? super Float> subMatcher) {
        return new FeatureMatcher<VariantStats, Float>(subMatcher, "with maf", "MAF") {
            @Override
            protected Float featureValueOf(VariantStats actual) {
                return actual.getMaf();
            }
        };
    }

    public static Matcher<VariantStats> withMgf(Matcher<? super Float> subMatcher) {
        return new FeatureMatcher<VariantStats, Float>(subMatcher, "with mgf", "MGF") {
            @Override
            protected Float featureValueOf(VariantStats actual) {
                return actual.getMgf();
            }
        };
    }

    public static <T extends Number> Matcher<T> gt(T n) {
        return new TypeSafeDiagnosingMatcher<T>() {
            @Override
            protected boolean matchesSafely(T item, Description mismatchDescription) {
                if (item.doubleValue() > n.doubleValue()) {
                    return true;
                } else {
                    mismatchDescription.appendText("is " + item);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("> " + n);
            }
        };
    }

    public static <T extends Number> Matcher<T> gte(T n) {
        return new TypeSafeDiagnosingMatcher<T>() {
            @Override
            protected boolean matchesSafely(T item, Description mismatchDescription) {
                if (item.doubleValue() >= n.doubleValue()) {
                    return true;
                } else {
                    mismatchDescription.appendText("is " + item);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(">= " + n);
            }
        };
    }

    public static <T extends Number> Matcher<T> lt(T n) {
        return new TypeSafeDiagnosingMatcher<T>() {
            @Override
            protected boolean matchesSafely(T item, Description mismatchDescription) {
                if (item.doubleValue() < n.doubleValue()) {
                    return true;
                } else {
                    mismatchDescription.appendText(" is " + item);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("< " + n);
            }
        };
    }

    public static <T extends Number> Matcher<T> lte(T n) {
        return new TypeSafeDiagnosingMatcher<T>() {
            @Override
            protected boolean matchesSafely(T item, Description mismatchDescription) {
                if (item.doubleValue() <= n.doubleValue()) {
                    return true;
                } else {
                    mismatchDescription.appendText(" is " + item);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("<= " + n);
            }
        };
    }

    public static <T> long count(List<T> objects, Matcher<T> matcher) {
        long c = 0;
        for (T t: objects) {
            if (matcher.matches(t)) {
                c++;
            }
        }
        return c;
    }

//    private static class VariantVariantAnnotationFeatureMatcher extends FeatureMatcher<Variant, VariantAnnotation> {
//        /**
//         * Constructor
//         *
//         * @param subMatcher         The matcher to apply to the feature
//         */
//        public VariantVariantAnnotationFeatureMatcher(Matcher<? super VariantAnnotation> subMatcher) {
//            super(subMatcher, "variant annotation", "annotation");
//        }
//
//        @Override
//        protected VariantAnnotation featureValueOf(Variant actual) {
//            return actual.getAnnotation();
//        }
//
//        public static VariantVariantAnnotationFeatureMatcher hasAnnotation(Matcher<? super VariantAnnotation> subMatcher) {
//            return new VariantVariantAnnotationFeatureMatcher(subMatcher);
//        }
//    }
}
