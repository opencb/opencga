package org.opencb.opencga.storage.core.variant.adaptors;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.core.Every;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.QueryResult;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 05/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMatchers {

    public static <T> FeatureMatcher<QueryResult<T>, List<T>> hasResult(Matcher<? super List<T>> subMatcher) {
        return new FeatureMatcher<QueryResult<T>, List<T>>(subMatcher, "A query result where", "QueryResult") {
            @Override
            protected List<T> featureValueOf(QueryResult<T> actual) {
                return actual.getResult();
            }
        };
    }

    public static <T> Matcher<QueryResult<T>> everyResult(Matcher<T> subMatcher) {
        return hasResult(Every.everyItem(subMatcher));
    }

    public static Matcher<QueryResult<?>> numTotalResults(Matcher<Long> subMatcher) {
        return new FeatureMatcher<QueryResult<?>, Long>(subMatcher, "QueryResult with NumTotalResults", "NumTotalResults") {
            @Override
            protected Long featureValueOf(QueryResult<?> actual) {
                return actual.getNumTotalResults();
            }
        };
    }
    public static Matcher<QueryResult<?>> numResults(Matcher<Integer> subMatcher) {
        return new FeatureMatcher<QueryResult<?>, Integer>(subMatcher, "QueryResult with NumResults", "NumResults") {
            @Override
            protected Integer featureValueOf(QueryResult<?> actual) {
                return actual.getNumResults();
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
