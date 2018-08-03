package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.IndividualMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.SampleMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.Annotable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

public class IndividualMongoDBIterator<E> extends AnnotableMongoDBIterator<E> {

    private Logger logger;
    private long studyUid;
    private String user;

    private SampleDBAdaptor sampleDBAdaptor;
    private QueryOptions sampleQueryOptions;

    private Queue<Document> individualListBuffer;
    private static final int BUFFER_SIZE = 100;

    public IndividualMongoDBIterator(MongoCursor mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                 Function<Document, Document> filter, SampleMongoDBAdaptor sampleMongoDBAdaptor,
                                 long studyUid, String user, QueryOptions options) {
        super(mongoCursor, converter, filter, options);

        this.user = user;
        this.studyUid = studyUid;

        this.sampleDBAdaptor = sampleMongoDBAdaptor;
        this.sampleQueryOptions = generateSampleQueryOptions();

        this.individualListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(IndividualMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = getNext();

        if (filter != null) {
            next = filter.apply(next);
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }

    @Override
    public boolean hasNext() {
        if (individualListBuffer.isEmpty()) {
            refillIndividualBuffer();
        }
        return !individualListBuffer.isEmpty();
    }

    private Document getNext() {
        return individualListBuffer.remove();
    }

    private void refillIndividualBuffer() {
        Set<String> sampleVersions = new HashSet<>();

        // Get next 100 documents
        int cont = 0;
        while (mongoCursor.hasNext() && cont < BUFFER_SIZE) {
            Document next = (Document) mongoCursor.next();

            individualListBuffer.add(next);
            cont++;

            // Extract all the samples
            Object samples = next.get(IndividualMongoDBAdaptor.QueryParams.SAMPLES.key());
            if (samples != null) {
                List<Document> sampleList = (List<Document>) samples;
                if (!sampleList.isEmpty()) {
                    sampleList.forEach(s -> {
                        String uid = String.valueOf(s.get("uid"));
                        String version = String.valueOf(s.get("version"));

                        sampleVersions.add(uid + "__" + version);
                    });
                }
            }
        }

        if (!sampleVersions.isEmpty()) {
            // Obtain all those samples

            List<Long> uidList = new ArrayList<>(sampleVersions.size());
            List<Integer> versionList = new ArrayList<>(sampleVersions.size());
            sampleVersions.forEach(s -> {
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "__");
                uidList.add(Long.valueOf(split[0]));
                versionList.add(Integer.valueOf(split[1]));
            });

            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(SampleDBAdaptor.QueryParams.UID.key(), uidList)
                    .append(SampleDBAdaptor.QueryParams.VERSION.key(), versionList);
            List<Document> sampleList;
            try {
                if (user != null) {
                    sampleList = sampleDBAdaptor.nativeGet(query, sampleQueryOptions, user).getResult();
                } else {
                    sampleList = sampleDBAdaptor.nativeGet(query, sampleQueryOptions).getResult();
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the samples associated to the individuals: {}", e.getMessage(), e);
                return;
            }

            // Map each sample uid - version to the sample entry
            Map<String, Document> sampleMap = new HashMap<>(sampleList.size());
            sampleList.forEach(s -> sampleMap.put(String.valueOf(s.get("uid")) + "__" + String.valueOf(s.get("version")), s));

            // Add the samples obtained to the corresponding individuals
            individualListBuffer.forEach(i -> {
                List<Document> tmpSampleList = new ArrayList<>();
                List<Document> samples = (List<Document>) i.get(IndividualMongoDBAdaptor.QueryParams.SAMPLES.key());

                samples.forEach(s -> {
                        String uid = String.valueOf(s.get("uid"));
                        String version = String.valueOf(s.get("version"));
                        String key = uid + "__" + version;

                        // If the samples has been returned... (it might have not been fetched due to permissions issues)
                        if (sampleMap.containsKey(key)) {
                            tmpSampleList.add(sampleMap.get(key));
                        }
                });

                i.put(IndividualMongoDBAdaptor.QueryParams.SAMPLES.key(), tmpSampleList);
            });
        }
    }

    private QueryOptions generateSampleQueryOptions() {
        QueryOptions queryOptions = new QueryOptions("lazy", true);

        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            List<String> includeList = new ArrayList<>();
            for (String include : currentIncludeList) {
                if (include.startsWith(IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".")) {
                    includeList.add(include.replace(IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".", ""));
                }
            }
            if (!includeList.isEmpty()) {
                includeList.add("version");
                includeList.add("uid");
                queryOptions.put(QueryOptions.INCLUDE, includeList);
            }
        }
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> currentExcludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            List<String> excludeList = new ArrayList<>();
            for (String exclude : currentExcludeList) {
                if (exclude.startsWith(IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".")) {
                    String replace = exclude.replace(IndividualDBAdaptor.QueryParams.SAMPLES.key() + ".", "");
                    if (!"version".equals(replace) && !"uid".equals(replace)) {
                        excludeList.add(replace);
                    }
                }
            }
            if (!excludeList.isEmpty()) {
                queryOptions.put(QueryOptions.EXCLUDE, excludeList);
            }
        }
        if (options.containsKey(Constants.FLATTENED_ANNOTATIONS)) {
            queryOptions.put(Constants.FLATTENED_ANNOTATIONS, options.getBoolean(Constants.FLATTENED_ANNOTATIONS));
        }

        return queryOptions;
    }

}
