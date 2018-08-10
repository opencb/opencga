package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FamilyMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.Annotable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class FamilyMongoDBIterator<E> extends AnnotableMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private IndividualDBAdaptor individualDBAdaptor;
    private QueryOptions individualQueryOptions;

    private Queue<Document> familyListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;


    public FamilyMongoDBIterator(MongoCursor mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                 Function<Document, Document> filter, IndividualDBAdaptor individualDBAdaptor,
                                 long studyUid, String user, QueryOptions options) {
        super(mongoCursor, converter, filter, options);

        this.user = user;
        this.studyUid = studyUid;

        this.individualDBAdaptor = individualDBAdaptor;
        this.individualQueryOptions = createFamilyQueryOptions();

        this.familyListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(FamilyMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = familyListBuffer.remove();

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
        if (familyListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !familyListBuffer.isEmpty();
    }

    private void fetchNextBatch() {
        Set<String> memberVersions = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document familyDocument = (Document) mongoCursor.next();

            familyListBuffer.add(familyDocument);
            counter++;

            // Extract all the members
            Object members = familyDocument.get(FamilyMongoDBAdaptor.QueryParams.MEMBERS.key());
            if (members != null && !options.getBoolean(NATIVE_QUERY)) {
                List<Document> memberList = (List<Document>) members;
                if (!memberList.isEmpty()) {
                    memberList.forEach(s -> {
                        String uid = String.valueOf(s.get(FamilyDBAdaptor.QueryParams.UID.key()));
                        String version = String.valueOf(s.get(FamilyDBAdaptor.QueryParams.VERSION.key()));

                        memberVersions.add(uid + "__" + version);
                    });
                }
            }
        }

        if (!memberVersions.isEmpty()) {
            // Obtain all those members

            List<Long> uidList = new ArrayList<>(memberVersions.size());
            List<Integer> versionList = new ArrayList<>(memberVersions.size());
            memberVersions.forEach(s -> {
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, "__");
                uidList.add(Long.valueOf(split[0]));
                versionList.add(Integer.valueOf(split[1]));
            });

            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(IndividualDBAdaptor.QueryParams.UID.key(), uidList)
                    .append(IndividualDBAdaptor.QueryParams.VERSION.key(), versionList);
            List<Document> memberList;
            try {
                if (user != null) {
                    memberList = individualDBAdaptor.nativeGet(query, individualQueryOptions, user).getResult();
                } else {
                    memberList = individualDBAdaptor.nativeGet(query, individualQueryOptions).getResult();
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the members associated to the families: {}", e.getMessage(), e);
                return;
            }

            // Map each member uid - version to the member entry
            Map<String, Document> memberMap = new HashMap<>(memberList.size());
            memberList.forEach(member ->
                    memberMap.put(String.valueOf(member.get(FamilyDBAdaptor.QueryParams.UID.key())) + "__"
                            + String.valueOf(member.get(FamilyDBAdaptor.QueryParams.VERSION.key())), member)
            );

            // Add the members obtained to the corresponding families
            familyListBuffer.forEach(family -> {
                List<Document> tmpMemberList = new ArrayList<>();
                List<Document> members = (List<Document>) family.get(FamilyMongoDBAdaptor.QueryParams.MEMBERS.key());

                members.forEach(s -> {
                    String uid = String.valueOf(s.get(FamilyDBAdaptor.QueryParams.UID.key()));
                    String version = String.valueOf(s.get(FamilyDBAdaptor.QueryParams.VERSION.key()));
                    String key = uid + "__" + version;

                    // If the members has been returned... (it might have not been fetched due to permissions issues)
                    if (memberMap.containsKey(key)) {
                        tmpMemberList.add(memberMap.get(key));
                    }
                });

                family.put(FamilyMongoDBAdaptor.QueryParams.MEMBERS.key(), tmpMemberList);
            });
        }
    }

    private QueryOptions createFamilyQueryOptions() {
        QueryOptions queryOptions = new QueryOptions(NATIVE_QUERY, true);

        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            List<String> includeList = new ArrayList<>();
            for (String include : currentIncludeList) {
                if (include.startsWith(FamilyDBAdaptor.QueryParams.MEMBERS.key() + ".")) {
                    includeList.add(include.replace(FamilyDBAdaptor.QueryParams.MEMBERS.key() + ".", ""));
                }
            }
            if (!includeList.isEmpty()) {
                // If we only have include uid or version, there is no need for an additional query so we will set current options to
                // native query
                boolean includeAdditionalFields = includeList.stream().anyMatch(
                        field -> !field.equals(IndividualDBAdaptor.QueryParams.VERSION.key())
                                && !field.equals(IndividualDBAdaptor.QueryParams.UID.key())
                );
                if (includeAdditionalFields) {
                    includeList.add(IndividualDBAdaptor.QueryParams.VERSION.key());
                    includeList.add(IndividualDBAdaptor.QueryParams.UID.key());
                    queryOptions.put(QueryOptions.INCLUDE, includeList);
                } else {
                    // User wants to include fields already retrieved
                    options.put(NATIVE_QUERY, true);
                }
            }
        }
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> currentExcludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            List<String> excludeList = new ArrayList<>();
            for (String exclude : currentExcludeList) {
                if (exclude.startsWith(FamilyDBAdaptor.QueryParams.MEMBERS.key() + ".")) {
                    String replace = exclude.replace(FamilyDBAdaptor.QueryParams.MEMBERS.key() + ".", "");
                    if (!FamilyDBAdaptor.QueryParams.UID.key().equals(replace)
                            && !FamilyDBAdaptor.QueryParams.VERSION.key().equals(replace)) {
                        excludeList.add(replace);
                    }
                }
            }
            if (!excludeList.isEmpty()) {
                queryOptions.put(QueryOptions.EXCLUDE, excludeList);
            } else {
                queryOptions.remove(QueryOptions.EXCLUDE);
            }
        }
        if (options.containsKey(Constants.FLATTENED_ANNOTATIONS)) {
            queryOptions.put(Constants.FLATTENED_ANNOTATIONS, options.getBoolean(Constants.FLATTENED_ANNOTATIONS));
        }

        return queryOptions;
    }


}
