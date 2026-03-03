package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationFindingsDBAdaptor;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.InterpretationFindingsMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.PanelMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogDBRuntimeException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor.QueryParams.PANELS;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.LAST_OF_VERSION;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class InterpretationCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;
    private boolean deleted;

    private PanelMongoDBAdaptor panelDBAdaptor;
    private QueryOptions panelQueryOptions;

    private final InterpretationFindingsMongoDBAdaptor findingDBAdaptor;

    private QueryOptions options;

    private Queue<Document> interpretationListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;
    private static final String UID_VERSION_SEP = "___";

    public InterpretationCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                                GenericDocumentComplexConverter<E> converter,
                                                OrganizationMongoDBAdaptorFactory dbAdaptorFactory, QueryOptions options, boolean deleted) {
        this(mongoCursor, clientSession, converter, dbAdaptorFactory, 0, null, options, deleted);
    }

    public InterpretationCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                                GenericDocumentComplexConverter<E> converter,
                                                OrganizationMongoDBAdaptorFactory dbAdaptorFactory, long studyUid, String user,
                                                QueryOptions options, boolean deleted) {
        super(mongoCursor, clientSession, converter, null);

        this.user = user;
        this.studyUid = studyUid;
        this.deleted = deleted;

        this.options = options;
        this.panelDBAdaptor = dbAdaptorFactory.getCatalogPanelDBAdaptor();
        this.panelQueryOptions = createInnerQueryOptionsForVersionedEntity(options, PANELS.key(), false);

        this.findingDBAdaptor = dbAdaptorFactory.getFindingsDBAdaptor();

        this.interpretationListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(InterpretationCatalogMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = interpretationListBuffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }


    @Override
    public boolean hasNext() {
        if (interpretationListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !interpretationListBuffer.isEmpty();
    }

    private void fetchNextBatch() {
        boolean nativeQuery = options.getBoolean(NATIVE_QUERY);
        Set<String> panelSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document interpretationDocument = mongoCursor.next();

            if (studyUid <= 0) {
                studyUid = interpretationDocument.get(PRIVATE_STUDY_UID, Number.class).longValue();
            }

            interpretationListBuffer.add(interpretationDocument);
            counter++;

            if (!nativeQuery) {
                // Extract the panels
                List<Document> panels = interpretationDocument.getList(InterpretationDBAdaptor.QueryParams.PANELS.key(), Document.class);
                if (CollectionUtils.isNotEmpty(panels)) {
                    for (Document panel : panels) {
                        if (panel != null && panel.get(UID, Number.class).longValue() > 0) {
                            panelSet.add(panel.get(UID) + UID_VERSION_SEP + panel.get(VERSION));
                        }
                    }
                }
            }
        }
        Map<String, Document> panelMap = fetchPanels(panelSet);

        if (!nativeQuery) {
            // Fill data in clinical analyses
            interpretationListBuffer.forEach(interpretation -> {
                fillPanels(interpretation, panelMap);
                fillFindings(interpretation, InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key());
                fillFindings(interpretation, InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key());
            });
        }
    }

    private void fillFindings(Document interpretation, String key) {
        List<MiniObject> findings = extractFindings(interpretation, key);
        if (findings.isEmpty()) {
            return;
        }

        String interpretationId = interpretation.getString(InterpretationDBAdaptor.QueryParams.ID.key());
        List<Document> documentFindings = queryFindings(findings, interpretationId);

        if (findings.size() > documentFindings.size()) {
            throw new CatalogDBRuntimeException("Some findings could not be found for interpretation " + interpretationId);
        }
        interpretation.put(key, documentFindings);
    }

    private List<Document> queryFindings(List<MiniObject> findings, String interpretationId) {
        List<String> findingIds = new ArrayList<>(findings.size());
        List<Integer> findingVersions = new ArrayList<>(findings.size());
        for (MiniObject finding : findings) {
            findingIds.add(finding.getId());
            if (finding.getVersion() != null) {
                findingVersions.add(finding.getVersion());
            }
        }
        Query query = new Query()
                .append(InterpretationFindingsDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(InterpretationFindingsDBAdaptor.QueryParams.INTERPRETATION_ID.key(), interpretationId)
                .append(InterpretationFindingsDBAdaptor.QueryParams.ID.key(), findingIds);
        if (!findingVersions.isEmpty()) {
            query.append(InterpretationFindingsDBAdaptor.QueryParams.VERSION.key(), findingVersions);
        }
        if (deleted) {
            query.append(InterpretationFindingsDBAdaptor.QueryParams.DELETED.key(), true);
        }

        try {
            return findingDBAdaptor.nativeGet(clientSession, query, QueryOptions.empty()).getResults();
        } catch (CatalogDBException e) {
            throw new CatalogDBRuntimeException(e.getMessage(), e);
        }
    }

    private List<MiniObject> extractFindings(Document interpretationDocument, String key) {
        List<MiniObject> findingsList = new LinkedList<>();

        List<Document> findings = interpretationDocument.getList(key, Document.class);
        if (CollectionUtils.isNotEmpty(findings)) {
            Boolean lastOfVersion = interpretationDocument.getBoolean(LAST_OF_VERSION);
            for (Document finding : findings) {
                if (finding != null) {
                    String findingId = finding.getString(InterpretationFindingsMongoDBAdaptor.QueryParams.ID.key());
                    if (lastOfVersion) {
                        findingsList.add(new MiniObject(findingId));
                    } else {
                        int version = finding.getInteger(InterpretationFindingsMongoDBAdaptor.QueryParams.VERSION.key());
                        findingsList.add(new MiniObject(findingId, version));
                    }
                }
            }
        }

        return findingsList;
    }



    private Map<String, Document> fetchPanels(Set<String> panelSet) {
        Map<String, Document> panelMap = new HashMap<>();

        if (panelSet.isEmpty()) {
            return panelMap;
        }

        // Extract list of uids and versions
        List<Long> panelUids = new ArrayList<>(panelSet.size());
        List<Integer> panelUidVersions = new ArrayList<>(panelSet.size());
        for (String panelId : panelSet) {
            String[] split = panelId.split(UID_VERSION_SEP);
            panelUids.add(Long.parseLong(split[0]));
            panelUidVersions.add(Integer.parseInt(split[1]));
        }

        // Fill panels with version
        List<Document> panelList = queryPanels(panelUids, panelUidVersions);
        panelList.forEach(panel
                -> panelMap.put(panel.get(UID) + UID_VERSION_SEP + panel.get(VERSION), panel));

        return panelMap;
    }

    private List<Document> queryPanels(List<Long> panelUids, List<Integer> panelUidVersions) {
        List<Document> panelList = new LinkedList<>();

        if (panelUids.isEmpty()) {
            return panelList;
        }

        // Build query object
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.UID.key(), panelUids)
                .append(PanelDBAdaptor.QueryParams.VERSION.key(), panelUidVersions);

        try {
            if (user != null) {
                query.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                panelList = panelDBAdaptor.nativeGet(clientSession, studyUid, query, panelQueryOptions, user).getResults();
            } else {
                panelList = panelDBAdaptor.nativeGet(clientSession, query, panelQueryOptions).getResults();
            }
        } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
            throw new CatalogDBRuntimeException("Could not obtain the panels associated to the interpretation: " + e.getMessage(), e);
        }
        return panelList;
    }

    private void fillPanels(Document interpretation, Map<String, Document> panelMap) {
        if (panelMap.isEmpty()) {
            return;
        }

        List<Document> sourcePanels = interpretation.getList(InterpretationDBAdaptor.QueryParams.PANELS.key(), Document.class);
        if (sourcePanels != null) {
            List<Document> targetPanels = new ArrayList<>(sourcePanels.size());
            for (Document panel : sourcePanels) {
                String panelKey = panel.get(UID) + UID_VERSION_SEP + panel.get(VERSION);
                if (panelMap.containsKey(panelKey)) {
                    targetPanels.add(panelMap.get(panelKey));
                }
            }
            interpretation.put(InterpretationDBAdaptor.QueryParams.PANELS.key(), targetPanels);
        }
    }

    private static class MiniObject {
        private final String id;
        private final Integer version;

        MiniObject(String id) {
            this(id, null);
        }

        MiniObject(String id, Integer version) {
            this.id = id;
            this.version = version;
        }

        public String getId() {
            return id;
        }

        public Integer getVersion() {
            return version;
        }
    }


}
