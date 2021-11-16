package org.opencb.opencga.catalog.db.mongodb.iterators;

import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor.QueryParams.PANELS;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class InterpretationCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private PanelDBAdaptor panelDBAdaptor;
    private QueryOptions panelQueryOptions;

    private QueryOptions options;

    private Queue<Document> interpretationListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;
    private static final String UID_VERSION_SEP = "___";

    public InterpretationCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, GenericDocumentComplexConverter<E> converter,
                                                DBAdaptorFactory dbAdaptorFactory, QueryOptions options) {
        this(mongoCursor, converter, dbAdaptorFactory, 0, null, options);
    }

    public InterpretationCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, GenericDocumentComplexConverter<E> converter,
                                                DBAdaptorFactory dbAdaptorFactory, long studyUid, String user, QueryOptions options) {
        super(mongoCursor, converter);

        this.user = user;
        this.studyUid = studyUid;

        this.options = options;
        this.panelDBAdaptor = dbAdaptorFactory.getCatalogPanelDBAdaptor();
        this.panelQueryOptions = createInnerQueryOptionsForVersionedEntity(options, PANELS.key(), false);

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
        Set<String> panelSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document interpretationDocument = mongoCursor.next();

            if (user != null && studyUid <= 0) {
                studyUid = interpretationDocument.get(PRIVATE_STUDY_UID, Number.class).longValue();
            }

            interpretationListBuffer.add(interpretationDocument);
            counter++;

            if (!options.getBoolean(NATIVE_QUERY)) {
                // Extract the panels
                List<Document> panels = interpretationDocument.getList(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), Document.class);
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

        if (!panelMap.isEmpty()) {
            // Fill data in clinical analyses
            interpretationListBuffer.forEach(interpretation -> {
                fillPanels(interpretation, panelMap);
            });
        }
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
        Query query = new Query(PanelDBAdaptor.QueryParams.UID.key(), panelUids)
                .append(PanelDBAdaptor.QueryParams.VERSION.key(), panelUidVersions);

        try {
            if (user != null) {
                query.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                panelList = panelDBAdaptor.nativeGet(studyUid, query, panelQueryOptions, user).getResults();
            } else {
                panelList = panelDBAdaptor.nativeGet(query, panelQueryOptions).getResults();
            }
        } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
            logger.warn("Could not obtain the panels associated to the clinical analyses: {}", e.getMessage(), e);
        }
        return panelList;
    }

    private void fillPanels(Document interpretation, Map<String, Document> panelMap) {
        if (panelMap.isEmpty()) {
            return;
        }

        List<Document> sourcePanels = interpretation.getList(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), Document.class);
        if (sourcePanels != null) {
            List<Document> targetPanels = new ArrayList<>(sourcePanels.size());
            for (Document panel : sourcePanels) {
                String panelKey = panel.get(UID) + UID_VERSION_SEP + panel.get(VERSION);
                if (panelMap.containsKey(panelKey)) {
                    targetPanels.add(panelMap.get(panelKey));
                }
            }
            interpretation.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), targetPanels);
        }
    }

}
