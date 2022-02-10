package org.opencb.opencga.app.migrations.v2_2_0.catalog.issue_1849;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.opencga.core.common.GitRepositoryState;

public class CompleteStatusDataModelUtils {

    public static void completeStatus(Document document) {
        Document status = document.get("status", Document.class);
        if (status != null) {
            String id = status.getString("id");
            String name = status.getString("name");
            if (StringUtils.isEmpty(id)) {
                status.put("id", StringUtils.isNotEmpty(name) ? name : "");
            }
            status.put("name", StringUtils.isNotEmpty(name) ? name : "");
        } else {
            document.put("status", new Document()
                    .append("id", "READY")
                    .append("name", "")
                    .append("description", "")
                    .append("date", "")
            );
        }
    }

    public static void completeInternalStatus(Document document) {
        Document internal = document.get("internal", Document.class);

        if (internal == null) {
            internal = new Document();
            document.put("internal", internal);
        }

        completeStatus(internal);
        Document status = internal.get("status", Document.class);
        status.put("version", GitRepositoryState.get().getBuildVersion());
        status.put("commit", GitRepositoryState.get().getCommitId());

        String id = status.getString("id");
        if (StringUtils.isEmpty(id)) {
            status.put("id", "READY");
        }
    }

}
