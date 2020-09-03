package org.opencb.opencga.core.models.common;

import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.sample.Sample;

import java.util.ArrayList;
import java.util.List;

public class ResourceReference extends PrivateStudyUid {

    private String id;
    private String uuid;

    public ResourceReference() {
    }

    public ResourceReference(String id, String uuid) {
        this.id = id;
        this.uuid = uuid;
    }

    public ResourceReference(String id, String uuid, long uid, long studyUid) {
        super(studyUid);
        super.setUid(uid);
        this.id = id;
        this.uuid = uuid;
    }

    public static <T extends IPrivateStudyUid> ResourceReference of(T reference) {
        return new ResourceReference(reference.getId(), reference.getUuid(), reference.getUid(), reference.getStudyUid());
    }

    public static List<ResourceReference> of(List<? extends IPrivateStudyUid> referenceList) {
        List<ResourceReference> resourceReferenceList = new ArrayList<>(referenceList.size());
        for (IPrivateStudyUid reference : referenceList) {
            resourceReferenceList.add(of(reference));
        }
        return resourceReferenceList;
    }

    public Sample toSample() {
        return new Sample()
                .setId(id)
                .setUuid(uuid)
                .setUid(getUid())
                .setStudyUid(getStudyUid());
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ResourceReference setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public ResourceReference setUid(long uid) {
        super.setUid(uid);
        return this;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    public ResourceReference setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }
}
