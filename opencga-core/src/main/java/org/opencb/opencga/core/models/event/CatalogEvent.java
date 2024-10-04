package org.opencb.opencga.core.models.event;

import org.opencb.opencga.core.events.OpencgaEvent;
import org.opencb.opencga.core.models.PrivateFields;

import java.util.List;

public class CatalogEvent extends PrivateFields {

    private String id;
    private String uuid;

    private List<EventSubscriber> subscribers;
    private boolean successful;

    private String creationDate;
    private String modificationDate;

    private OpencgaEvent event;

    public CatalogEvent() {
    }

    public CatalogEvent(String id, OpencgaEvent event) {
        this.id = id;
        this.event = event;
    }

    public CatalogEvent(String id, List<EventSubscriber> subscribers, boolean successful, String creationDate, String modificationDate,
                        OpencgaEvent event) {
        this.id = id;
        this.subscribers = subscribers;
        this.successful = successful;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.event = event;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CatalogEvent{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", subscribers=").append(subscribers);
        sb.append(", successful=").append(successful);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", event=").append(event);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public CatalogEvent setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public CatalogEvent setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public List<EventSubscriber> getSubscribers() {
        return subscribers;
    }

    public CatalogEvent setSubscribers(List<EventSubscriber> subscribers) {
        this.subscribers = subscribers;
        return this;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public CatalogEvent setSuccessful(boolean successful) {
        this.successful = successful;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public CatalogEvent setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public CatalogEvent setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public OpencgaEvent getEvent() {
        return event;
    }

    public CatalogEvent setEvent(OpencgaEvent event) {
        this.event = event;
        return this;
    }
}
