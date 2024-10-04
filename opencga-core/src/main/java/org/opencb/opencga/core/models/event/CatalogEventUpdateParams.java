package org.opencb.opencga.core.models.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class CatalogEventUpdateParams {

    private List<EventSubscriber> subscribers;
    private Boolean successful;

    public CatalogEventUpdateParams() {
    }

    public CatalogEventUpdateParams(List<EventSubscriber> subscribers) {
        this.subscribers = subscribers;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CatalogEventUpdateParams{");
        sb.append("subscribers=").append(subscribers);
        sb.append(", successful=").append(successful);
        sb.append('}');
        return sb.toString();
    }

    public List<EventSubscriber> getSubscribers() {
        return subscribers;
    }

    public CatalogEventUpdateParams setSubscribers(List<EventSubscriber> subscribers) {
        this.subscribers = subscribers;
        return this;
    }

    public Boolean isSuccessful() {
        return successful;
    }

    public CatalogEventUpdateParams setSuccessful(Boolean successful) {
        this.successful = successful;
        return this;
    }
}
