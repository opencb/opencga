package org.opencb.opencga.core.models.federation;

import java.util.List;

public class Federation {

    private List<FederationClient> clients;
    private List<FederationServer> servers;

    public Federation() {
    }

    public Federation(List<FederationClient> clients, List<FederationServer> servers) {
        this.clients = clients;
        this.servers = servers;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Federation{");
        sb.append("clients=").append(clients);
        sb.append(", servers=").append(servers);
        sb.append('}');
        return sb.toString();
    }

    public List<FederationClient> getClients() {
        return clients;
    }

    public Federation setClients(List<FederationClient> clients) {
        this.clients = clients;
        return this;
    }

    public List<FederationServer> getServers() {
        return servers;
    }

    public Federation setServers(List<FederationServer> servers) {
        this.servers = servers;
        return this;
    }
}
