package org.opencb.opencga.core.models.federation;

import java.util.List;

public class Federation {

    private List<FederationClientParams> clients;
    private List<FederationServerParams> servers;

    public Federation() {
    }

    public Federation(List<FederationClientParams> clients, List<FederationServerParams> servers) {
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

    public List<FederationClientParams> getClients() {
        return clients;
    }

    public Federation setClients(List<FederationClientParams> clients) {
        this.clients = clients;
        return this;
    }

    public List<FederationServerParams> getServers() {
        return servers;
    }

    public Federation setServers(List<FederationServerParams> servers) {
        this.servers = servers;
        return this;
    }
}
