/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.repository.elasticsearch.spring;

import io.gravitee.repository.elasticsearch.configuration.ElasticConfiguration;
import io.gravitee.repository.elasticsearch.model.HostAddress;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AbstractFactoryBean;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @author David BRASSELY (brasseld at gmail.com)
 */
public class ElasticClientFactory extends AbstractFactoryBean<Client> {

    private final Logger LOGGER = LoggerFactory.getLogger(ElasticClientFactory.class);

    @Autowired
    private ElasticConfiguration configuration;

    @Override
    public Class<Client> getObjectType() {
        return Client.class;
    }

    @Override
    protected Client createInstance() throws Exception {
        switch (configuration.getProtocol()) {
            case TRANSPORT:
                return createTransportClient();
            case NODE:
                return createNodeClient();
            default:
                LOGGER.error("Unsupported protocol [{}] for elastic client", configuration.getProtocol());
                throw new IllegalStateException(String.format("Unsupported protocol [%s] for elastic client", configuration.getProtocol()));
        }
    }

    private Client createTransportClient() {
        Settings settings = Settings.builder().put("cluster.name", configuration.getClusterName()).build();
        TransportClient transportClient = new PreBuiltTransportClient(settings);

        List<HostAddress> addresses = configuration.getHostsAddresses();

        for (HostAddress address : addresses) {
            try {
                transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address.getHostname()), address.getPort()));
            } catch (final UnknownHostException uhe) {
                LOGGER.error("Invalid hostname [{}]", address.getHostname());
                throw new IllegalStateException(String.format("Invalid hostname [%s]", address.getHostname()), uhe);
            }
        }

        return transportClient;
    }

    private Client createNodeClient() {
        Settings settings = Settings.builder()
                .put("cluster.name", configuration.getClusterName())
                .put("gateway.type","none")
                // .put("index.number_of_shards",numberOfShards)
                // .put("index.number_of_replicas",0)
                .build();

        return new Node(settings).client();
    }
}
