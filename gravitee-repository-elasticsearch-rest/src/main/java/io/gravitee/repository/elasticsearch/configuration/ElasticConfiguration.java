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
package io.gravitee.repository.elasticsearch.configuration;

import io.gravitee.repository.elasticsearch.model.HostAddress;
import io.gravitee.repository.elasticsearch.model.Protocol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.List;

/**
 * Elasticsearch client configuration.
 *
 * @author David BRASSELY (brasseld at gmail.com)
 */
public class ElasticConfiguration {
	
	private static final String PORT_SEPARATOR = ":";

	@Autowired
	private Environment environment;
	
	/**
	 *  Client communication protocol. 
	 */
	@Value("${analytics.elasticsearch.protocol:HTTP}")
	private Protocol protocol;
	
	/**
	 * Cluster name. Used only for node protocol
	 */
	@Value("${analytics.elasticsearch.cluster:elasticsearch}")
	private String clusterName;
	
	/**
	 * Prefix index name. 
	 */
	@Value("${analytics.elasticsearch.index:gravitee}")
	private String indexName;
	
	/**
	 * Elasticsearch basic oauth login. 
	 */
	@Value("${analytics.elasticsearch.username}")
	private String username;
	
	/**
	 * Elasticsearch basic oauth password. 
	 */
	@Value("${analytics.elasticsearch.password}")
	private String password;

	/**
	 * Elasticsearch hosts
	 */
	private List<HostAddress> hostsAddresses;
	
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Protocol getProtocol() {
		return protocol;
	}

	public String getClusterName() {
		return clusterName;
	}

	public List<HostAddress> getHostsAddresses() {
		if(hostsAddresses == null){
			hostsAddresses = initializeHostsAddresses();
		}
		return hostsAddresses;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	private List<HostAddress> initializeHostsAddresses(){
		String key = String.format("analytics.elasticsearch.hosts[%s]", 0);
		List<HostAddress> res = new ArrayList<>();
		
		while (environment.containsProperty(key)) {
			String serializedHost = environment.getProperty(key);
			
			if (serializedHost.contains(PORT_SEPARATOR)) {
				String[] hostParts = serializedHost.split(PORT_SEPARATOR);
				
				String hostname = hostParts[0].toLowerCase();
				Integer port = Integer.parseInt(hostParts[1].trim());
				
				res.add(new HostAddress(hostname, port));
			} else {
				res.add(new HostAddress(serializedHost.trim(), protocol.getDefaultPort()));
			}
			
			key = String.format("analytics.elasticsearch.hosts[%s]", res.size());
		}
		
		// Use default host if required
		if(res.isEmpty()){
			res.add(new HostAddress("localhost", protocol.getDefaultPort()));
		}
		return res;
	}
}
