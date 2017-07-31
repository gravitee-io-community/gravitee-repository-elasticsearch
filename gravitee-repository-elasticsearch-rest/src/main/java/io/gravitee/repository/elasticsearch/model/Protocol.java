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
package io.gravitee.repository.elasticsearch.model;

/**
 * Protocol supported by this elasticsearch repository : only HTTP
 * 
 * @author David BRASSELY (brasseld at gmail.com)
 * @author Guillaume Waignier
 * @author Sebastien Devaux
 */
public enum Protocol {
	
	HTTP(9200);
	
	private final Integer defaultPort;
	
	private Protocol(Integer defaultPort) {
		this.defaultPort = defaultPort;
	}
	
	public static Protocol getByName(final String value){
		
		for (Protocol protocol : values()) {
			if(protocol.name().equalsIgnoreCase(value)){
				return protocol;
			}
		}
		throw new IllegalArgumentException(String.format("Unsupported protocol [%s]", value));
		
	}

	public Integer getDefaultPort() {
		return defaultPort;
	}
}
