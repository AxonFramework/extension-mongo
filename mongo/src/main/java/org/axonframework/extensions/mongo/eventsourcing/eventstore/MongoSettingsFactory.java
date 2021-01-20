/*
 * Copyright (c) 2010-2016. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.mongo.eventsourcing.eventstore;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Factory class used to create a {@code MongoClientSettings} instance. The instance makes use of the defaults as
 * provided by the MongoOptions class. The moment you set a valid value, that value is used to create the options
 * object.
 * </p>
 * <p>
 * Note: WriteConcern enums were changed in MongoDb driver 4.x. Depending on the number of addresses provided, the
 * factory defaults to either {@link WriteConcern#W2} when more than one address is provided, or {@link
 * WriteConcern#JOURNALED} when only one server is available. The idea of these defaults is that data must be able to
 * survive a (not too heavy) crash without loss of data. We wouldn't want to publish untraceable events, would we...
 *
 * @author Jettro Coenradie
 * @since 2.0 (in incubator since 0.7)
 */
public class MongoSettingsFactory {

    private static final Logger logger = LoggerFactory.getLogger(MongoSettingsFactory.class);

    private final MongoClientSettings defaults;
    private List<ServerAddress> mongoAddresses = Collections.emptyList();
    private WriteConcern writeConcern;
    private int connectionsPerHost;
    private int socketConnectTimeout;
    private long maxWaitTime;
    private int socketReadTimeOut;

    /**
     * Default constructor for the factory that initializes the defaults.
     */
    public MongoSettingsFactory() {
        defaults = MongoClientSettings.builder().build();
    }

    /**
     * Uses the configured parameters to create a MongoOptions instance.
     *
     * @return MongoOptions instance based on the configured properties
     */
    public MongoClientSettings createMongoClientSettings() {
         final MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder()
                .applyToConnectionPoolSettings(builder -> builder.maxWaitTime(getMaxWaitTime(), TimeUnit.MILLISECONDS).maxSize(getConnectionsPerHost()))
                .applyToSocketSettings(builder -> builder.connectTimeout(getSocketConnectTimeout(), TimeUnit.MILLISECONDS).readTimeout(getSocketReadTimeOut(), TimeUnit.MILLISECONDS))
                .writeConcern(defaultWriteConcern());

        if (this.mongoAddresses != null && this.mongoAddresses.size() > 0) {
            mongoClientSettingsBuilder.applyToClusterSettings(builder -> builder.hosts(mongoAddresses));
        }

        MongoClientSettings settings = mongoClientSettingsBuilder.build();

        if (logger.isDebugEnabled()) {
            logger.debug("Mongo Options");
            logger.debug("Connections per host :{}", settings.getConnectionPoolSettings().getMaxSize());
            logger.debug("Connection timeout : {}", settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
            logger.debug("Max wait timeout : {}", settings.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
            logger.debug("Socket read timeout : {}", settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
        }

        return settings;
    }

    /**
     * Getter for connectionsPerHost.
     *
     * @return number representing the connections per host
     */
    public int getConnectionsPerHost() {
        return (connectionsPerHost > 0) ? connectionsPerHost : defaults.getConnectionPoolSettings().getMaxSize();
    }

    /**
     * Setter for the connections per host that are allowed.
     *
     * @param connectionsPerHost number representing the number of connections per host
     */
    public void setConnectionsPerHost(int connectionsPerHost) {
        this.connectionsPerHost = connectionsPerHost;
    }

    /**
     * Sets the connection timeout in milliseconds for doing something in mongo. Zero is indefinite
     *
     * @return number representing milli seconds of timeout
     */
    public int getSocketConnectTimeout() {
        return (socketConnectTimeout > 0) ? socketConnectTimeout : defaults.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS);
    }

    /**
     * Setter for socket connection timeout.
     *
     * @param socketConnectTimeout number representing the connection timeout in millis
     */
    public void setSocketConnectTimeout(int socketConnectTimeout) {
        this.socketConnectTimeout = socketConnectTimeout;
    }

    /**
     * get the maximum time a blocked thread that waits for a connection should wait.
     *
     * @return number of milli seconds the thread waits for a connection
     */
    public long getMaxWaitTime() {
        return (maxWaitTime > 0) ? maxWaitTime : defaults.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS);
    }

    /**
     * Set the max wait time for a blocked thread in milli seconds.
     *
     * @param maxWaitTime number representing the number of milli seconds to wait for a thread
     */
    public void setMaxWaitTime(long maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    /**
     * Getter for the socket read timeout.
     *
     * @return Number representing the amount of milli seconds to wait for a socket connection
     */
    public int getSocketReadTimeOut() {
        return (socketReadTimeOut > 0) ? socketReadTimeOut : defaults.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS);
    }

    /**
     * Setter for the socket read timeout.
     *
     * @param socketReadTimeOut number representing the amount of milli seconds to wait for a socket connection
     */
    public void setSocketReadTimeOut(int socketReadTimeOut) {
        this.socketReadTimeOut = socketReadTimeOut;
    }

    /**
     * Provide a list of ServerAddress objects to use for locating the Mongo replica set. An empty list will result in a
     * single Mongo instance being used on the default host ({@code 127.0.0.1}) and port ({@code
     * com.mongodb.ServerAddress#defaultPort})
     * <p>
     * Defaults to an empty list, which locates a single Mongo instance on the default host ({@code 127.0.0.1}) and
     * port
     * <code>({@code com.mongodb.ServerAddress#defaultPort})</code>
     *
     * @param mongoAddresses List of ServerAddress instances
     */
    public void setMongoAddresses(List<ServerAddress> mongoAddresses) {
        this.mongoAddresses = mongoAddresses;
    }

    /**
     * Provided a write concern to be used by the mongo instance. The provided concern should be compatible with the
     * number of addresses provided with {@link #setMongoAddresses(java.util.List)}. For example, providing {@link
     * WriteConcern#W2} in combination with a single address will cause each write operation to hang.
     * <p>
     * While safe (e.g. {@link WriteConcern#W2}) WriteConcerns allow you to detect concurrency issues immediately, you
     * might want to use a more relaxed write concern if you have other mechanisms in place to ensure consistency.
     * <p>
     * Defaults to {@link WriteConcern#W2} if you provided more than one address with {@link
     * #setMongoAddresses(java.util.List)}, or {@link WriteConcern#JOURNALED} if there is only one address (or none at
     * all).
     *
     * @param writeConcern WriteConcern to use for the connections
     */
    public void setWriteConcern(WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    private WriteConcern defaultWriteConcern() {
        if (writeConcern != null) {
            return this.writeConcern;
        } else if (mongoAddresses.size() > 1) {
            return WriteConcern.W2;
        } else {
            return WriteConcern.JOURNALED;
        }
    }
}
