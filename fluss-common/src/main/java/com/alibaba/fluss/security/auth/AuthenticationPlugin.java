/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.security.auth;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.plugin.Plugin;

/**
 * The AuthenticationPlugin interface defines a contract for authentication mechanisms in the Fluss
 * RPC system. Implementations of this interface provide specific authentication protocols (e.g.,
 * PLAINTEXT, AK-SK) and must be registered via the Service Provider Interface (SPI) mechanism to be
 * discoverable by the system.
 *
 * <p>Authentication plugins are used by {@link AuthenticatorLoader} to create client and server
 * authenticators based on configuration settings such as {@link
 * com.alibaba.fluss.config.ConfigOptions#CLIENT_SECURITY_PROTOCOL} and {@link
 * com.alibaba.fluss.config.ConfigOptions#SERVER_SECURITY_PROTOCOL_MAP}.
 *
 * @since 0.7
 */
public interface AuthenticationPlugin extends Plugin {
    /**
     * Returns the authentication protocol identifier for this plugin (e.g., "PLAINTEXT", "AK-SK").
     * This identifier is used by the system to match plugins with configuration settings and select
     * the appropriate authentication mechanism.
     *
     * <p>It is typically configured via:
     *
     * <ul>
     *   <li>{@link com.alibaba.fluss.config.ConfigOptions#CLIENT_SECURITY_PROTOCOL} for client-side
     *       authentication.
     *   <li>{@link com.alibaba.fluss.config.ConfigOptions#SERVER_SECURITY_PROTOCOL_MAP} for server
     *       listener-specific authentication.
     * </ul>
     *
     * @return The protocol name (e.g., "PLAINTEXT").
     */
    String authProtocol();

    /**
     * Creates a client-side authenticator instance for this authentication protocol.
     *
     * @return A new client authenticator instance implementing the protocol defined by {@link
     *     #authProtocol()}
     */
    ClientAuthenticator createClientAuthenticator(Configuration configuration);

    /**
     * Creates a server-side authenticator instance for this authentication protocol.
     *
     * @return A new server authenticator instance implementing the protocol defined by {@link
     *     #authProtocol()}
     */
    ServerAuthenticator createServerAuthenticator(Configuration configuration);
}
