/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.gs;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Access to credentials to access Google Cloud Storage buckets during integration tests. */
public class GSTestCredentials {

    @Nullable private static final String GS_TEST_BUCKET = System.getenv("IT_CASE_GS_BUCKET");

    @Nullable
    private static final String GS_SERVICE_ACCOUNT_PATH = System.getenv("IT_CASE_GS_SERVICE_ACCOUNT_PATH");

    // ------------------------------------------------------------------------

    /**
     * Checks whether Google Cloud Storage test credentials are available in the environment
     * variables of this JVM.
     */
    private static boolean credentialsAvailable() {
        return isNotEmpty(GS_TEST_BUCKET) && isNotEmpty(GS_SERVICE_ACCOUNT_PATH);
    }

    /** Checks if a String is not null and not empty. */
    private static boolean isNotEmpty(@Nullable String str) {
        return str != null && !str.isEmpty();
    }

    /** Checks whether credentials are available in the environment variables of this JVM. */
    public static void assumeCredentialsAvailable() {
        assumeTrue(
                credentialsAvailable(), "No GS credentials available in this test's environment");
    }

    /**
     * Gets the Service Account path.
     *
     * <p>This method throws an exception if the key is not available. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getServiceAccountPath() {
        if (GS_SERVICE_ACCOUNT_PATH != null) {
            return GS_SERVICE_ACCOUNT_PATH;
        } else {
            throw new IllegalStateException("Service account configuration not available");
        }
    }

    /**
     * Gets the URI for the path under which all tests should put their data.
     *
     * <p>This method throws an exception if the bucket was not configured. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getTestBucketUri() {
        return getTestBucketUriWithScheme("gs");
    }

    /**
     * Gets the URI for the path under which all tests should put their data.
     *
     * <p>This method throws an exception if the bucket was not configured. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getTestBucketUriWithScheme(String scheme) {
        if (GS_TEST_BUCKET != null) {
            return scheme + "://" + GS_TEST_BUCKET + "/temp/";
        } else {
            throw new IllegalStateException("Google Cloud Storage test bucket not available");
        }
    }
}
