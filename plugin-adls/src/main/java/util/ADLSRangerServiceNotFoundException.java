/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//
// RangerServiceNotFoundException class has been
// copied from agents-common/src/main/java/org/apache/ranger/plugin/util/RangerServiceNotFoundException.java
// and edited then
//

package util;

import org.apache.commons.lang.StringUtils;

public class ADLSRangerServiceNotFoundException extends Exception {
    static private final String formatString = "\"ADLS_RANGER_ERROR_SERVICE_NOT_FOUND: ServiceName=%s\"";

    public ADLSRangerServiceNotFoundException(String serviceName) {
        super(serviceName);
    }

    public static final String buildExceptionMsg(String serviceName) {
        return String.format(formatString, serviceName);
    }

    public static final void throwExceptionIfServiceNotFound(String serviceName, String exceptionMsg) throws ADLSRangerServiceNotFoundException {
        String expectedExceptionMsg = buildExceptionMsg(serviceName);
        if (StringUtils.startsWith(exceptionMsg, expectedExceptionMsg)) {
            throw new ADLSRangerServiceNotFoundException(serviceName);
        }
    }
}
