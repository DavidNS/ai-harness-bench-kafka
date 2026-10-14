/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;

/**
 * {@code PollResult} consist of {@code UnsentRequest} if there are requests to send; otherwise, return the time till
 * the next poll event.
 */
public interface RequestManager {

    /**
     * On each poll cycle of the network thread, this method is invoked to give the {@code RequestManager} the
     * opportunity to create the requests that should be sent.
     *
     * @param currentTimeMs The current time, in milliseconds
     * @return {@link PollResult} with the requests to send and the time until the next poll
     */
    PollResult poll(long currentTimeMs);

    /**
     * On shutdown of the network thread, this method is invoked to give the {@code RequestManager} the opportunity
     * to create any final requests (such as closing fetch sessions) that should be sent before it terminates.
     *
     * @return {@link PollResult} with the requests to send; by default, there are none
     */
    default PollResult pollOnClose() {
        return PollResult.EMPTY;
    }
}
