/**
 * Copyright (C) 2011 Google, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.entur.kishar.gtfsrt;

import uk.org.siri.siri20.PtSituationElement;

import java.time.ZonedDateTime;

public class AlertData {
    private final PtSituationElement situation;

    private final String producer;

    private final ZonedDateTime expirationTime;

    public AlertData(PtSituationElement situation, String producer, ZonedDateTime expirationTime) {
        this.situation = situation;
        this.producer = producer;
        this.expirationTime = expirationTime;
    }

    public PtSituationElement getSituation() {
        return situation;
    }

    public String getProducer() {
        return producer;
    }

    public ZonedDateTime getExpirationTime() {
        return expirationTime;
    }
}
