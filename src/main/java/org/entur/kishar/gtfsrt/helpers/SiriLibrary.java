/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */
package org.entur.kishar.gtfsrt.helpers;

import java.time.Instant;
import java.time.ZonedDateTime;

public class SiriLibrary {

    public static ZonedDateTime getCurrentTime() {
       return ZonedDateTime.now();
    }

    public static Instant getLatestTimestamp(Instant t1, Instant t2) {
        if (t1 == null) {
            return t2;
        } else if (t2 == null) {
            return t1;
        } else if (t2.isAfter(t1)) {
            return t2;
        } else {
            return t1;
        }
    }
}
