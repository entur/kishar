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

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

public class SiriLibrary {

    public static Timestamp getCurrentTime() {
        long millis = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1000000)).build();
        return timestamp;
    }

    public static Timestamp getLatestTimestamp(Timestamp t1, Timestamp t2) {
        if (t1 == null) {
            return t2;
        } else if (t2 == null) {
            return t1;
        } else if (Timestamps.compare(t1, t2) < 0) {
            return t2;
        } else {
            return t1;
        }
    }
}
