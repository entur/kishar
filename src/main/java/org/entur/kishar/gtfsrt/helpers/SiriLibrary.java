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

import org.entur.kishar.gtfsrt.AlertData;
import uk.org.siri.siri20.HalfOpenTimestampOutputRangeStructure;
import uk.org.siri.siri20.PtSituationElement;
import uk.org.siri.siri20.WorkflowStatusEnumeration;

import java.time.ZonedDateTime;
import java.util.List;

public class SiriLibrary {

    public static boolean isSituationClosed(PtSituationElement situation) {
        WorkflowStatusEnumeration progress = situation.getProgress();
        return progress != null && (progress == WorkflowStatusEnumeration.CLOSING || progress == WorkflowStatusEnumeration.CLOSED);
    }

    public static boolean isSituationExpired(PtSituationElement situation, ZonedDateTime currentTime) {
        HalfOpenTimestampOutputRangeStructure publicationWindow = situation.getPublicationWindow();
        if(publicationWindow != null && isTimeRangeActiveOrUpcoming(publicationWindow, currentTime)) {
            return false;
        } else {
            List<HalfOpenTimestampOutputRangeStructure> periods = situation.getValidityPeriods();
            if(periods != null) {
                for (HalfOpenTimestampOutputRangeStructure period : periods) {
                    if(isTimeRangeActiveOrUpcoming(period, currentTime)) {
                        return false;
                    }
                }
            }

            return publicationWindow != null || periods != null && !periods.isEmpty();
        }
    }

    public static boolean isTimeRangeActiveOrUpcoming(HalfOpenTimestampOutputRangeStructure range, ZonedDateTime time) {
        return range.getStartTime() == null && range.getEndTime() == null?false:range.getEndTime() == null || !time.isAfter(range.getEndTime());
    }

    public static boolean isTimeRangeActive(HalfOpenTimestampOutputRangeStructure range, ZonedDateTime time) {
        if(range.getStartTime() == null && range.getEndTime() == null) {
            return false;
        } else {
            boolean from = range.getStartTime() == null || !range.getStartTime().isAfter(time);
            boolean to = range.getEndTime() == null || !range.getEndTime().isBefore(time);
            return from && to;
        }
    }

    public static boolean isAlertDataExpired(AlertData data) {
        if (data.getExpirationTime() == null) {
            return false;
        }
        return data.getExpirationTime().isBefore(ZonedDateTime.now());
    }


    public static boolean isSituationPublishedOrValid(PtSituationElement situation, ZonedDateTime currentTime) {
        HalfOpenTimestampOutputRangeStructure publicationWindow = situation.getPublicationWindow();
        if(publicationWindow != null && isTimeRangeActive(publicationWindow, currentTime)) {
            return true;
        } else {
            List<HalfOpenTimestampOutputRangeStructure> periods = situation.getValidityPeriods();
            if(periods != null) {
                for (HalfOpenTimestampOutputRangeStructure period : periods) {
                    if(isTimeRangeActive(period, currentTime)) {
                        return true;
                    }
                }
            }

            return publicationWindow == null && (periods == null || periods.isEmpty());
        }
    }
}
