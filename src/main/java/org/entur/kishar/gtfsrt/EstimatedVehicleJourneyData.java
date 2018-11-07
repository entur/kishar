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
package org.entur.kishar.gtfsrt;

import uk.org.siri.siri20.EstimatedVehicleJourney;

class EstimatedVehicleJourneyData {

  private final TripAndVehicleKey key;

  private final long timestamp;

  private final EstimatedVehicleJourney estimatedVehicleJourney;

  private final String producer;

  public EstimatedVehicleJourneyData(TripAndVehicleKey key, long timestamp,
                                     EstimatedVehicleJourney estimatedVehicleJourney, String producer) {
    this.key = key;
    this.timestamp = timestamp;
    this.estimatedVehicleJourney = estimatedVehicleJourney;
    this.producer = producer;
  }
  
  public TripAndVehicleKey getKey() {
    return key;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public EstimatedVehicleJourney geEstimatedVehicleJourney() {
    return estimatedVehicleJourney;
  }
  
  public String getProducer() {
    return producer;
  }
}
