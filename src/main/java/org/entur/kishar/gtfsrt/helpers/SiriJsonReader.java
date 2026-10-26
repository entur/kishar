package org.entur.kishar.gtfsrt.helpers;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.entur.avro.realtime.siri.model.EstimatedVehicleJourneyRecord;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import org.entur.avro.realtime.siri.model.VehicleActivityRecord;

import java.io.IOException;

/**
 * Same as {@link org.entur.avro.realtime.siri.helper.JsonReader}, but reuses one JsonDecoder per thread
 * instead of creating a new one per message. Creating a JsonDecoder rebuilds the parsing grammar for the
 * whole schema, which dominated the cost of reading small messages.
 */
public class SiriJsonReader {

    private static final DatumReader<VehicleActivityRecord> vmReader = new SpecificDatumReader<>(VehicleActivityRecord.class);
    private static final DatumReader<EstimatedVehicleJourneyRecord> etReader = new SpecificDatumReader<>(EstimatedVehicleJourneyRecord.class);
    private static final DatumReader<PtSituationElementRecord> sxReader = new SpecificDatumReader<>(PtSituationElementRecord.class);

    private static final ThreadLocal<JsonDecoder> vmDecoder = decoderFor(VehicleActivityRecord.getClassSchema());
    private static final ThreadLocal<JsonDecoder> etDecoder = decoderFor(EstimatedVehicleJourneyRecord.getClassSchema());
    private static final ThreadLocal<JsonDecoder> sxDecoder = decoderFor(PtSituationElementRecord.getClassSchema());

    public static VehicleActivityRecord readVehicleActivity(String json) throws IOException {
        return vmReader.read(null, vmDecoder.get().configure(json));
    }

    public static EstimatedVehicleJourneyRecord readEstimatedVehicleJourney(String json) throws IOException {
        return etReader.read(null, etDecoder.get().configure(json));
    }

    public static PtSituationElementRecord readPtSituationElement(String json) throws IOException {
        return sxReader.read(null, sxDecoder.get().configure(json));
    }

    private static ThreadLocal<JsonDecoder> decoderFor(Schema schema) {
        return ThreadLocal.withInitial(() -> {
            try {
                return DecoderFactory.get().jsonDecoder(schema, "");
            } catch (IOException e) {
                throw new IllegalStateException("Could not create JsonDecoder for " + schema.getName(), e);
            }
        });
    }
}
