package org.entur.kishar.gtfsrt;

import org.entur.avro.realtime.siri.model.FramedVehicleJourneyRefRecord;
import org.entur.avro.realtime.siri.model.InfoLinkRecord;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import org.entur.avro.realtime.siri.model.TranslatedStringRecord;

import java.util.List;

public class Helper {

    static String situationNumberValue = "TST:SituationNumber:1234";
    static String summaryValue = "Situation summary";
    static String descriptionValue = "Situation description";

    static PtSituationElementRecord createPtSituationElement(String datasource) {

        String situationNumber =situationNumberValue;

        TranslatedStringRecord summary = TranslatedStringRecord.newBuilder()
                .setValue(summaryValue)
                .build();

        TranslatedStringRecord description = TranslatedStringRecord.newBuilder()
                .setValue(descriptionValue)
                .build();

        String requestorRef = datasource;

        InfoLinkRecord infoLinksType = InfoLinkRecord
            .newBuilder()
                .setUri("http://www.example.com")
                .build()
            ;

        PtSituationElementRecord ptSituation = PtSituationElementRecord.newBuilder()
                .setSituationNumber(situationNumber)
                .setSummaries(List.of(summary))
                .setDescriptions(List.of(description))
                .setParticipantRef(requestorRef)
                .setInfoLinks(List.of(infoLinksType))
                .build();

        return ptSituation;
    }

    static FramedVehicleJourneyRefRecord createFramedVehicleJourneyRefStructure(String datedVehicleJourneyRef) {
        if (datedVehicleJourneyRef == null) {
            return null;
        }

        return FramedVehicleJourneyRefRecord.newBuilder()
                .setDatedVehicleJourneyRef(datedVehicleJourneyRef)
                .setDataFrameRef("2018-12-12")
                .build();
    }

}
