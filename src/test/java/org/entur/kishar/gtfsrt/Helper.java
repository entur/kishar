package org.entur.kishar.gtfsrt;

import uk.org.siri.www.siri.*;

public class Helper {

    static String situationNumberValue = "TST:SituationNumber:1234";
    static String summaryValue = "Situation summary";
    static String descriptionValue = "Situation description";

    static PtSituationElementStructure createPtSituationElement(String datasource) {

        EntryQualifierStructure situationNumber = EntryQualifierStructure.newBuilder()
                .setValue(situationNumberValue)
                .build();

        DefaultedTextStructure summary = DefaultedTextStructure.newBuilder()
                .setValue(summaryValue)
                .build();

        DefaultedTextStructure description = DefaultedTextStructure.newBuilder()
                .setValue(descriptionValue)
                .build();

        ParticipantRefStructure requestorRef = ParticipantRefStructure.newBuilder()
                .setValue(datasource)
                .build();

        PtSituationElementStructure.InfoLinksType infoLinksType = PtSituationElementStructure.InfoLinksType
            .newBuilder()
            .addInfoLink(InfoLinkStructure.newBuilder()
                .setUri("http://www.example.com")
                .build()
            )
            .build();

        PtSituationElementStructure ptSituation = PtSituationElementStructure.newBuilder()
                .setSituationNumber(situationNumber)
                .addSummary(summary)
                .addDescription(description)
                .setParticipantRef(requestorRef)
                .setInfoLinks(infoLinksType)
                .build();


        return ptSituation;
    }

    static FramedVehicleJourneyRefStructure createFramedVehicleJourneyRefStructure(String datedVehicleJourneyRef) {
        if (datedVehicleJourneyRef == null) {
            return null;
        }

        DataFrameRefStructure dataFrameRef = DataFrameRefStructure.newBuilder()
                .setValue("2018-12-12")
                .build();

        return FramedVehicleJourneyRefStructure.newBuilder()
                .setDatedVehicleJourneyRef(datedVehicleJourneyRef)
                .setDataFrameRef(dataFrameRef)
                .build();
    }

    static LineRefStructure createLineRef(String lineRefValue) {
        return LineRefStructure.newBuilder()
                .setValue(lineRefValue)
                .build();
    }
}
