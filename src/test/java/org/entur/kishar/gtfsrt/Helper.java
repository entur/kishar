package org.entur.kishar.gtfsrt;

import uk.org.siri.siri20.*;

public class Helper {

    static String situationNumberValue = "TST:SituationNumber:1234";
    static String summaryValue = "Situation summary";
    static String descriptionValue = "Situation description";
    static String datasource = "RUT";

    static PtSituationElement createPtSituationElement() {
        PtSituationElement ptSituation = new PtSituationElement();

        SituationNumber situationNumber = new SituationNumber();
        situationNumber.setValue(situationNumberValue);
        ptSituation.setSituationNumber(situationNumber);

        DefaultedTextStructure summary = new DefaultedTextStructure();
        summary.setValue(summaryValue);
        ptSituation.getSummaries().add(summary);

        DefaultedTextStructure description = new DefaultedTextStructure();
        description.setValue(descriptionValue);
        ptSituation.getDescriptions().add(description);

        RequestorRef requestorRef = new RequestorRef();
        requestorRef.setValue(datasource);
        ptSituation.setParticipantRef(requestorRef);
        return ptSituation;
    }

    static FramedVehicleJourneyRefStructure createFramedVehicleJourneyRefStructure(String datedVehicleJourneyRef) {
        if (datedVehicleJourneyRef == null) {
            return null;
        }

        FramedVehicleJourneyRefStructure framedVehicleJourney = new FramedVehicleJourneyRefStructure();
        framedVehicleJourney.setDatedVehicleJourneyRef(datedVehicleJourneyRef);

        DataFrameRefStructure dataFrameRef = new DataFrameRefStructure();
        dataFrameRef.setValue("2018-12-12");
        framedVehicleJourney.setDataFrameRef(dataFrameRef);
        return framedVehicleJourney;
    }

    static LineRef createLineRef(String lineRefValue) {
        LineRef line = new LineRef();
        line.setValue(lineRefValue);
        return line;
    }
}
