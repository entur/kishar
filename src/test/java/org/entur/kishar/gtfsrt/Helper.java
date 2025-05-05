package org.entur.kishar.gtfsrt;

import org.entur.avro.realtime.siri.converter.jaxb2avro.Jaxb2AvroConverter;
import org.entur.avro.realtime.siri.model.PtSituationElementRecord;
import uk.org.siri.siri21.AffectedLineStructure;
import uk.org.siri.siri21.AffectsScopeStructure;
import uk.org.siri.siri21.DefaultedTextStructure;
import uk.org.siri.siri21.LineRef;
import uk.org.siri.siri21.PtSituationElement;
import uk.org.siri.siri21.ReportTypeEnumeration;
import uk.org.siri.siri21.RequestorRef;
import uk.org.siri.siri21.SituationNumber;

public class Helper {

    static String situationNumberValue = "TST:SituationNumber:1234";
    static String summaryValue = "Situation summary";
    static String descriptionValue = "Situation description";

    static PtSituationElementRecord createPtSituationElement(String datasource) {

        PtSituationElement siriSituation = new PtSituationElement();
        SituationNumber situationNumber = new SituationNumber();
        situationNumber.setValue(situationNumberValue);
        RequestorRef participant = new RequestorRef();
        participant.setValue(datasource);
        siriSituation.setParticipantRef(participant);

        siriSituation.setReportType(ReportTypeEnumeration.GENERAL);

        siriSituation.setSituationNumber(situationNumber);
        DefaultedTextStructure summary = new DefaultedTextStructure();
        summary.setValue(summaryValue);

        DefaultedTextStructure description = new DefaultedTextStructure();
        description.setValue(descriptionValue);
        description.setLang("no");


        siriSituation.getSummaries().add(summary);
        siriSituation.getDescriptions().add(description);
        AffectsScopeStructure affects = new AffectsScopeStructure();
        AffectsScopeStructure.Networks network = new AffectsScopeStructure.Networks();
        AffectsScopeStructure.Networks.AffectedNetwork affectedNetwork = new AffectsScopeStructure.Networks.AffectedNetwork();
        AffectedLineStructure affectedLine = new AffectedLineStructure();
        LineRef lineRef = new LineRef();
        lineRef.setValue("TST:Line:1234");
        affectedLine.setLineRef(lineRef);
        affectedNetwork.getAffectedLines().add(affectedLine);
        network.getAffectedNetworks().add(affectedNetwork);
        affects.setNetworks(network);
        siriSituation.setAffects(affects);


        return Jaxb2AvroConverter.convert(siriSituation);
    }
}
