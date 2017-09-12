

import com.navis.argo.ContextHelper
import com.navis.argo.business.api.GroovyApi
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.road.RoadApptsField
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.atoms.AppointmentStateEnum
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat

public class RTMITTUtils extends AbstractExtensionCallback {
    /*
   then 20'40'20'40'20' will be 1-3-5-7-9
    <gate>
        <create-truck-visit>
            <gate-id>MG</gate-id>
            <stage-id>office</stage-id>
            <lane-id>L1</lane-id>
            <truck license-nbr="MFE590P" trucking-co-id="ACME"/>
            <driver license-nbr="PS9VF"/>
            <truck-visit gos-tv-key="186643" bat-nbr="F124"/>
            <timestamp>2007-10-01T17:49:22</timestamp>
        </create-truck-visit>
    </gate>
  */
    public Long createTruckVisit(String gateId, String MTSTrkLic) {

        //create truck visit
        String dateStr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());
        StringWriter writer = new StringWriter();
        writer.write('<?xml version="1.0" encoding="utf-8" ?>\n');
        MarkupBuilder builder = new MarkupBuilder(writer);
        builder.setDoubleQuotes(true);
        builder.'gate'
        {
            'create-truck-visit' {
                'gate-id'(gateId)
                'stage-id'(INGATE_STAGE_ID)
                'exchange-area-id'(EXCHANGE_AREA_ID)
                'truck'('license-nbr': MTSTrkLic)
                'timestamp'(dateStr)
            }
        }
        log(writer.toString());
        String requestXml = writer.toString();
        String response = new GroovyApi().getGroovyClassInstance("APMTWebServiceUtil").sendN4WSRequest(requestXml);
        log("response create-truck-visit:" + response);
        Node rootNode = new XmlParser().parseText(response);
        String tvGkey = rootNode.value().getAt(0).value().getAt(0).attributes().'tv-key';
        log("Truck Visit gkey:" + tvGkey);
        // TruckVisitDetails

        return tvGkey.toLong();
    }

    /*
     <submit-transaction>
        <gate-id>MOB GATE</gate-id>
        <stage-id>ingate</stage-id>
        <lane-id>206</lane-id>
        <truck license-nbr="1062430" />
        <driver card-id="2D12F200FBFF" />
        <truck-visit tv-key="203839" scale-weight="91740" scale-weight-unit="lb" />
        <truck-transaction tran-key="337696">
          <container eqid="CAXU8150232" on-chassis-id="BWIF124015" />
          <chassis eqid="BWIF124015" is-owners="true" />
        </truck-transaction>
      </submit-transaction>
    </gate>
     */
    public String submitTransaction(String gateId, String gateStageId, Long tvdGkey, Long apptNbr, String MTSTrkLic, String posOnTrk) {

        StringWriter writer = new StringWriter();
        writer.write('<?xml version="1.0" encoding="utf-8" ?>\n');
        MarkupBuilder builder = new MarkupBuilder(writer);
        builder.setDoubleQuotes(true);
        builder.'gate'
        {
            'submit-transaction' {
                'gate-id'(gateId)
                'stage-id'(gateStageId)
                'truck'('license-nbr': MTSTrkLic)
                'truck-visit'('tv-key' : tvdGkey)
                'truck-transaction'('truck-position' : posOnTrk, 'appointment-nbr': apptNbr)
            }
        }

        String requestXml = writer.toString();
        log("requestXml: " + requestXml);
        String response = new GroovyApi().getGroovyClassInstance("APMTWebServiceUtil").sendN4WSRequest(requestXml);
        log("response submit-transaction:$response");
        // Node rootNode = new XmlParser().parseText(response);
        return response;

    }

    /*
      <gate>
       <process-truck>
         <gate-id>RAIL-ITT</gate-id>
         <stage-id>Ingate</stage-id>
         <truck-visit appointment-nbr="542524" />
       </process-truck>
      </gate>
      <gate>*/
    public String processTruck(String gateId, String gateStageId, Long tvdGkey, String MTSTrkLic, List ctrList) {
        StringWriter writer = new StringWriter();
        writer = new StringWriter();
        writer.write('<?xml version="1.0" encoding="utf-8" ?>\n');
        MarkupBuilder builder = new MarkupBuilder(writer);
        builder.setDoubleQuotes(true);
        builder.'gate'
        {
            'process-truck' {
                'gate-id'(gateId)
                'stage-id'(gateStageId)
                'truck'('license-nbr': MTSTrkLic)
                'truck-visit'('tv-key': tvdGkey)
                'equipment' {
                    ctrList.each {ctrId ->
                     'container'('eqid' : ctrId)
                    }
                }
            }
        }

        String requestXml = writer.toString();
        log(requestXml);

        String response = new GroovyApi().getGroovyClassInstance("APMTWebServiceUtil").sendN4WSRequest(requestXml);
        log("response:" + response);
        Node rootNode = new XmlParser().parseText(response);
        return response;
    }

    /*
    <gate>
        <notify-arrival>
            <gate-id>RAIL-ITT</gate-id>
            <stage-id>yard</stage-id>
            <exchange-area-id>ITT</exchange-area-id>
            <lane-id>ITT01</lane-id>
            <truck license-nbr="MTS67"/>
        </notify-arrival>
    </gate>
     */
    public String confirmArrival(String gateId, String gateStageId, String MTSTrkLic, String exchangeAreaId, String exchangeLaneId) {
        StringWriter writer = new StringWriter();
        writer = new StringWriter();
        writer.write('<?xml version="1.0" encoding="utf-8" ?>\n');
        MarkupBuilder builder = new MarkupBuilder(writer);
        builder.setDoubleQuotes(true);
        builder.'gate'
        {
            'notify-arrival' {
                'gate-id'(gateId)
                'stage-id'(gateStageId)
                'exchange-area-id'(exchangeAreaId)
                'lane-id'(exchangeLaneId)
                'truck'('license-nbr': MTSTrkLic)
            }
        }

        String requestXml = writer.toString();
        log(requestXml);

        String response = new GroovyApi().getGroovyClassInstance("APMTWebServiceUtil").sendN4WSRequest(requestXml);
        log("response:" + response);

        return response
    }

    public GateAppointment findActiveGateAppointmentByCtrNbr(String inCtrNbr)
    {
        MetafieldId ruleField = MetafieldIdFactory.getCompoundMetafieldId(RoadApptsField.GAPPT_TIME_SLOT, RoadApptsField.ATSLOT_QUOTA_RULE);
        MetafieldId ruleSetField = MetafieldIdFactory.getCompoundMetafieldId(ruleField, RoadApptsField.ARULE_QUOTA_RULE_SET);
        MetafieldId facilityField = MetafieldIdFactory.getCompoundMetafieldId(ruleSetField, RoadApptsField.ARULESET_FACILITY);
        DomainQuery dq = QueryUtils.createDomainQuery("GateAppointment")
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_CTR_ID, inCtrNbr))
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_STATE, AppointmentStateEnum.CREATED))
                .addDqPredicate(PredicateFactory.eq(facilityField, ContextHelper.getThreadFacility().getFcyGkey()));
        return (GateAppointment)Roastery.getHibernateApi().getUniqueEntityByDomainQuery(dq);
    }

    private final String GATE_ID = "RAIL-ITT";
    private final String INGATE_STAGE_ID = "Ingate";
    private final String YARD_STAGE_ID = "yard";
    private final String EXCHANGE_AREA_ID = "ITT";

}

  