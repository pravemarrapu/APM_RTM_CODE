import com.navis.argo.ContextHelper
import com.navis.argo.business.api.IEvent
import com.navis.argo.business.atoms.WiMoveStageEnum
import com.navis.external.framework.util.ExtensionUtils
import com.navis.external.services.AbstractGeneralNoticeCodeExtension
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.inventory.business.moves.WorkInstruction
import com.navis.inventory.business.units.Unit
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.atoms.TransactionClassEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.services.business.event.GroovyEvent
import com.navis.services.business.rules.EventType

/*
 AUTHOR      : Pradeep Arya
 Date Written: 16-Mar-17
 Requirements: Complete ITT Inbound process on last container unloaded from MTS

 Deployment Steps:
 a) If the groovy source is provided, then follow the given below steps.
 b) Administration -> System -> Groovy Plug-ins
 c) Click on + (Add) button.
 d) In short description paste the groovy name as RTMITTCompleteInbound
 e) Paste the code Groovy Code Section and click on Save.

 Trigger:
   a) Attach to event UNIT_RECEIVE in General Notices as execute code extension
   b) select RTMITTCompleteInbound

 *
 */

class RTMITTCompleteInbound extends AbstractGeneralNoticeCodeExtension {
	public void execute(GroovyEvent inEvent) {
		this.log("RTMITTCompleteInbound execute started:" + new Date());
		
		Unit unit  = inEvent.getEntity();
        IEvent event = inEvent.getEvent();
        if(unit) {
            def ittUtils =  ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "RTMITTUtils");
            String MTSTrkLic = unit.getFieldValue(MetafieldIdFactory.valueOf("customFlexFields.unitCustomDFFITTMTS"));
            List tvds = TruckVisitDetails.findTVActiveByTruckLicenseNbr(MTSTrkLic);
            TruckVisitDetails tvd = tvds.size() > 0 ? tvds.get(0) : null;
            log("tvd:$tvd");
            //run the logic only for RAIL-ITT gate
            if(tvd && tvd.getTvdtlsGate().getGateId().equals(GATE_ID)) {

                Set transList = tvd.getActiveTransactions(TransactionClassEnum.DROPOFF);
                boolean isLastCntrOnMTS = false;
                List ctrList = [];
                for(TruckTransaction tran1 : transList) {
                    ctrList.add(tran1.getTranUnit().getUnitId());
                    //log("ufv t-state:" + tran1.getTranUfv().getUfvTransitState());
                    List wis = tran1.getTranUfv().getCurrentWiList();
                    log("wis:$wis");
                    WorkInstruction wi = wis.size() > 0 ? wis.get(0) : null;
                    if(wi) {
                        log("Move stage:" + wi.getWiMoveStage());
                        log("Tran Ufv:" + tran1.getTranUfv());
                    }
                    if(!wi || wi.getWiMoveStage() == WiMoveStageEnum.COMPLETE) {
                        isLastCntrOnMTS = true;
                    }
                    else {
                        isLastCntrOnMTS = false;
                        break;
                    }
                }
                log("Transaction Ctrs:" + ctrList);

                if(isLastCntrOnMTS || transList.size() == 1) {
                    String response = ittUtils.processTruck(GATE_ID, OUTGATE_STAGE_ID, tvd.getCvdGkey(), tvd.getTruckLicenseNbr(), ctrList);
                    log("RTMITTCompleteInbound ProcessTruck Response:$response");
                    EventType eventType = EventType.findEventType("ITT_LASTMTS_RECEIVE");
                    unit.recordUnitEvent(eventType, null, "Recorded by Groovy");
                }
            }

        }
		
		this.log("RTMITTCompleteInbound execute ended:" + new Date());
	}

    private final String GATE_ID = "RAIL_ITT";
    private final String OUTGATE_STAGE_ID = "Outgate";
	
}