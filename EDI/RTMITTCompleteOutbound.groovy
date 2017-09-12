package com.weserve.APM.EDI

import com.navis.argo.ContextHelper
import com.navis.argo.business.api.IEvent
import com.navis.argo.business.atoms.WiMoveStageEnum
import com.navis.external.framework.util.ExtensionUtils
import com.navis.external.services.AbstractGeneralNoticeCodeExtension
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.moves.WorkInstruction
import com.navis.inventory.business.units.Unit
import com.navis.inventory.presentation.WorkInstructionViewHelper
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.atoms.TranSubTypeEnum
import com.navis.road.business.atoms.TransactionClassEnum
import com.navis.road.business.atoms.TruckVisitStatusEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.services.business.event.GroovyEvent
import com.navis.services.business.rules.EventType

/*
 AUTHOR      : Pradeep Arya
 Date Written: 03/15/2017
 Requirements: Complete ITT Outbound process on last container placed on MTS

 Deployment Steps:
 a) If the groovy source is provided, then follow the given below steps.
 b) Administration -> System -> Groovy Plug-ins
 c) Click on + (Add) button.
 d) In short description paste the groovy name as RTMITTCompleteOutbound
 e) Paste the code Groovy Code Section and click on Save.

 Trigger:
   a) Attach to event UNIT_DELIVER in General Notices as execute code extension
   b) select RTMITTCompleteOutbound

 *
 */

class RTMITTCompleteOutbound extends AbstractGeneralNoticeCodeExtension {
	public void execute(GroovyEvent inEvent) {
		this.log("RTMITTCompleteOutbound execute started:" + new Date());
		
		Unit unit  = inEvent.getEntity();
        IEvent event = inEvent.getEvent();
        if(unit) {
            def ittUtils =  ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "RTMITTUtils");

          //  TruckVisitStatusEnum[]tvStatusEnum = [TruckVisitStatusEnum.OK];
           // TranStatusEnum[]tranStatusEnum = [TranStatusEnum.OK];
            //TruckTransaction tran = TruckTransaction.findLatestTruckTransactionByCtrNbr(unit.getUnitId(), tvStatusEnum, tranStatusEnum);
            String MTSTrkLic = unit.getFieldValue(MetafieldIdFactory.valueOf("customFlexFields.unitCustomDFFITTMTS"));
            List tvds = TruckVisitDetails.findTVActiveByTruckLicenseNbr(MTSTrkLic);
            TruckVisitDetails tvd = tvds.size() > 0 ? tvds.get(0) : null;
            log("tvd:$tvd");
            //run the logic only for RAIL-ITT gate
            if(tvd && tvd.getTvdtlsGate().getGateId().equals(GATE_ID)) {

                boolean isLastCntrOnMTS = false;
                List ctrList = [];
                Set transList = tvd.getActiveTransactions(TransactionClassEnum.PICKUP);
                log("transList:$transList");

                for(TruckTransaction tran1 : transList) {
                    ctrList.add(tran1.getTranUnit().getUnitId());
                    //log("ufv t-state:" + tran1.getTranUfv().getUfvTransitState());
                    List wis = tran1.getTranUfv().getCurrentWiList();
                    WorkInstruction wi = wis.size() > 0 ? wis.get(0) : null;
                    if(wi) {
                        log("Move stage:" + wi.getWiMoveStage());
                        log("Tran Ufv:" + tran1.getTranUfv());
                    }
                    if(tran1.getTranUfv().getUfvTransitState() == UfvTransitStateEnum.S60_LOADED)
                        isLastCntrOnMTS = true;
                    else {
                        isLastCntrOnMTS = false;
                        break;
                    }

                }
                log("Transaction Ctrs:" + ctrList);
                log("isLastCntrOnMTS:$isLastCntrOnMTS");

                if(isLastCntrOnMTS || transList.size() == 1) {
                    ittUtils.processTruck(GATE_ID, YARD_STAGE_ID, tvd.getCvdGkey(), tvd.getTruckLicenseNbr(), ctrList);
                    ittUtils.processTruck(GATE_ID, OUTGATE_STAGE_ID, tvd.getCvdGkey(), tvd.getTruckLicenseNbr(), ctrList);
                    unit.setFieldValue(MetafieldIdFactory.valueOf("customFlexFields.unitCustomDFFITTLastCntr"), true);

                    EventType eventType = EventType.findEventType("ITT_LASTMTS_DELIVER");
                    unit.recordUnitEvent(eventType, null, "Recorded by Groovy");
                }
            }

        }
		
		this.log("RTMITTCompleteOutbound execute ended:" + new Date());
	}

    private final String GATE_ID = "RAIL_ITT";
    private final String YARD_STAGE_ID = "yard";
    private final String OUTGATE_STAGE_ID = "Outgate";
	
}