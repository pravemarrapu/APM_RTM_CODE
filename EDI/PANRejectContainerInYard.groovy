/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 * 31-Jul-2014 CSDV-2007 record soft error if either full or empty ctr is in the yard
 *
 */

package com.navis.apex.groovy.mv2_rwg.preannouncements.validation.bizflow.tasks;

import com.navis.argo.ContextHelper;
import com.navis.argo.business.api.ArgoUtils;
import com.navis.argo.business.atoms.FreightKindEnum;
import com.navis.argo.business.model.LocPosition;
import com.navis.argo.business.reference.Container;
import com.navis.external.road.AbstractGateTaskInterceptor;
import com.navis.external.road.EGateTaskInterceptor;
import com.navis.framework.business.Roastery;
import com.navis.framework.util.TransactionParms;
import com.navis.framework.util.message.MessageCollector;
import com.navis.framework.util.message.MessageCollectorFactory;
import com.navis.framework.util.message.MessageLevel;
import com.navis.inventory.business.api.UnitFinder;
import com.navis.inventory.business.atoms.UfvTransitStateEnum;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.road.RoadPropertyKeys;
import com.navis.road.business.util.RoadBizUtil;
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.external.framework.util.ExtensionUtils;

public class PANRejectContainerInYard extends AbstractGateTaskInterceptor implements EGateTaskInterceptor {

    public void execute(TransactionAndVisitHolder inWfCtx) {

        MessageCollector origMsgCollector = RoadBizUtil.getMessageCollector();
        MessageCollector thisTaskMsgCollector =  MessageCollectorFactory.createMessageCollector();
        TransactionParms.getBoundParms().setMessageCollector(thisTaskMsgCollector);


        Container ctr = inWfCtx.getTran().getTranContainer();
        if (ctr != null) {
            UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
            Unit unit = unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), ctr);
            if (unit != null) {
                UnitFacilityVisit ufv = unit.getUfvForFacilityLiveOnly(inWfCtx.getGate().getGateFacility());

                //All ACTIVE transit states: EC/IN, EC/OUT, YARD, LOADED
                if (UfvTransitStateEnum.S30_ECIN.equals(ufv.getUfvTransitState()) || UfvTransitStateEnum.S40_YARD.equals(ufv.getUfvTransitState())
                        || UfvTransitStateEnum.S50_ECOUT.equals(ufv.getUfvTransitState()) || UfvTransitStateEnum.S60_LOADED.equals(ufv.getUfvTransitState())) {
                    LocPosition pos = unit.findCurrentPosition();

                    String[] params = new String[2];
                    params[0] = ctr.getEqIdFull();
                    params[1] = pos.toString();

                    RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE, RoadPropertyKeys.GATE__CTR_IN_YARD,
                            null, params);
                }
            }

            if (thisTaskMsgCollector.hasError()){

                //if (unit != null && FreightKindEnum.FCL.equals(unit.getUnitFreightKind())) {
                if (unit != null) {
                    _panGateTaskHelper.recordPreanError(inWfCtx, getClass().getSimpleName(), RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector));

                }
                else {
                    ArgoUtils.appendMessagesToCollector(origMsgCollector,thisTaskMsgCollector.getMessages());
                }
            }

            log("Details about Transaction :: START");
            log("Truck Transaction :: "+inWfCtx.getTran());
            log("Current Facility :: "+ContextHelper.getThreadFacility());
            log("Current Carrier Visit :: "+inWfCtx.getTran().setTranCarrierVisit(null));
            log("Details about Transaction :: END");

            TransactionParms.getBoundParms().setMessageCollector(origMsgCollector);
        }
    }
    private static def _panGateTaskHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateTaskHelper");
}