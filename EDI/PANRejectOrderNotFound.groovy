/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 * 10/11/14 CSDV-2415  check prean eqo nbr if tran eqo nbr is null
 *
 *  Modified By: Pradeep Arya
 *  Date 20-Mar-17 WF#801147 - Order validation for pickup
 *  13-Sept-17 - WF#891531 - ERO validation for dropoff empties
 *  18-Sept-17 - WF#895061 - ITT - RE RAIL: no error on PREAN unknown booking
 */

package com.navis.apex.groovy.mv2_rwg.preannouncements.validation.bizflow.tasks

import com.navis.argo.ContextHelper
import com.navis.external.framework.util.ExtensionUtils
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.external.road.EGateTaskInterceptor
import com.navis.framework.util.TransactionParms
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollector
import com.navis.framework.util.message.MessageCollectorFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.orders.business.eqorders.EquipmentDeliveryOrder
import com.navis.orders.business.eqorders.EquipmentReceiveOrder
import com.navis.road.RoadPropertyKeys
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum
import com.navis.road.business.util.RoadBizUtil
import com.navis.road.business.workflow.IUnitBean
import com.navis.road.business.workflow.TransactionAndVisitHolder
import org.apache.fop.viewer.UserMessage

public class PANRejectOrderNotFound extends AbstractGateTaskInterceptor implements EGateTaskInterceptor {

    public void execute(TransactionAndVisitHolder inWfCtx) {

        MessageCollector origMsgCollector = getMessageCollector();
        MessageCollector thisTaskMsgCollector = MessageCollectorFactory.createMessageCollector();
        TransactionParms.getBoundParms().setMessageCollector(thisTaskMsgCollector);

        executeExternal(inWfCtx.getTran(), inWfCtx.getAppt());

        /*if (thisTaskMsgCollector.hasError()) {
            List<UserMessage> severMessages = thisTaskMsgCollector.getMessages(MessageLevel.SEVERE);
            if (severMessages != null && severMessages.size() > 0) {
                for (UserMessage curretUserMessage : severMessages) {
                    if (curretUserMessage.getMessageKey().getKey().equalsIgnoreCase("PANRejectLineNotFound") || curretUserMessage.getMessageKey().getKey().equalsIgnoreCase("PANRejectEquipmentNotFound")) {
                        _panGateTaskHelper.recordPreanError(inWfCtx, curretUserMessage.getMessageKey().getKey(), curretUserMessage);
                    } else {
                        _panGateTaskHelper.recordPreanError(inWfCtx, getClass().getSimpleName(), curretUserMessage);
                    }

                }
            }


        }*/
        // WF#895061
        if (thisTaskMsgCollector.hasError()) {
            if (RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector).getMessageKey().getKey().equalsIgnoreCase("PANRejectLineNotFound") ||
                    RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector).getMessageKey().getKey().equalsIgnoreCase("PANRejectEquipmentNotFound")) {
                _panGateTaskHelper.recordPreanError(inWfCtx, RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector).getMessageKey().getKey(), RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector));
            }
            else
                _panGateTaskHelper.recordPreanError(inWfCtx, getClass().getSimpleName(), RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector));
        }
        TransactionParms.getBoundParms().setMessageCollector(origMsgCollector);
    }

    public void executeExternal(IUnitBean inTran, GateAppointment inPrean) {

        String eqoNbr = inTran.getTranEqoNbr() != null ? inTran.getTranEqoNbr() : inPrean.getFieldString(_panFields.PREAN_EQO_NBR);
        String ediEqoNbr = inPrean.getFieldString(_panFields.PREAN_EQO_NBR);
        String apptEqoNbr = inTran.getTranEqoNbr();
        //log("ediEqoNbr:$ediEqoNbr + apptEqoNbr:$apptEqoNbr");
        String lineId = inTran.getTranLineId();
        log("ediEqoNbr:$ediEqoNbr + apptEqoNbr:$apptEqoNbr :: line Id:: $lineId Equipment Selected :: "+inTran.getTranEq());
        //if DM already reserved against an EDO N4 always populate the transaction with that
        //checking if tran/appt EDO are different and error out if the edi EDO doesn't exist
        if(apptEqoNbr && ediEqoNbr && ediEqoNbr != apptEqoNbr) {
            //WF#891531 - For RM error out if ERO does not exist
            if(inTran.isReceival() && inPrean.getGapptTranType() == TruckerFriendlyTranSubTypeEnum.DOM) {
                EquipmentReceiveOrder ero = EquipmentReceiveOrder.findEroByUniquenessCriteria(eqoNbr, inTran.getTranLine());
                if (ero == null) {
                    inTran.setTranEqoNbr(eqoNbr);
                    RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE,
                            RoadPropertyKeys.GATE__ORDER_NOT_FOUND, null, [eqoNbr]);
                }
            }
            else {
                EquipmentDeliveryOrder eqo = EquipmentDeliveryOrder.findEquipmentDeliveryOrder(ediEqoNbr, inTran.getTranLine());
                if (eqo == null) {
                    inTran.setTranEqoNbr(ediEqoNbr);
                    RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE,
                            RoadPropertyKeys.GATE__ORDER_NOT_FOUND, null, [eqoNbr]);
                }
            }
        }
        else if (eqoNbr != null && inTran.getTranEqo() == null) {

            if (lineId != null) {
                RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE,
                        RoadPropertyKeys.GATE__ORDER_NOT_FOUND_FOR_LINE, null, [eqoNbr, lineId]);
            } else {
                RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE,
                        RoadPropertyKeys.GATE__ORDER_NOT_FOUND, null, [eqoNbr]);
            }
        }

        if (ediEqoNbr == null && apptEqoNbr == null) {
            RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE,
                    RoadPropertyKeys.GATE__ORDER_NOT_FOUND, null, null);
        }

        if (inTran.getTranEq() != null && inTran.getTranEq().getEqEquipType() != null && inTran.getTranEq().getEqEquipType().isUnknownType()) {
            RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE,
                    PropertyKeyFactory.valueOf("PANRejectEquipmentNotFound"), null, null);
        }
    }

    private static def _panGateTaskHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateTaskHelper");
    private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
}