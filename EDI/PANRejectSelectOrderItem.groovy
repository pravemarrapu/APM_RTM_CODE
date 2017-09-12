/*
 * @version $Id: PANRejectSelectOrderItem.groovy 261316 2016-10-18 15:05:23Z extroberso $
 *
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 *
*/


package com.navis.apex.groovy.mv2_rwg.preannouncements.validation.bizflow.tasks;


import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.navis.argo.ContextHelper;
import com.navis.argo.business.reference.EquipType;
import com.navis.argo.business.reference.LineOperator;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.external.road.AbstractGateTaskInterceptor;
import com.navis.external.road.EGateTaskInterceptor;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.util.TransactionParms;
import com.navis.framework.util.message.MessageCollector;
import com.navis.framework.util.message.MessageCollectorFactory;
import com.navis.framework.util.message.MessageCollectorUtils;
import com.navis.framework.util.message.MessageLevel;
import com.navis.inventory.business.imdg.HazardItem;
import com.navis.inventory.business.imdg.Hazards;
import com.navis.inventory.business.imdg.ImdgClass;
import com.navis.inventory.business.units.EqBaseOrder;
import com.navis.orders.OrdersField;
import com.navis.orders.OrdersPropertyKeys;
import com.navis.orders.business.OrdersFinderImpl;
import com.navis.orders.business.api.OrdersFinder;
import com.navis.orders.business.eqorders.EquipmentOrder;
import com.navis.orders.business.eqorders.EquipmentOrderItem;
import com.navis.road.RoadPropertyKeys;
import com.navis.road.business.adaptor.order.ReadOrderItem;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.TranSubTypeEnum;
import com.navis.road.business.model.TruckTransaction;
import com.navis.road.business.util.RoadBizUtil;
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.EqBaseOrderItem
import com.navis.inventory.business.units.UnitEquipment
import com.navis.argo.business.atoms.EquipmentOrderSubTypeEnum;

/* APMT
   CSDV-3990 Multiple Equipment Order Items on the Same ISO Type implementation

   06/Oct/2016 CSDV-3990 If unit has already been pre-advised to an order
                          if the ctr type still match (exactly or a substitute) don't proceed with the select logic;
   18/Oct/2016 CSDV-3990 Fixed null pointer exception bug introduced in the previous checkin

 */

public class PANRejectSelectOrderItem extends AbstractGateTaskInterceptor implements EGateTaskInterceptor {

    public void execute(TransactionAndVisitHolder inWfCtx) {

        MessageCollector origMsgCollector = getMessageCollector();
        MessageCollector thisTaskMsgCollector =  MessageCollectorFactory.createMessageCollector();
        TransactionParms.getBoundParms().setMessageCollector(thisTaskMsgCollector);

        //addUniquenessFields(inWfCtx);
        executeCustomizableInternal(inWfCtx);

        if (thisTaskMsgCollector.hasError()){

            if (thisTaskMsgCollector.containsMessage(OrdersPropertyKeys.ERRKEY__MATCHING_EQOI_NOT_FOUND) && thisTaskMsgCollector.getMessageCount(MessageLevel.SEVERE) == 1)  {

                _panGateTaskHelper.recordPreanError(inWfCtx, getClass().getSimpleName(), RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector));
            }
            else {
                MessageCollectorUtils.appendMessages(origMsgCollector, thisTaskMsgCollector);

            }

            TransactionParms.getBoundParms().setMessageCollector(origMsgCollector);

        }


    }

    private void addUniquenessFields(TransactionAndVisitHolder inDao) {

        /*
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_TEMP_REQUIRED);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_VENT_REQUIRED);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_CO2_REQUIRED);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_O2_REQUIRED);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_HUMIDITY_REQUIRED);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_HAZARDS);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_IS_OOG);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_OOG_FRONT_CM);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_OOG_BACK_CM);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_OOG_LEFT_CM);
        inDao.addCustomCodeResponse(com.navis.orders.OrdersField.EQOI_OOG_RIGHT_CM);
        inDao.addCustomCodeResponse(com.navis.orders.Order
        sField.EQOI_OOG_TOP_CM);
        */
    }

    //From SelectOrderItem
    public void executeCustomizableInternal(TransactionAndVisitHolder inDao) {

        if (inDao.getTran().getTranEqo() != null) {
            //RoadBizUtil.callCustomCode(inDao, inCustomCode, BEAN_ID);
            List<MetafieldId> eqoiUniquenessFields = new ArrayList<MetafieldId>();
            for (Object fieldId : inDao.getCustomCodeResponse()) {
                eqoiUniquenessFields.add((MetafieldId) fieldId);
            }

            Unit unit = inDao.getTran().getTranUnit();
            EquipmentOrderItem currentUnitEqoi = null;

            if (unit != null) {
                currentUnitEqoi = getCurrentUnitEqoi(inDao.getTran().getTranEqo(),unit);
            }

            //CSDV-3990 If unit has already been pre-advised to an order
            //if ctr type still match don't proceed with the select logic, else set DIFFERENT_CTR_TYPE to YES (to be picked up by PANRejectOrderNotMatchingPreadvise) and proceed

            if (currentUnitEqoi != null && inDao.getTran().getTranEqo() == EquipmentOrder.resolveEqoFromEqbo(currentUnitEqoi.getEqboiOrder())) {

                List<EquipmentOrderItem> eqois = null;

                if (inDao.getTran().isDelivery()) {

                    eqois = inDao.getTran().getTranEqo().findMatchingItemsDsp(inDao.getTran().getCtrType(), false);
                } else {
                    eqois = inDao.getTran().getTranEqo().findMatchingItemsRcv(inDao.getTran().getCtrType(), false);
                }

                if (!eqois.isEmpty()) {
                    if (eqois.contains(currentUnitEqoi)) {
                        inDao.getTran().setTranEqoItem(currentUnitEqoi);
                        inDao.getAppt().setGapptOrderItem(currentUnitEqoi);
                        return;
                    }
                }
                else {

                    RoadBizUtil.getMessageCollector().appendMessage(
                            MessageLevel.SEVERE,
                            OrdersPropertyKeys.ERRKEY__MATCHING_EQOI_NOT_FOUND,
                            null,
                            [inDao.getTran().getCtrType().getEqtypId()]
                    );
                    inDao.getTran().setTranEqoItem(null);
                    return;
                }
            }

            //The eqo item maybe set incorrectly in ReadOrderItem,
            //null it out, so SelectOrderItem logic works
            inDao.getTran().setTranEqoItem(null);

            //previous preadvice maybe cancelled (if it was initally done by a prean)
            /*if (currentUnitEqoi != null) {

                def preanUtils =  ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANPreanUtils");

                boolean isCancelled = preanUtils.cancelUnitToOrderPreadvice(inDao.getAppt().isReceival(), EquipmentOrder.resolveEqoFromEqbo(currentUnitEqoi.getEqboiOrder()), unit, inDao.getAppt().getGapptGate().getGateFacility());

                if (!isCancelled) {
                    return;
                }
            }*/



            List<EquipmentOrderItem> eqois = resolve(
                    inDao.getTran(),
                    inDao.getAppt(),
                    inDao.getTran().getTranEqo(),
                    eqoiUniquenessFields,
                    inDao.orderQtyCheckIncludesPreadvice(),
                    inDao.orderQtyCheckIncludesReserved()

            );
            inDao.setOrderItems(eqois);

        }
    }

    @Nullable
    private List<EquipmentOrderItem> resolve(
            TruckTransaction inTran,
            GateAppointment inGappt,
            EquipmentOrder inEqo,
            List<MetafieldId> inEqoiUniquenessFields,
            boolean inIncludePreadvicesAgainstOrder,
            boolean inIncludeReservedInTallyCheck
    ) {

        List<EquipmentOrderItem> eqois = null;
        EquipType eqType = null;
        if (inTran.getTranSubType().equals(TranSubTypeEnum.DC)||inTran.getTranSubType().equals(TranSubTypeEnum.RC)) {
            eqType = inTran.getChsType();
        } else {
            eqType = inTran.getCtrType();
        }

        if (eqType != null) {
            LineOperator line = inTran.getTranLine();
            final boolean orderItemsNonUnique = line != null && Boolean.TRUE.equals(line.getLineopOrderItemNotUnique());

            if (inTran.getTranEqoItem() == null) {

                if (inTran.isDelivery()) {
                    eqois = inEqo.findMatchingItemsDsp(eqType, inIncludeReservedInTallyCheck);
                } else {
                    eqois = inEqo.findMatchingItemsRcv(eqType, inIncludePreadvicesAgainstOrder);
                }
                //2012-08-13 lingaga - ARGO-38508 - Error key was changed from GATE__ORDER_ITEM_NOT_MATCHED to ERRKEY__MATCHING_EQOI_NOT_FOUND
                if (eqois.isEmpty()) {
                    RoadBizUtil.getMessageCollector().appendMessage(
                            MessageLevel.SEVERE,
                            OrdersPropertyKeys.ERRKEY__MATCHING_EQOI_NOT_FOUND,
                            null,
                            [eqType.getEqtypId()]
                    );
                }
                else if (eqois.size() == 1) {
                    inTran.setTranEqoItem(eqois.get(0));
                    ReadOrderItem.populateTranFromEqoItem(inTran, inTran.getTranEqoItem());
                } else if (orderItemsNonUnique) {

                    inTran.setTranEqoItem(resolveFromMultiples(eqois, inEqoiUniquenessFields));
                    ReadOrderItem.populateTranFromEqoItem(inTran, inTran.getTranEqoItem());
                }
                //2009-11-25 pmelappalayam ARGO-22170 Error message should be raised only when order items is greater than 1
                else if (eqois != null && eqois.size() > 1) {
                    RoadBizUtil.getMessageCollector().appendMessage(
                            MessageLevel.SEVERE,
                            RoadPropertyKeys.GATE__ORDER_ITEM_NOT_UNIQUE,
                            null,
                            [inTran.getTranEqo() != null ? inTran.getTranEqo().getEqboNbr() : "?"]
                    );
                }
            }
            //2011-08-07 psethuraman ARGO-32384: Updating eqo item to appt
            if (inGappt != null) {
                inGappt.setGapptOrderItem(inTran.getTranEqoItem());
            }
        }
        return eqois;
    }


    /**
     * The list of order items can only be further resolved if the fields marked for uniqueness are all unique and the order items have sequence nbrs
     *
     * @param inEqois
     * @param inEqoiUniquenessFields
     */
    @Nullable
    private EquipmentOrderItem resolveFromMultiples(List<EquipmentOrderItem> inEqois, List<MetafieldId> inEqoiUniquenessFields) {
        EquipmentOrderItem resolvedEqoi = null;
        Map<MetafieldId, Object> fieldValues = new HashMap<MetafieldId, Object>();
        boolean isResolvable = true;
        for (Iterator<EquipmentOrderItem> eqoiItr = inEqois.iterator(); eqoiItr.hasNext() && isResolvable;) {
            EquipmentOrderItem eqoi = eqoiItr.next();
            /* if (eqoi.getEqoiSeqNbr() == null) {
             RoadBizUtil.appendMessage(MessageLevel.SEVERE, RoadPropertyKeys.GATE__ORDER_ITEM_SEQUENCE_REQUIRED);
             isResolvable = false;
         } else { */
            for (Iterator<MetafieldId> fieldIdItr = inEqoiUniquenessFields.iterator(); fieldIdItr.hasNext() && isResolvable;) {
                MetafieldId fieldId = fieldIdItr.next();
                if (!fieldValues.containsKey(fieldId)) {
                    fieldValues.put(fieldId, eqoi.getFieldValue(fieldId));
                } else if (!isEqual(fieldId, fieldValues.get(fieldId), eqoi.getFieldValue(fieldId))) {
                    RoadBizUtil.appendMessage(MessageLevel.SEVERE, RoadPropertyKeys.GATE__ORDER_ITEM_DIFFERENCES);
                    isResolvable = false;
                }
            }
            if (resolvedEqoi == null) {

                resolvedEqoi = eqoi;
            }
            else if (eqoi.getEqoiSeqNbr() != null && resolvedEqoi.getEqoiSeqNbr() != null) {

                if (eqoi.getEqoiSeqNbr() < resolvedEqoi.getEqoiSeqNbr()) {

                    resolvedEqoi = eqoi;
                }
            }
            else if (eqoi.getEqoiSeqNbr() == null && resolvedEqoi.getEqoiSeqNbr() == null){

                if (eqoi.getEqboiGkey() <  resolvedEqoi.getEqboiGkey()) {

                    resolvedEqoi = eqoi;
                }
            }
            else if (eqoi.getEqoiSeqNbr() != null && resolvedEqoi.getEqoiSeqNbr() == null) {

                resolvedEqoi = eqoi;
            }

            //}
        }
        if (!isResolvable) {
            resolvedEqoi = null;
        }
        return resolvedEqoi;
    }

    private static boolean isEqual(MetafieldId inFieldId, Object inOldValue, Object inNewValue) {
        boolean equals;
        if (OrdersField.EQOI_HAZARDS.equals(inFieldId)) {
            Hazards oldHazards = inOldValue != null ? (Hazards) Roastery.getHibernateApi().load(Hazards.class, (Long) inOldValue) : null;
            Hazards newHazards = inNewValue != null ? (Hazards) Roastery.getHibernateApi().load(Hazards.class, (Long) inNewValue) : null;
            equals = compareHazardItems(oldHazards, newHazards);
        } else {
            if (inOldValue == null && inNewValue == null) {
                equals = true;
            } else if (inOldValue == null || inNewValue == null) {
                equals = false;
            } else {
                equals = inOldValue.equals(inNewValue);
            }
        }
        return equals;
    }

    private static boolean compareHazardItems(Hazards inHazards1, Hazards inHazards2) {
        boolean equals;
        Set<ImdgClass> haz1Imdgs = new HashSet<ImdgClass>();
        Set<String> haz1UnNbrs = new HashSet<String>();
        if (inHazards1 != null) {
            for (Iterator<HazardItem> itr = inHazards1.getHazardItemsIterator(); itr.hasNext();) {
                HazardItem haz1itm = itr.next();
                haz1Imdgs.add(haz1itm.getHzrdiImdgClass());
                haz1UnNbrs.add(haz1itm.getHzrdiUNnum());
            }
        }

        Set<ImdgClass> haz2Imdgs = new HashSet<ImdgClass>();
        Set<String> haz2UnNbrs = new HashSet<String>();
        if (inHazards2 != null) {
            for (Iterator<HazardItem> itr = inHazards2.getHazardItemsIterator(); itr.hasNext();) {
                HazardItem haz2itm = itr.next();
                haz2Imdgs.add(haz2itm.getHzrdiImdgClass());
                haz2UnNbrs.add(haz2itm.getHzrdiUNnum());
            }
        }

        if (haz1Imdgs.size() == haz2Imdgs.size() && haz1UnNbrs.size() == haz2UnNbrs.size()) {
            equals = true;
            for (ImdgClass imdg : haz1Imdgs) {
                if (!haz2Imdgs.contains(imdg)) {
                    equals = false;
                }
            }
            for (String unnbr : haz1UnNbrs) {
                if (!haz2UnNbrs.contains(unnbr)) {
                    equals = false;
                }
            }
        } else {
            equals = false;
        }

        return equals;
    }
    //@todo - EDO?
    public EquipmentOrderItem getCurrentUnitEqoi(EqBaseOrder inEqBaseOrder, Unit inUnit) {

        UnitEquipment ue = inUnit.getUnitPrimaryUe();
        EqBaseOrderItem preadvisedItem;
        if (inEqBaseOrder != null && ue != null) {

            if (EquipmentOrderSubTypeEnum.ERO.equals(inEqBaseOrder.getEqboSubType())) {
                preadvisedItem = ue.getUeArrivalOrderItem();
            } else {
                preadvisedItem = ue.getUeDepartureOrderItem();
            }
            if (preadvisedItem != null) {
                return  EquipmentOrderItem.resolveEqoiFromEqboi(preadvisedItem);

            }
        }

        return null;
    }


    private static def _panGateTaskHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateTaskHelper");

}