/*
 * Copyright (c) 2016 Navis LLC. All Rights Reserved.
 *
 * @version $Id: PANRejectOrderNotMatchingPreadvise.groovy 261194 2016-10-14 16:12:51Z neefpa $
 *
 */

/* APMT
*
* Date 05/06/14 CSDV-1244 Try to find applicable EQ_PREADVISE_NN event to decide if the original unit order was set by a prean process
*
* Date 11/10/16 CSDV-3990 Record error if there is an iso type mismatch (for the same order)
*/


package com.navis.apex.groovy.mv2_rwg.preannouncements.validation.bizflow.tasks;

import com.navis.argo.ContextHelper;
import com.navis.argo.business.api.Serviceable;
import com.navis.argo.business.atoms.EquipmentOrderSubTypeEnum;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.external.road.AbstractGateTaskInterceptor;
import com.navis.external.road.EGateTaskInterceptor;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.Ordering;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.util.TransactionParms;
import com.navis.framework.util.message.MessageCollector;
import com.navis.framework.util.message.MessageCollectorFactory;
import com.navis.framework.util.message.MessageLevel;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitEquipment;
import com.navis.orders.business.eqorders.EquipmentOrder;
import com.navis.road.RoadPropertyKeys;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.model.TruckTransaction;
import com.navis.road.business.util.RoadBizUtil;
import com.navis.road.business.workflow.TransactionAndVisitHolder;
import com.navis.services.ServicesEntity;
import com.navis.services.ServicesField;
import com.navis.services.business.rules.EventType;

public class PANRejectOrderNotMatchingPreadvise extends AbstractGateTaskInterceptor implements EGateTaskInterceptor {

    public void execute(TransactionAndVisitHolder inWfCtx) {
        TruckTransaction tran = inWfCtx.getTran();
        if (tran != null) {
            Unit unit = tran.getTranUnit();
            if (unit != null) {


                MessageCollector origMsgCollector = RoadBizUtil.getMessageCollector();
                MessageCollector thisTaskMsgCollector =  MessageCollectorFactory.createMessageCollector();
                TransactionParms.getBoundParms().setMessageCollector(thisTaskMsgCollector);

                executeExternal(inWfCtx);

                if (thisTaskMsgCollector.hasError()){
                    tran.setTranEqoItem(null);
                    _panGateTaskHelper.recordPreanError(inWfCtx, getClass().getSimpleName(), RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector));

                }
                TransactionParms.getBoundParms().setMessageCollector(origMsgCollector);


            }
        }
    }

    private void executeExternal(TransactionAndVisitHolder inOutDao) {

        TruckTransaction tran = inOutDao.getTran();
        GateAppointment prean = inOutDao.getAppt();

        if (tran != null) {

            EquipmentOrder eqo = tran.getTranEqo();

            if (eqo != null) {

                Unit unit = tran.getTranUnit();
                if (unit != null && unit.isUnitPreadvised()) {

                    UnitEquipment ue = unit.getUnitPrimaryUe();

                    if (ue != null) {


                        if (EquipmentOrderSubTypeEnum.BOOK.equals(eqo.getEqboSubType()) || EquipmentOrderSubTypeEnum.RAIL.equals(eqo.getEqboSubType())) {
                            if (ue.getUeDepartureOrderItem() != null) {

                                EquipmentOrder preadviseEqo = EquipmentOrder.resolveEqoFromEqbo(ue.getUeDepartureOrderItem().getEqboiOrder());

                                if (!eqo.equals(preadviseEqo) || (tran.getTranEqoItem() != null && (tran.getTranEqoItem() != ue.getUeDepartureOrderItem()))) {

                                    String evntTypeID = EQ_PREADVISE_EVENT_TYPES.get(preadviseEqo.getEqboSubType().getKey());

                                    if (!isOrderSetByPrean(EventType.findEventType(evntTypeID),preadviseEqo,unit.getUnitId())) {

                                        Object[] parms =[unit.getUnitId(), preadviseEqo.getEqboNbr()];
                                        RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE,
                                                RoadPropertyKeys.GATE__UNIT_PREADVISED_AGAINST_ORDER, null,
                                                parms);
                                        //Set tran eqo item to null so that PANCreateContainerPreadvisedVisit doesn't re-assign unit to a new order
                                        tran.setTranEqoItem(null);

                                    }
                                }
                            }
                        } else if (EquipmentOrderSubTypeEnum.ERO.equals(eqo.getEqboSubType())) {
                            if (ue.getUeArrivalOrderItem() != null) {

                                EquipmentOrder preadviseEqo = EquipmentOrder.resolveEqoFromEqbo(ue.getUeArrivalOrderItem().getEqboiOrder());

                                String evntTypeID = EQ_PREADVISE_EVENT_TYPES.get(preadviseEqo.getEqboSubType().getKey());

                                if (!eqo.equals(preadviseEqo) || (tran.getTranEqoItem() != null && (tran.getTranEqoItem() != ue.getUeArrivalOrderItem()))) {

                                    if (!isOrderSetByPrean(EventType.findEventType(evntTypeID),preadviseEqo,unit.getUnitId())) {

                                        Object[] parms = [unit.getUnitId(), preadviseEqo.getEqboNbr()];
                                        RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE,
                                                RoadPropertyKeys.GATE__UNIT_PREADVISED_AGAINST_ORDER, null,
                                                parms);
                                        //Set tran eqo item to null so that PANCreateContainerPreadvisedVisit doesn't re-assign unit to a new order
                                        tran.setTranEqoItem(null);

                                    }
                                }

                            }
                        }


                    }
                }
            }
        }
    }


    public boolean isOrderSetByPrean(EventType inEventType, Serviceable inTargetServiceable, String inUnitId) {

        boolean result = true;

        DomainQuery dq = QueryUtils.createDomainQuery(ServicesEntity.EVENT)
                .addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_APPLIED_TO_PRIMARY_KEY, inTargetServiceable.getPrimaryKey()))
                .addDqOrdering(Ordering.desc(ServicesField.EVNT_GKEY))
                .addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_EVENT_TYPE, inEventType.getEvnttypeGkey()))
                .addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_NOTE, inUnitId))
                .addDqPredicate(PredicateFactory.eq(EVENT_APPLIED_BY_PROCESS, "PREAN"));

        dq.setDqMaxResults(1);

        dq.setRequireTotalCount(false);


        Serializable[] eventGkeys = _hibernateApi.findPrimaryKeysByDomainQuery(dq);
        if (eventGkeys == null || eventGkeys.length == 0) {
            result = false;
        }

        //Event event = (Event) _hibernateApi.load(Event.class, eventGkeys[0]);
        return result;

    }


    private static def _panGateTaskHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateTaskHelper");
    private static String PREAN = "PREAN";


    private static MetafieldId EVENT_APPLIED_BY_PROCESS = MetafieldIdFactory.valueOf("evntFlexString01");
    private static HibernateApi _hibernateApi = HibernateApi.getInstance();

    public static Map<String,String> EQ_PREADVISE_EVENT_TYPES = new HashMap();

    static {

        EQ_PREADVISE_EVENT_TYPES.put("BOOK", "EQ_PREADVISE_BKG");
        EQ_PREADVISE_EVENT_TYPES.put("ERO", "EQ_PREADVISE_ERO");
        EQ_PREADVISE_EVENT_TYPES.put("RAIL", "EQ_PREADVISE_RO");
    }

}