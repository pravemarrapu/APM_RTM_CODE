/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 * 23-Oct-2013 CSDV-1410 Get APERAK error text from Gen Refs
 *
 * 29/09/14 CSDV-2392 Commented out !ufv.isFuture condition to allow to verify events for advised units
 *
 * 10/03/2015 CSDV-2558 error desc: concatenate gen ref value2 and value 3
 */

package com.navis.apex.groovy.mv2_rwg.preannouncements.validation.bizflow.tasks;


import com.navis.argo.ContextHelper
import com.navis.argo.business.api.IEventType
import com.navis.argo.business.api.Serviceable
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.EventEnum
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.reference.Equipment
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.external.road.EGateTaskInterceptor
import com.navis.framework.business.Roastery
import com.navis.framework.business.atoms.LifeCycleStateEnum;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.util.BizViolation

import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.orders.business.eqorders.EquipmentOrder
import com.navis.road.business.util.RoadBizUtil
import com.navis.road.business.workflow.IBizTaskParm
import com.navis.road.business.workflow.IUnitBean
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.road.portal.GateConfigurationParameterConstants
import com.navis.services.ServicesEntity;
import com.navis.services.ServicesField;
import com.navis.services.ServicesPropertyKeys
import com.navis.services.business.rules.EventType
import com.navis.services.business.rules.HoldPermissionView
import com.navis.services.business.rules.ServiceRule

import com.navis.external.framework.util.ExtensionUtils
import com.navis.services.business.rules.FlagType

class PANRejectUnitServiceRules extends AbstractGateTaskInterceptor implements EGateTaskInterceptor{

    public void execute(TransactionAndVisitHolder inWfCtx) {

        executeCustom(inWfCtx);

    }

    public void executeCustom(TransactionAndVisitHolder inWfCtx)  {
        log("PANRejectUnitServiceRules.start");
        // Determine the intended Service
        EventEnum service;
        IUnitBean tran = inWfCtx.getTran();

        String gateId = tran.getGate().getGateId();

        if (tran.isReceival()) {
            if ("BARGE".equals(gateId)) {
                service = EventEnum.UNIT_DISCH;
            }
            else if ("RAIL".equals(gateId)){
                service = EventEnum.UNIT_DERAMP;
            }
            else {
                service = EventEnum.UNIT_RECEIVE;
            }

        } else {
            if ("BARGE".equals(gateId)) {
                service = EventEnum.UNIT_LOAD;
            }
            else if ("RAIL".equals(gateId)){
                service = EventEnum.UNIT_RAMP;
            }
            else {
                service = EventEnum.UNIT_DELIVER;
            }

        }

        try {
            execute(inWfCtx, service);
        }
        catch (BizViolation bve) {
            RoadBizUtil.appendExceptionChain(bve);
        }
        log("PANRejectUnitServiceRules.end");
    }

    public void execute(TransactionAndVisitHolder inWfCtx, EventEnum inEventType) throws BizViolation {

        IUnitBean tran = inWfCtx.getTran();

        Unit unit = tran.getTranUnit();
        EquipmentOrder edo = tran.getTranEqo();
        if (edo != null){
            Boolean isIgnoreHolds = edo.getEqoIgnoreHolds() != null? edo.getEqoIgnoreHolds():false;
            if (isIgnoreHolds){
                return;
            }
        }

        //ARGO-16125 2008-12-19 rsatish v1.8.D v1.6.15 activate the unit if not active as some of the service rule predicate fields
        //applies on unit's activeUFV entity. This code is moved from CreateContainerVisit as many of the attributes might not be
        //set when UFV activates, thus the rules registered against UNIT_ACTIVATE may fail.
        if (unit != null) {

            log("unit ID: "+unit.getUnitId());

            UnitFacilityVisit ufv = tran.getTranUfv();
            if (ufv != null) {
                if (ufv.isFuture() && !activeUnitExists(unit.getUnitEquipment(unit.getUnitId()).getUeEquipment())) {
                    try {
                        ufv.makeActive();
                    } catch (BizViolation inBv) {
                        RoadBizUtil.appendExceptionChain(inBv);
                    }
                }
            }


            // Ask Services module if this Service can be applied
            // only if the unit was activated successfully (otherwise some of the filters won't work)
            // CSDV-???? commented out the !ufv.isFuture condition

            if (ufv != null){   //&& !ufv.isFuture()) {

                BizViolation bv = verifyEventAllowed(inEventType, unit, isCovertHoldsIncluded(tran),inWfCtx);
                if (bv != null) {
                    RoadBizUtil.appendExceptionChain(bv);
                }
            }
        }
    }

    private boolean isCovertHoldsIncluded(IUnitBean inTran) {
        IBizTaskParm param = inTran.getBizTaskParm();
        String includeCovertHoldsParam = (String) param.get(GateConfigurationParameterConstants.TASK_PARAMETER_INCLUDE_COVERT_HOLDS);
        boolean isIncluded = true;
        if (includeCovertHoldsParam != null) {
            if ("FALSE".equalsIgnoreCase(includeCovertHoldsParam) || "NO".equalsIgnoreCase(includeCovertHoldsParam)) {
                isIncluded = false;
            }
        }
        return isIncluded;
    }

    public BizViolation verifyEventAllowed( IEventType inEventType,
                                            Serviceable inServiceable,
                                            Boolean inIncludeCovertHolds,
                                            TransactionAndVisitHolder inWfCtx
    ) throws BizViolation{

        log("PANRejectUnitServiceRules.verifyEventAllowed start");
        EventType eventType = EventType.resolveIEventType(inEventType);


        // Verify that the event type filter allows the eventType to be applied to the serviceable
        if (!eventType.filterAppliesToEntity(inServiceable)) {
            return BizViolation.create(ServicesPropertyKeys.EVENT_TYPE_FILTER_NOT_FOR_THIS_SERVICEABLE, null,
                    eventType.getEvnttypeId(), inServiceable.getLogEntityId(), eventType.explainFilterFailure(inServiceable));
        }

        // Find all Service Rules that apply
        ServicesManager sm = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
        List rules = findRulesForEventType(eventType, inIncludeCovertHolds);
        BizViolation violations = null;

        String newPreanStatus = inWfCtx.get(_panGateTaskHelper.NEW_PREAN_STATUS_KEY);

        String tranType = inWfCtx.getTran().isReceival() ? "RECEIVAL" : "DELIVERY";

        for (Object rule : rules) {

            ServiceRule serviceRule = (ServiceRule) rule;
            log("Service Rule Name: "+ serviceRule.getSrvrulName());

            FlagType  srvFlgType = serviceRule.getSrvrulFlagType();

            if (srvFlgType != null) {
                log("Flag Type(hold/perm ID): "+srvFlgType.getFlgtypId());
                HoldPermissionView holdPermGroup = srvFlgType.getFlgtypHoldPermView();

                if (holdPermGroup != null) {

                    String holdPermGroupID = holdPermGroup.getHpvId();
                    log("Flag Type(hold/perm ID): "+ holdPermGroupID);
                    if (holdPermGroupID.startsWith(HOLD_PERM_GROUP_ID_PREFIX)) {

                        log("holdPermGroupID starts with PREAN");
                        if (serviceRule.isFilterForEntity(inServiceable)) {
                            log("filter for this rule matches for the entered entity");
                        }
                        else {
                            log("filter for this rule does not match for the entered entity");
                        }

                        BizViolation bv = serviceRule.verifyRuleSatisfied(inServiceable);

                        if (bv != null) {

                            GeneralReference errRef = GeneralReference.findUniqueEntryById("PREAN_ERR_CONDITION",tranType,"UnitServiceRules",holdPermGroup.getHpvId());
                            if (errRef != null) {
                                log("errRef.getRefValue1(): " + errRef.getRefValue1());
                                log("errRef.getRefValue1(): " + errRef.getRefValue2());
                                _preanErrorUtil.recordError(inWfCtx.getAppt(),
                                        errRef.getRefValue1(),
                                        errRef.getRefValue2() + (errRef.getRefValue3() == null ? "" : errRef.getRefValue3()),
                                        inWfCtx.get(_panGateTaskHelper.VALIDATION_RUN_ID_KEY));

                                newPreanStatus = "NOK";

                            }

                        }
                    }
                }

            }


        }
        inWfCtx.put(_panGateTaskHelper.NEW_PREAN_STATUS_KEY, newPreanStatus);
        return  violations;
    }

    private boolean activeUnitExists(Equipment inEq){
        UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
        return unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), inEq) != null;

    }

    private List findRulesForEventType(IEventType inEventType, Boolean inIncludeCovertHolds) {
        // Deal with system vs. user event
        EventType eventType = EventType.resolveIEventType(inEventType);

        // Construct query for Service Rules for the input Service Type (results will be auto-restricted by scoping mechanism)
        DomainQuery dq = QueryUtils.createDomainQuery(ServicesEntity.SERVICE_RULE)
                .addDqPredicate(PredicateFactory.eq(ServicesField.SRVRUL_SERVICE_TYPE, eventType.getEvnttypeGkey()))
                .addDqPredicate(PredicateFactory.eq(ServicesField.SRVRUL_LIFE_CYCLE_STATE, LifeCycleStateEnum.ACTIVE));

        // Add extra Predicate if Covert related rules are not to be included
        if (inIncludeCovertHolds != null && !inIncludeCovertHolds) {
            DomainQuery subQuery = QueryUtils.createDomainQuery(ServicesEntity.FLAG_TYPE)
                    .addDqPredicate(PredicateFactory.eq(ServicesField.FLGTYP_IS_COVERT_HOLD_REQUIRED, Boolean.TRUE))
                    .addDqField(ServicesField.FLGTYP_GKEY);
            dq.addDqPredicate(PredicateFactory.subQueryNotIn(subQuery, ServicesField.SRVRUL_FLAG_TYPE));
        }

        return HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
    }


    private static String HOLD_PERM_GROUP_ID_PREFIX = "PREAN_";
    private static def _panGateTaskHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateTaskHelper");
    private static def _preanErrorUtil = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANPreanErrorUtil");
}