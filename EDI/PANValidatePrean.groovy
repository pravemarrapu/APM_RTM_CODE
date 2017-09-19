/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 *  Date 09/04/14 CSDV-1841 fixed method signatures
 */


package apmt.edi

import com.navis.argo.business.atoms.EventEnum
import com.navis.framework.metafields.MetafieldId
import com.navis.inventory.business.api.UnitField;
import org.apache.log4j.Level;


import com.navis.apex.business.model.GroovyInjectionBase;
import com.navis.argo.ContextHelper;
import com.navis.argo.business.atoms.FreightKindEnum;
import com.navis.external.framework.util.EFieldChanges;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.Disjunction;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.util.BizViolation;
import com.navis.inventory.business.units.Unit;
import com.navis.road.RoadApptsEntity;
import com.navis.road.RoadApptsField;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.AppointmentStateEnum;
import com.navis.road.business.atoms.GateClientTypeEnum;
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum;
import com.navis.services.business.event.Event;
import com.navis.services.business.event.GroovyEvent
import com.navis.argo.business.model.GeneralReference
import com.navis.framework.portal.query.PredicateIntf;

/**
 * When a relevant event is recorded against a unit,
 * all preans linked to the unit are found and re-validated
 *
 *
 * Author: Sophia Robertson
 * Date: 03/04/13
 * Called from: General Notices
 *
 * 09-06-14 CSDV-1244 Pass the event type id to the validation flow
 * 11-07-14 CSDV-2169 if system is configured to send mutiple APERAKs for COPINO 13, include COPINO 13 preans in the list of preans to validate
 *
 * 20-06-17 : weserve team - Update Unit's routing details with prean's
 * 23-08-17 : weserve team - Update Prean's typeISO with Unit's typeISO
 *
 */


public class PANValidatePrean extends GroovyInjectionBase {

    public void execute(GroovyEvent inGroovyEvent) {
        Event evnt = inGroovyEvent.getEvent();
        Serializable unitPk = evnt.getEventAppliedToGkey();
        Unit unit = (Unit) HibernateApi.getInstance().load(Unit.class, unitPk);
        if (!(EventEnum.UNIT_CREATE.getId().equalsIgnoreCase(evnt.getEventTypeId()) || EventEnum.UNIT_ACTIVATE.getId().equalsIgnoreCase(evnt.getEventTypeId())
                && YES.equals(unit.getFieldString(CAN_BE_DELETED_BY_PREAN)))) {
            List<GateAppointment> unitApptList = findOpenGateAppointmentsByUnit(unit);
            if (unitApptList.size() == 0) {
                unitApptList = findPickupPreansByContainerIdAndFreightKind(unit.getUnitId(), unit.getUnitFreightKind());
            }
            for (GateAppointment appt : unitApptList) {
                try {
                    updatePreanRtgDetailsToUnit(appt, unit);

                    //weserve team - Update Prean's typeISO with Unit's typeISO
                    updatePreanWithUnitIsoType(appt, unit);

                    appt.setGapptOrderNbr((appt.getFieldString(_panFields.PREAN_EQO_NBR)));
                    appt.setPinNumber(appt.getFieldString(_panFields.PREAN_PIN));

                    List<String> extensionData = new ArrayList<String>();
                    extensionData.add(0, evnt.getEventTypeId());

                    appt.submit(GateClientTypeEnum.CLERK, extensionData);

                } catch (BizViolation bv) {
                    //@todo - Revisit
                    log(Level.ERROR, bv.getMessage());
                }

            }
        }
    }

    public List findOpenGateAppointmentsByUnit(Unit inUnit) {

        DomainQuery dq = QueryUtils.createDomainQuery(RoadApptsEntity.GATE_APPOINTMENT)
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_UNIT, inUnit.getUnitGkey()))
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_STATE, AppointmentStateEnum.CREATED))

        PredicateIntf criterion = constructRspMsgTypeCriterion();

        if (criterion != null) {
            dq.addDqPredicate(criterion);
        }

        List<GateAppointment> gapptList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
        return gapptList;
    }

    public List findPickupPreansByContainerIdAndFreightKind(String inCtrId, FreightKindEnum inFreightKind) {

        DomainQuery dq = QueryUtils.createDomainQuery(RoadApptsEntity.GATE_APPOINTMENT)
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_CTR_ID, inCtrId))
                .addDqPredicate(PredicateFactory.in(RoadApptsField.GAPPT_TRAN_TYPE, [TruckerFriendlyTranSubTypeEnum.PUI, TruckerFriendlyTranSubTypeEnum.PUM]))
                .addDqPredicate(PredicateFactory.isNull(RoadApptsField.GAPPT_UNIT))
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_FREIGHT_KIND, inFreightKind.getKey()))
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_STATE, AppointmentStateEnum.CREATED));

        PredicateIntf criterion = constructRspMsgTypeCriterion();

        if (criterion != null) {
            dq.addDqPredicate(criterion);
        }

        List<GateAppointment> gapptList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
        return gapptList;
    }

    private PredicateIntf constructRspMsgTypeCriterion() {

        PredicateIntf criterion = null;

        if (!multipleAperaksSent()) {

            Disjunction disc = PredicateFactory.disjunction();
            disc.add(PredicateFactory.isNull(_panFields.RESPONSE_MSG_TYPE));
            disc.add(PredicateFactory.ne(_panFields.RESPONSE_MSG_TYPE, STATUS_UPDATE));
            criterion = disc;
        }
        return criterion;
    }

    private boolean multipleAperaksSent() {

        boolean multipleAperaksSent = false;

        GeneralReference genRef = GeneralReference.findUniqueEntryById("NON_STD_APERAK_OUT", "COPINO_13", "MULTIPLE_APERAKS");
        multipleAperaksSent = genRef != null && "YES".equals(genRef.getRefValue1());
        if (!multipleAperaksSent) {
            log("GeneralReference record NON_STD_APERAK_OUT/COPINO_13/MULTIPLE_APERAKS does not exist")
        }
        return multipleAperaksSent;
    }

    /**
     * Updates the routing details from prean to unit
     * @param appt
     * @param unit
     */
    private void updatePreanRtgDetailsToUnit(GateAppointment appt, Unit unit) {
        if (AppointmentStateEnum.CREATED.equals(appt.getApptState()) && unit.getUnitRouting() != null) {
            if (appt.getGapptPol() != null) {
                unit.getUnitRouting().setRtgPOL(appt.getGapptPol());
            }
            if (appt.getGapptPod1() != null) {
                unit.getUnitRouting().setRtgPOD1(appt.getGapptPod1());
            }
        }
    }

    /**
     * Update Prean's typeISO with Unit's typeISO
     * @param appt
     * @param unit
     */
    private void updatePreanWithUnitIsoType(GateAppointment appt, Unit unit) {
        if (AppointmentStateEnum.CREATED.equals(appt.getApptState()) && unit.getUnitEquipment() != null) {
            appt.setGapptCtrEquipType(unit.getUnitEquipment().getEqEquipType());
        }
    }

    private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
    private static String STATUS_UPDATE = "STATUS_UPDATE";
    private static MetafieldId CAN_BE_DELETED_BY_PREAN = UnitField.UNIT_FLEX_STRING02;
    private static String YES = "YES";
}
