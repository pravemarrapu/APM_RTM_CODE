/*
 * Copyright (c) 2015 Navis LLC. All Rights Reserved.
 *
 * $Id: GateAppointmentInterceptor.groovy 234044 2015-11-23 10:44:36Z extneefpa $
 *
 */

package com.navis.apex.groovy.mv2_rwg;

import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.EventEnum;
import com.navis.argo.business.atoms.FreightKindEnum;
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.RoutingPoint;
import com.navis.external.framework.entity.AbstractEntityLifecycleInterceptor;
import com.navis.external.framework.entity.EEntityView;
import com.navis.external.framework.util.EFieldChanges;
import com.navis.external.framework.util.EFieldChangesView;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.Entity;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.FieldChange
import com.navis.framework.portal.FieldChanges;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.util.BizViolation
import com.navis.inventory.business.units.EqBaseOrder
import com.navis.inventory.business.units.Routing;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentOrder;
import com.navis.road.RoadApptsField;
import com.navis.road.business.appointment.api.IAppointmentManager;
import com.navis.road.business.appointment.model.AppointmentTimeSlot;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.AppointmentStateEnum;
import com.navis.road.business.atoms.TranSubTypeEnum;
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum;
import com.navis.road.business.model.Gate
import com.navis.road.business.appointment.model.TruckVisitAppointment
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger;

/*

25/06/2015 CSDV-2832 re-factored setting prean Send Msg functionality: commented it out here

20/11/2015 CSDV-3400 Temp fix: Adding 100000000 to ApptNbr to avoid duplicate error
WF#777643 22-Mar-17 - Added 300000000 to ApptNbr to avoid duplicate error for RAIL-ITT
Modified By: Praveen Babu
Date 06/09/2017 APMT #24 and #32 - Logic to update the prean eqo number and order number for RAIL_ITT gate transactions.
Modified By: Pradeep Arya - 08-Sept-17 WF#892222 added null check for booking
19-Sept-17 Pradeep Arya - WF#896862 - set field 'can be retired by prean' to YES so that it can be retired on Cancel
Modified By: Praveen Babu: Date 20/09/2017 APMT #32 - Re-assign the prean eqo nbr when the unit is retired and re-assigned to prean
Modified By: Praveen Babu: Date 20/09/2017 APMT #32 - Implement the fix for other gates.

 */

public class GateAppointmentInterceptor extends AbstractEntityLifecycleInterceptor {

    public void onCreate(
            EEntityView inEntity,
            EFieldChangesView inOriginalFieldChanges,
            EFieldChanges inMoreFieldChanges) {

        FieldChange tranTypeFc = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_TRAN_TYPE);
        TruckerFriendlyTranSubTypeEnum tranType = (TruckerFriendlyTranSubTypeEnum) tranTypeFc.getNewValue();
        if (TruckerFriendlyTranSubTypeEnum.PUM.equals(tranType) || TruckerFriendlyTranSubTypeEnum.DOM.equals(tranType)) {
            inMoreFieldChanges.setFieldChange(RoadApptsField.GAPPT_FREIGHT_KIND, FreightKindEnum.MTY);
        }

        FieldChange gateFc = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_GATE);
        Gate gate = (Gate) gateFc.getNewValue();

        //UPDATE gapptNbr to gapptNbr + 100 000 000 (100 million)
        if (("BARGE".equals(gate.getGateId()) || "RAIL".equals(gate.getGateId()))) {

            FieldChange apptNbrFc = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_NBR);
            inMoreFieldChanges.setFieldChange(RoadApptsField.GAPPT_NBR, (Long) apptNbrFc.getNewValue() + 100000000L);

        } else if ("RAIL_ITT".equals(gate.getGateId())) {
            FieldChange apptNbrFc = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_NBR);
            inMoreFieldChanges.setFieldChange(RoadApptsField.GAPPT_NBR, (Long) apptNbrFc.getNewValue() + RAIL_ITT_APPT_NBR);
        }


        if (("BARGE".equals(gate.getGateId()) || "RAIL".equals(gate.getGateId())) && ContextHelper.getThreadEdiPostingContext() == null) {

            FieldChange customFlexFieldsFc = (FieldChange) inOriginalFieldChanges.findFieldChange(MetafieldIdFactory.valueOf("customFlexFields"));
            Map<String, Object> customFields = (Map<String, Object>) customFlexFieldsFc.getNewValue();
            CarrierVisit cv = (CarrierVisit) customFields.get("gapptCustomDFFlandsideCV");

            Date cvEta = cv.getCvCvd().getCvdETA();
            if (cvEta == null) {
                registerError("Barge/Train ETA is not set");
            } else {

                Date today = new Date();
                if (cvEta.after(today)) {

                    inMoreFieldChanges.setFieldChange(RoadApptsField.GAPPT_REQUESTED_DATE, cvEta);

                    IAppointmentManager appMgr = (IAppointmentManager) Roastery.getBean(IAppointmentManager.BEAN_ID);
                    AppointmentTimeSlot slot;
                    try {
                        slot = appMgr.getAppointmentTimeSlot(gate.getGateGkey(), TranSubTypeEnum.RE, cvEta, null, false);
                        if (slot != null) {
                            inMoreFieldChanges.setFieldChange(RoadApptsField.GAPPT_TIME_SLOT, slot);
                        } else { //should never get to this - the rule set should have unlimited appt slots
                            registerError("Tme slot is not avaialable - modify the rule set to have unlimited appt nbr");
                        }

                    } catch (BizViolation bv) {
                        //
                    }
                } else {
                    registerError("Barge/Train has already arrived or ETA is wrong (set to before today)");
                }

            }

        }
    }

    public void onUpdate(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {

        LOGGER.setLevel(Level.DEBUG);
        LOGGER.debug("oRIGINAL Filed Changes:: " + inOriginalFieldChanges)
        GateAppointment prean = inEntity._entity;

        if (("BARGE".equals(prean.getGate().getGateId()) || "RAIL".equals(prean.getGate().getGateId())) && ContextHelper.getThreadEdiPostingContext() == null) {
            FieldChange gapptTimeSlotFc = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_TIME_SLOT);
            if (gapptTimeSlotFc != null && gapptTimeSlotFc.getNewValue() == null) {
                inMoreFieldChanges.setFieldChange(RoadApptsField.GAPPT_TIME_SLOT, gapptTimeSlotFc.getPriorValue());
            }
        }

        FieldChange gateApptStateFc = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_STATE);
        if (gateApptStateFc != null && AppointmentStateEnum.CANCEL.equals((AppointmentStateEnum) gateApptStateFc.getNewValue())) {

            if (prean != null) {
                Unit unit = prean.getGapptUnit();
                if (unit != null) {
                    UnitFacilityVisit ufv = unit.getUfvForFacilityLiveOnly(prean.getGapptGate().getGateFacility());
                    if (ufv != null) {
                        propagatePreanStatusToUfv(ufv, null, prean.isReceival());
                        if (prean.isDelivery()) {
                            setUfvOutboundCarrierToGeneric(ufv, prean);
                        }
                    }
                }

                /*  CSDV-2832
                FieldChange rspMsgTypeFc = (FieldChange)inOriginalFieldChanges.findFieldChange(RESPONSE_MSG_TYPE );

                if (!STATUS_UPDATE.equals(prean.getFieldString(RESPONSE_MSG_TYPE))) {
                  if (rspMsgTypeFc == null || rspMsgTypeFc.getNewValue() == null || !NO_RESPONSE_STR.equals((String)rspMsgTypeFc.getNewValue())){
                    inMoreFieldChanges.setFieldChange(SEND_MSG,"YES");
                  }

                }*/
            }


        }

        FieldChange unitFc = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_UNIT);
        if (unitFc != null && unitFc.getPriorValue() != null) {

            Unit unit = (Unit) unitFc.getPriorValue();
            if (unit != null) {
                UnitFacilityVisit ufv = unit.getUfvForFacilityLiveOnly(prean.getGapptGate().getGateFacility());
                if (ufv != null) {
                    propagatePreanStatusToUfv(ufv, null, prean.isReceival());
                }
            }

        }
        //added by Pradeep Arya
        //WF#896862 - set field 'can be retired by prean' to YES after the post
        if (unitFc != null) {
            Unit unit = (Unit) unitFc.getNewValue();

            if (unit) {
                boolean wasNewlyCreatedUnit = wasUnitJustCreatedByPrean(unit);
                log("wasNewlyCreatedUnit:$wasNewlyCreatedUnit");
                if (wasNewlyCreatedUnit) {
                    inMoreFieldChanges.setFieldChange(RoadApptsField.GAPPT_UNIT_FLEX_STRING02, "YES");
                    //prean.setGapptUnitFlexString02("YES");
                }
                //Logic to compare the unit booking and prean booking number vs prean eqo nbr
                if (unit.getDepartureOrder() != null && prean.getGapptOrder() != null) {
                    if (unit.getDepartureOrder().getEqboNbr().equalsIgnoreCase(prean.getGapptOrder().getEqboNbr())
                            && !prean.getGapptOrder().getEqboNbr().equalsIgnoreCase(prean.getFieldString(_panFields.PREAN_EQO_NBR))) {
                        inMoreFieldChanges.setFieldChange(_panFields.PREAN_EQO_NBR, prean.getGapptOrder().getEqboNbr())
                        inMoreFieldChanges.setFieldChange(RoadApptsField.GAPPT_NOTES, "REVALIDATE")
                    }
                }
            }
        }

        FieldChange tvaFc = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_TRUCK_VISIT_APPOINTMENT);
        if (tvaFc != null && tvaFc.getNewValue() == null) {
            log("Disassoc. TVA from Gappt");
            logImportantInfo((TruckVisitAppointment) tvaFc.getPriorValue(), (GateAppointment) inEntity._entity);
        }


        FieldChange gapptBkg = (FieldChange) inOriginalFieldChanges.findFieldChange(RoadApptsField.GAPPT_ORDER);
        if (gapptBkg != null && gapptBkg.getNewValue() != null) {
            String preanEqoNbr = prean.getFieldValue(_panFields.PREAN_EQO_NBR);
            EqBaseOrder eqbo = (EqBaseOrder) gapptBkg.getNewValue();
            EquipmentOrder eqo = Booking.resolveEqoFromEqbo(eqbo)
            Booking booking = Booking.resolveBkgFromEqo(eqo)
            //WF#892222 Pradeep Arya - added null check for booking
            if (preanEqoNbr != null && booking != null && !preanEqoNbr.equalsIgnoreCase(booking.getEqboNbr())) {
                prean.setFieldValue(_panFields.PREAN_EQO_NBR, booking.getEqboNbr());
                prean.setFieldValue(_panFields.PREAN_STATUS, "OK")
                if (prean.getGapptUnit() != null) {
                    UnitFacilityVisit ufv = prean.getGapptUnit().getUfvForFacilityLiveOnly(prean.getGapptGate().getGateFacility());
                    if (ufv != null) {
                        propagatePreanStatusToUfv(ufv, "OK", prean.isReceival());
                    }
                }
            }
        }

    }

    private void propagatePreanStatusToUfv(UnitFacilityVisit inUfv, String inNewPreanStatus, boolean isReceival) {
        if (isReceival) {
            inUfv.setFieldValue(_panFields.UFV_PREAN_RECEIVAL_STATUS, inNewPreanStatus);
        } else {
            inUfv.setFieldValue(_panFields.UFV_PREAN_DELIVERY_STATUS, inNewPreanStatus);
        }
    }

    void setUfvOutboundCarrierToGeneric(UnitFacilityVisit inUfv, GateAppointment inPrean) {

        String gateId = inPrean.getGapptGate().getGateId();

        CarrierVisit genCarrierVisit = null;

        if ("BARGE".equals(gateId)) {
            genCarrierVisit = CarrierVisit.findVesselVisit(inPrean.getGapptGate().getGateFacility(), "GEN_BARGE");
        } else if ("RAIL".equals(gateId)) {
            genCarrierVisit = CarrierVisit.getGenericTrainVisit(ContextHelper.getThreadComplex());
        }

        if (genCarrierVisit != null) {

            inUfv.updateObCv(genCarrierVisit);
        }

    }

    public void preDelete(Entity inEntity) {
        deletePreanErrors(inEntity);
    }

    private void deletePreanErrors(Entity inEntity) {
        DomainQuery dq = QueryUtils.createDomainQuery(PREAN_VALIDATION_ERROR)
                .addDqPredicate(PredicateFactory.eq(PREAN_KEY, inEntity.getPrimaryKey()));

        HibernateApi.getInstance().deleteByDomainQuery(dq);
    }

    public void logImportantInfo(TruckVisitAppointment inTva, GateAppointment inGappt) {

        log("=========   Gappt  info ==========");
        log("APPT NBR: " + inGappt.getLogEntityId());
        log("===== TVA info ====");

        log("APPT NBR: " + inTva.getLogEntityId());
        log("REF NBR: " + inTva.getTvapptReferenceNbr());
        log("STATE: " + inTva.getTvapptState().getKey());
        log("APPTS" + inTva.getTvapptAssociatedGateApptIds());
        log("CHANGER" + inTva.getTvapptChanger());
        log("NOTES" + inTva.getTvapptNotes());

        log("===== USER INFO ====");
        log("USER ID: " + ContextHelper.getThreadUserId());

        log("====  STACK TRACE =====");

        Throwable stackTrace = new Throwable();
        stackTrace.fillInStackTrace();

        String stackTraceText = ExceptionUtils.getStackTrace(stackTrace);
        log(stackTraceText);

    }

    public boolean wasUnitJustCreatedByPrean(Unit unit) {

        boolean wasCreated = false;

        Date unitCreated = (unit == null) ? null : unit.getUnitCreateTime();
        Date currentTime = ArgoUtils.timeNow();
        if (unitCreated != null) {
            long diffMillis = Math.abs(unitCreated.getTime() - currentTime.getTime());
            wasCreated = diffMillis < 10000; // 10 seconds
        }
        return wasCreated;
    }


    private static String PREAN_VALIDATION_ERROR = "com.navis.external.custom.CustomPreanValidationError";
    public static MetafieldId PREAN_KEY = MetafieldIdFactory.valueOf("customEntityFields.custompaverrPreanKey");

    private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");

    public static MetafieldId SEND_MSG = _panFields.SEND_MSG;
    public static MetafieldId RESPONSE_MSG_TYPE = _panFields.RESPONSE_MSG_TYPE;

    private static String NO_RESPONSE_STR = "NO_RESPONSE";
    private static String STATUS_UPDATE = "STATUS_UPDATE";
    private Long RAIL_ITT_APPT_NBR = 300000000L;
    private static String RAIL_ITT_GATE = "RAIL_ITT";

    private static final Logger LOGGER = Logger.getLogger(this.class);


}
