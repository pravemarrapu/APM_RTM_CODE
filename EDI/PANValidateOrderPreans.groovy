/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 * Date 09/04/14 CSDV-1841 fixed method signatures
 *
 *
 * Date 10/09/2014 CSDV-2234 pass event type ID to appt.submit. The event type ID is used in the validation flow to decide if a task should be run
 *                           (usually a task affecting the data, such as CreateCtrPreadvisedVisit)
 *
 * Date 07/10/2014 CSDV-2415  Only EDOs are used for empty piuckups
 *
 * 25/03/2015 CSDV-2788 (APMT Mantis-5600/5601) fixed validateDOEPreansForVVBeginRcv syntax(passing subTypes parameter)
 *
 * 30/06/2015 CSDV-2786 If Book override cutoff value changed, re-validate the associated preans (add BOOKING_PROPERTY_UPDATE gen notice to trigger)
 *
 * 09/07/2015 CSDV-2832
 */

package com.navis.apex.groovy.mv2_rwg.preannouncements.validation;

import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.navis.apex.business.model.GroovyInjectionBase;
import com.navis.argo.ContextHelper;
import com.navis.argo.business.model.GeneralReference;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.framework.business.Roastery;
import com.navis.framework.esb.client.ESBClientHelper;
import com.navis.framework.esb.server.FrameworkMessageQueues;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.util.BizViolation;
import com.navis.orders.business.eqorders.Booking;
import com.navis.orders.business.eqorders.EquipmentOrder;
import com.navis.orders.business.eqorders.EquipmentReceiveOrder;
import com.navis.orders.business.eqorders.RailOrder;
import com.navis.road.RoadApptsField;
import com.navis.road.business.appointment.api.AppointmentFinder;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.AppointmentStateEnum;
import com.navis.road.business.atoms.GateClientTypeEnum;
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum;
import com.navis.services.business.event.Event;
import com.navis.services.business.event.GroovyEvent
import com.navis.orders.business.eqorders.EquipmentDeliveryOrder
import com.navis.services.business.event.EventFieldChange
import com.navis.orders.OrdersField;

/**
 * all preans linked to the bkg are found and re-validated
 *
 *
 * Author: Sophia Robertson
 * Date: 03/04/13
 * Called from: General Notices
 */


public class PANValidateOrderPreans extends GroovyInjectionBase {

    public void execute(GroovyEvent inGroovyEvent) {
        log("PANValidateOrderPreans.start");
        Event evnt = inGroovyEvent.getEvent();
        Serializable eqoPk = evnt.getEventAppliedToGkey();
        EquipmentOrder eqo = (EquipmentOrder)HibernateApi.getInstance().load(EquipmentOrder.class, eqoPk);
        log("eqo.getEqboNbr "+eqo.getEqboNbr());
        _extensionData = new ArrayList<String>();
        _extensionData.add(0,evnt.getEvntEventType().getId());
        log("Additonal Information :: "+evnt.getEvntEventType().getId())

        if ("BOOKING_OVRD_VV_BGN_RCV_FLAG_UPDATE".equals(evnt.getEvntEventType().getId())){
            log("Additonal Information :: Inside the method to start Validate DOE")

            validateDOEPreansForVVBeginRcv(Booking.resolveBkgFromEqo(eqo));

        }
        else if (evnt.getEvntEventType().getId().startsWith("EQ_UPDATE_")   ) {

            if (eqo instanceof Booking || eqo instanceof RailOrder || eqo  instanceof EquipmentReceiveOrder) {

                validateDropoffPreans(eqo, evnt.getEvntEventType().getId());
            }
            // only EDOs are used for empty pickups
            if (eqo instanceof EquipmentDeliveryOrder) {
                validatePickupPreans(eqo, evnt.getEvntEventType().getId());
            }

        }
        else if ("BOOKING_PROPERTY_UPDATE".equals(evnt.getEvntEventType().getId())){

            EventFieldChange ovrdCutoffFc = getFieldChange(evnt, OrdersField. EQO_OVERRIDE_CUTOFF);

            if (ovrdCutoffFc != null) {

                validateDropoffPreans(eqo, evnt.getEvntEventType().getId());
            }


        }
        log("PANValidateOrderPreans.end");
    }

    private void validateDOEPreansForVVBeginRcv(Booking inBkg) {

        log("PANValidateOrderPreans.validateDOEPreansForVVBeginRcv start");
        Map<MetafieldId, Object> queryFields = new HashMap<MetafieldId, Object>();
        queryFields.put(RoadApptsField.GAPPT_ORDER, inBkg.getPrimaryKey());
        TruckerFriendlyTranSubTypeEnum[] subTypes = [TruckerFriendlyTranSubTypeEnum.DOE];
        List<GateAppointment> bkgApptList = _apptFndr.findGateAppointmentsByFields(queryFields, subTypes, AppointmentStateEnum.CREATED);
        //for COPINO 13 dropoffs
        log("PANValidateOrderPreans.validateDOEPreansForVVBeginRcv :: bkgApptList :: "+bkgApptList);
        if (bkgApptList ==  null || bkgApptList.isEmpty()) {

            queryFields = new HashMap<MetafieldId, Object>();
            queryFields.put(_panFields.PREAN_EQO_NBR, inBkg.getEqboNbr());
            subTypes = [TruckerFriendlyTranSubTypeEnum.DOE];
            bkgApptList = _apptFndr.findGateAppointmentsByFields(queryFields, subTypes, AppointmentStateEnum.CREATED);
        }

        log("PANValidateOrderPreans.validateDOEPreansForVVBeginRcv :: bkgApptList After:: "+bkgApptList);
        for (GateAppointment appt : bkgApptList) {
            try {
                appt.setGapptOrderNbr(inBkg.getEqboNbr());
                appt.submit(GateClientTypeEnum.CLERK, _extensionData);

            } catch (BizViolation bv) {
                log(Level.ERROR, bv.getMessage());
            }

        }
    }

    private void validateDropoffPreans(EquipmentOrder inEqo, String inEventId) {
        log("PANValidateOrderPreans.validateDropoffPreans start");
        Map<MetafieldId, Object> queryFields = new HashMap<MetafieldId, Object>();
        queryFields.put(_panFields.PREAN_EQO_NBR, inEqo.getEqboNbr());
        TruckerFriendlyTranSubTypeEnum[] subTypes = [TruckerFriendlyTranSubTypeEnum.DOE, TruckerFriendlyTranSubTypeEnum.DOM, TruckerFriendlyTranSubTypeEnum.DOI];

        List<GateAppointment> preans = _apptFndr.findGateAppointmentsByFields(queryFields, subTypes, AppointmentStateEnum.CREATED);

        //results are ordered by requested date (in the finder)
        for (GateAppointment prean : preans) {

            try {

                if (prean.getGapptOrder() == null) {

                    if (TruckerFriendlyTranSubTypeEnum.DOM.equals(prean.getGapptTranType()) && ("EQ_UPDATE_BKG".equals(inEventId) || "EQ_UPDATE_RO".equals(inEventId))) {

                        sendEmail(prean);
                        return;

                    }

                    prean.setGapptOrderNbr(inEqo.getEqboNbr());
                }
                else {

                    if(inEqo.getEqboGkey() != prean.getGapptOrder().getEqboGkey()) {
                        return;
                    }
                }

                prean.submit(GateClientTypeEnum.CLERK, _extensionData);


            } catch (BizViolation bv) {

                log(bv.getMessage());
            }

        }
        log("PANValidateOrderPreans.validateDropoffPreans end");
    }

    private void validatePickupPreans(EquipmentOrder inEqo, String inEventId) {

        Map<MetafieldId, Object> queryFields = new HashMap<MetafieldId, Object>();
        queryFields.put(_panFields.PREAN_EQO_NBR, inEqo.getEqboNbr());
        TruckerFriendlyTranSubTypeEnum[] subTypes = [TruckerFriendlyTranSubTypeEnum.PUM];

        List<GateAppointment> preans = _apptFndr.findGateAppointmentsByFields(queryFields, subTypes, AppointmentStateEnum.CREATED);

        //results are ordered by requested date (in the finder)
        for (GateAppointment prean : preans) {
            try {

                if (prean.getGapptOrder() == null) {
                    prean.setGapptOrderNbr(inEqo.getEqboNbr());
                }
                else {
                    if(inEqo.getEqboGkey() != prean.getGapptOrder().getEqboGkey()) {
                        return;
                    }
                }

                prean.submit(GateClientTypeEnum.CLERK, _extensionData);

            } catch (BizViolation bv) {
                log(bv.getMessage());
            }

        }

    }
    private void sendEmail(GateAppointment inPrean) {
        try {

            GeneralReference emailGf = GeneralReference.findUniqueEntryById("PREANNOUNCEMENT","EMAIL","TRANS_TYPE_MISMATCH");
            if (emailGf != null) {

                String emailAddress = emailGf.getRefValue1();
                String fromEmailAddress = emailGf.getRefValue2();
                String subject = "Update COPINO does not match original COPINO tran type";
                String message = "Original Preannouncement EDI Tran Ref Nbr: " + inPrean.getApptRefNbr();

                ESBClientHelper.sendEmailAttachments(ContextHelper.getThreadUserContext(), FrameworkMessageQueues.EMAIL_QUEUE, emailAddress, fromEmailAddress, subject, message, null);
            }
            else {
                log("General Reference for PREANNOUNCEMENT/EMAIL/TRANS_TYPE_MISMATCH hasn't been set up");
            }

        } catch (Exception e) {
            log(e.getMessage());
        }
    }

    private EventFieldChange getFieldChange(Event inEvent, MetafieldId inFieldId) {

        Set changes = inEvent.getEvntFieldChanges();
        for (Object change : changes) {
            EventFieldChange eventFieldChange = (EventFieldChange) change;
            if (eventFieldChange.getEvntfcMetafieldId().equals(inFieldId.getFieldId())) {
                return eventFieldChange;
            }
        }

        return null;
    }

    private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
    private static AppointmentFinder _apptFndr =  (AppointmentFinder)Roastery.getBean(AppointmentFinder.BEAN_ID);
    List<String> _extensionData = null;
}