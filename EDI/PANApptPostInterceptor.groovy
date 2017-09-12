import com.navis.inventory.InvField

/*
 * Copyright (c) 2015 Navis LLC. All Rights Reserved.
 *
 * @version $Id: PANApptPostInterceptor.groovy 231226 2015-10-14 12:40:21Z extroberso $
 *
 */


import org.apache.commons.lang.ObjectUtils
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.xmlbeans.XmlObject
import org.hibernate.exception.ConstraintViolationException
import org.jdom.Element
import org.springframework.dao.DataIntegrityViolationException

import com.google.common.collect.Lists;
import com.navis.argo.AppointmentTransactionDocument
import com.navis.argo.AppointmentTransactionsDocument
import com.navis.argo.ArgoPropertyKeys
import com.navis.argo.ContextHelper
import com.navis.argo.EdiFacility
import com.navis.argo.EdiTrainVisit
import com.navis.argo.EdiVesselVisit
import com.navis.argo.EdiWeight
import com.navis.argo.Port
import com.navis.argo.RailRoad
import com.navis.argo.ShippingLine
import com.navis.argo.AppointmentTransactionDocument.AppointmentTransaction
import com.navis.argo.business.api.ArgoEdiFacade
import com.navis.argo.business.api.ArgoEdiUtils
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.api.EdiPoster
import com.navis.argo.business.atoms.BizRoleEnum
import com.navis.argo.business.atoms.DataSourceEnum
import com.navis.argo.business.atoms.DrayStatusEnum
import com.navis.argo.business.atoms.EquipClassEnum
import com.navis.argo.business.atoms.EventEnum
import com.navis.argo.business.atoms.FreightKindEnum
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.atoms.WeightUnitEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.Complex
import com.navis.argo.business.model.EdiPostingContext
import com.navis.argo.business.model.Facility
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.model.VisitDetails
import com.navis.argo.business.reference.Container
import com.navis.argo.business.reference.EquipType
import com.navis.argo.business.reference.Equipment;
import com.navis.argo.business.reference.LineOperator;
import com.navis.argo.business.reference.RoutingPoint
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.edi.business.edimodel.EdiConsts
import com.navis.edi.business.entity.EdiSession
import com.navis.external.edi.entity.AbstractEdiPostInterceptor
import com.navis.external.framework.util.ExtensionUtils
import com.navis.framework.business.Roastery
import com.navis.framework.esb.client.ESBClientHelper
import com.navis.framework.esb.server.FrameworkMessageQueues
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.persistence.HibernatingEntity
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.BizFailure
import com.navis.framework.util.BizViolation
import com.navis.framework.util.BizWarning;
import com.navis.framework.util.DateUtil
import com.navis.framework.util.internationalization.PropertyKey
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.atoms.UfvTransitStateEnum;
import com.navis.inventory.business.atoms.UnitVisitStateEnum;
import com.navis.inventory.business.units.EqBaseOrder;
import com.navis.inventory.business.units.EqBaseOrderItem;
import com.navis.inventory.business.units.EqBaseOrderItemHbr;
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.orders.OrdersField
import com.navis.orders.business.api.OrdersFinder;
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EqTypeGroup;
import com.navis.orders.business.eqorders.EquipmentDeliveryOrder
import com.navis.orders.business.eqorders.EquipmentOrder
import com.navis.orders.business.eqorders.EquipmentOrderItem
import com.navis.orders.business.eqorders.EquipmentReceiveOrder
import com.navis.orders.business.eqorders.RailOrder
import com.navis.rail.business.atoms.TrainDirectionEnum
import com.navis.rail.business.entity.TrainVisitDetails
import com.navis.road.RoadApptsEntity
import com.navis.road.RoadApptsField
import com.navis.road.RoadApptsPropertyKeys
import com.navis.road.RoadPropertyKeys
import com.navis.road.business.apihandler.AppointmentApiXmlUtil
import com.navis.road.business.appointment.api.AppointmentFinder
import com.navis.road.business.appointment.api.IAppointmentManager
import com.navis.road.business.appointment.api.TimeSlotBean
import com.navis.road.business.appointment.model.AppointmentQuotaRule
import com.navis.road.business.appointment.model.AppointmentTimeSlot
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.appointment.model.TruckVisitAppointment
import com.navis.road.business.atoms.AppointmentStateEnum
import com.navis.road.business.atoms.GateClientTypeEnum
import com.navis.road.business.atoms.TranSubTypeEnum
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum
import com.navis.road.business.model.Gate
import com.navis.road.business.model.TruckingCompany
import com.navis.road.business.util.RoadBizUtil
import com.navis.road.business.util.TransactionTypeUtil;
import com.navis.road.util.appointment.AppointmentIdProvider

/**
 * Intercepts COPINO Appointment Posting
 *
 * Author: Sophia Robertson
 * Date: 03/04/13
 * Hooked to EDI Copino Sessions for Truck, Barge, and Rail
 *
 * Date: 03/10/13 CSDV-1410 When cancelling a prean get cancel note from General Refs
 * Date: 07/10/13 CSDV-1409 When a ctrId is missing in the edi message, it is set to DUMMYCTRID in the mapping. beforeEdiPost checks for this vaule and wipes it out.
 * Date: 04/11/13 CSDV-1503 Set prean field EDI Partner Name
 * Date: 07/11/13 CSDV-1503 modify deriveCategory: uncomment the logic getting the category from the existing unit for pickups
 * Date  28/01/14 CSDV-1630 fixed private vs. public/protected methods issue for 3.0.1 upgrade
 *
 * Date  12/02/14 CSDV-1644 When COPINO update has a different TAR the updated prean will be associated with the supplied TAR
 *                          If both Requested time and TAR are changed, the Requested time is ignored
 *                          If there are no corresponding ctr slots for the tar, the post will fail.
 *  Date 13/02/14 CSDV-1637 modified deriveCategory: If the COPINO is for a full container and an active unit exists for import with dray status DRAY-IN than the transaction type should be Drop off Import
 *  Date 17/02/14 Fixed csdv-1644 and csdv-1637
 *  Date 06/03/14 CSDV-1637 fixed deriveCategory for the dropoff COPINO with a non-active pre-existing unit
 *  Date 09/04/14 CSDV-1841 fixed method signatures
 *  Date 14/05/14 CSDV-1657 Temp fix while waiting for the product fix - instead of the  call to AppointmentManager.getAppointmentTimeSlot, call  local getAppointmentTimeSlot which is a modified copy. The modification bypassed the setting lock
 *                          functionality therefore avoiding the Stale State error
 *  Date 05/06/14 CSDV-1244
 *  Date 10/06/14 CSDV-1244 pass eqo to resolveDuplicatePrean so that the unit preadvise to an order is cancelled if applicable
 *
 *  Date 25/06/14 CSDV-2152 use GateAppointment dynamic flex fields instead of gapptUfv and gapptUnit flex fields for prean specific values
 *
 *  Date 27/06/14 CSDV-2156 use Tva Reference Nbr instead of Tva Appointment Nbr as TAR, so that when a prean truck appointment time is changed and a new Tva is created, the TAR will stays the same
 Date 05/07/14 CSDV-2159 Set SKIP_POSTER to TRUE in beforeEdiPost and moved all the processing into afterEdiPost;
 Pre-generate app nbrs for gappt(s) and tva before any invocations of hibrnate instance.save are made.
 The reason - when an id is generated by AppointmentIdProvider and the created transaction is commited to the database, all the transitory entities
 in the outer transaction are also commited to the db. So if an error occures after that, there is no rollback;
 When more then one prean is connected to the same TAR and it's a COPINO with a time update is processed,
 the COPINO prean is deleted and a new one is created with a new time slot and a new Tva is created for it (exist. funct)
 All the other preans that were linked to the original TAR, are deleted and created with a new time slot
 and  linked to the new Tva. Note: The "find next avaiable timeslot" functionality is turned off in this case.
 Date 13/08/14 CSDV- 2219  Post will fail if barge/train carrier visit Stop Accepting COPINOs is set to true
 * Date 07/10/2014 CSDV-2415  Only EDOs are used for empty piuckups
 *
 * Date 05/03/2015 APMT Mantis-4170 (when requested time of the existing prean changed, set prean PIN nbr before validating mutiple preans)
 *
 Date 10/04/2015 APMT Mantis-6149 Modified findOrder: if booking is found, but it's FreightKind does not match edi freight kind, ignore the bookng
 Date 15/04/2015 CSDV-2839 (APMT Mantis-6081) Added flush after previous COPINO 13 is cancelled
 Date 21/04/2015  CSDV-2733 Moved unit cleanup functionality to PANPreanUtils
 Date 04-05-2015 CSDV-2832 Allow COPINO 13 Dropoffs to be posted
 Date 25/06/2015 CSDV-2832 re-factored setting prean Send Msg functionality
 Date 02/09/2015 CSDV-2159 made variables fcy and truckPrean local to eliminate issues caused by singleton pattern for extension invocation
 Date 24/09/2015 CSDV-3040 If a 'Create' COPINO contains a TAR and the corresponding active TVA is found, the edi appointment time will be set to the TVA's requested time
 Date 09/10/2015 CSDV-3188 If ctrID, line or booking changes in COPINO update, call getLibrary("PANPreanUtils").cleanupPreanUnit(originalPrean);
 Modified By: Pradeep Arya
 Date 22-Feb-17 WF#798457 - if prean already exist for the container, system should not allow to cancel and create the new one for another TC;
 Date 20-Mar-17 WF#801147 - EDO validation for DM
 Modified By: Praveen Babu
 Date 06/09/2017 APMT #24 and #32 - Logic to update the prean eqo number and order number for RAIL_ITT gate transactions.
 Author: Gopinath Kannappan
 Date 07/09/2017 APMT #34 - OOG is not updating from Booking to Unit, hence updated the OOG information to Unit from booking.
 Modified By: Praveen Babu
 Date 11/09/2017 APMT #25 - Update the right validation run id to Prean.

 */

public class PANApptPostInterceptor extends AbstractEdiPostInterceptor {

    public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
        AppointmentTransactionDocument.AppointmentTransaction apptTran = getApptTran(inXmlTransactionDocument);
        log("beforeEdiPost: $apptTran")
        inParams.put(EdiConsts.SKIP_POSTER, Boolean.TRUE);

    }

    public void afterEdiPost(XmlObject inXmlTransactionDocument, HibernatingEntity inHibernatingEntity, Map inParams) {

        AppointmentTransactionDocument.AppointmentTransaction apptTran = getApptTran(inXmlTransactionDocument);
        log("afterEdiPost: $apptTran")

        if (apptTran != null) {

            try {

                process(ContextHelper.getThreadEdiPostingContext(), apptTran, inParams, inXmlTransactionDocument);

            } catch (BizViolation bv) {

                RoadBizUtil.appendExceptionChain(bv);

            }
        }

    }


    private void process(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, Map inParms, XmlObject inXmlTransactionDocument) throws BizViolation {

        validateTranRefNbr(inEdiAppointment.getInterchange().getInterchangeNumber(), inEdiPostingContext);

        inEdiPostingContext.throwIfAnyViolations();

        String msgFunction = inEdiAppointment.getMsgFunction();
        String basicType = inEdiAppointment.getAppointmentType();

        Facility fcy = determineFacility(inEdiPostingContext, inEdiAppointment);

        inEdiPostingContext.throwIfAnyViolations();

        inEdiPostingContext.setTimeZone(fcy.getTimeZone());

        Gate gate = determineGate(inEdiPostingContext, inEdiAppointment, fcy);
        inEdiPostingContext.throwIfAnyViolations();

        CarrierVisit landsideCarrierVisit = determineLandsideVisit(gate, inEdiPostingContext, inEdiAppointment);
        inEdiPostingContext.throwIfAnyViolations();


        String ediCtrApptPriorRefNbr = inEdiAppointment.getMsgReferenceNbr();

        setEdiCtrId(inEdiAppointment);
        String ctrId = inEdiAppointment.getContainerId();
        log("Container:" + ctrId);

        Date ediRequestDateTime = mergeDateTime(inEdiPostingContext, inEdiAppointment);

        GateAppointment prean = null;

        //16-May-2017 : Create the Prean for the RAIL ITT gate.
        boolean truckPrean = !(BARGE_GATE.equals(gate.getGateId()) || RAIL_GATE.equals(gate.getGateId()) || RAIL_ITT_GATE.equalsIgnoreCase(gate.getGateId()));

        String ediTvApptRefNbr;
        TruckVisitAppointment ediTvAppt;

        if (truckPrean) {
            ediTvApptRefNbr = inEdiAppointment.getChassisAccessoryType();
            ediTvAppt = validateTar(inEdiPostingContext, inEdiAppointment, ediTvApptRefNbr);
            if (ediTvAppt != null) {
                Calendar originalEdiApptTime = inEdiAppointment.getAppointmentTime();
                setNewEdiApptTime(ediTvAppt.getTvapptRequestedDate(), originalEdiApptTime, inEdiAppointment);
            }
            inEdiPostingContext.throwIfAnyViolations();
        }

        ScopedBizUnit line = getLine(inEdiAppointment, inEdiPostingContext, gate);
        inEdiPostingContext.throwIfAnyViolations();

        String freightKindStr = inEdiAppointment.getFreightKind();
        FreightKindEnum freightKind = FreightKindEnum.getEnum(freightKindStr);

        EquipmentOrder eqo = null;
        String ediEqoNbr = inEdiAppointment.getOrderNbr();
        log("ediEqoNbr:$ediEqoNbr");

        if (ediEqoNbr != null) {

            eqo = findOrder(inEdiPostingContext, inEdiAppointment, line, basicType, freightKind);
            log("eqo:" + eqo);

            //commented out as it's causing issue on posting DM preannoucement
            //WF#801147 - make the edo order available for DM posting
            // inEdiAppointment.setOrderNbr(null);

            inParms.put(EDI_ORDER_NBR_KEY, ediEqoNbr);

            if (eqo != null) {
                inParms.put(EDI_ORDER_KEY, eqo);

            }
        }
        // Do not raise the error if the current transaction is an RAIL-ITT COPINO receive flow it is handled in the validate RAIL ITT COPINO validations.
        else if (inEdiAppointment.getReleaseNbr() == null && !(DROPOFF.equalsIgnoreCase(inEdiAppointment.getAppointmentType()))) {
            BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_ORDER_NBR_OR_RELEASE_NBR_REQUIRED"), (Throwable) null, null, null, null);
            inEdiPostingContext.addViolation(bv);
        }



        if (!truckPrean && !MSG_FUNCTION_GET_STATUS_UPDATE.equals(msgFunction)) {
            setEdiAppointmentDateTimeToEta(gate, landsideCarrierVisit, inEdiPostingContext, inEdiAppointment);
        }

        inEdiPostingContext.throwIfAnyViolations();

        //Set Category
        UnitCategoryEnum categoryEnum = deriveCategory(inEdiAppointment, eqo, fcy);
        inEdiAppointment.setCategory(categoryEnum.getKey());

        TruckerFriendlyTranSubTypeEnum apptType = determineAppointmentType(inEdiPostingContext, inEdiAppointment);

        TruckVisitAppointment originalPreanTvAppt;

        _apptNbrs = new ArrayList();

        if (MSG_FUNCTION_CREATE.equals(msgFunction)) {

            checkIfCopinoAccepted(inEdiPostingContext, landsideCarrierVisit);
            inEdiPostingContext.throwIfAnyViolations();

            generateApptNbrs(1);

            if (truckPrean) {

                boolean useSuppliedTar = false;

                if (ediTvAppt != null) {
                    Date tvApptRequestedDate = trimMillis(ediTvAppt.getTvapptRequestedDate());

                    Map<Date, TimeSlotBean> gapptSlots = _appMgr.getOpenTimeSlotsForDate(gate.getGateGkey(), apptType.getInternalTranSubTypeEnum(), tvApptRequestedDate, false, null, null);
                    if (gapptSlots != null && !gapptSlots.isEmpty()) {
                        TimeSlotBean gapptSlotBean = getExactTimeSlot(gapptSlots, tvApptRequestedDate);
                        useSuppliedTar = gapptSlotBean != null;
                    }
                }

                if (!useSuppliedTar) {
                    generateApptNbrs(1);
                    changeRequestedTimeIfNeeded(inEdiPostingContext, inEdiAppointment, null, gate, apptType, inParms, true);
                    ediTvAppt = null;
                }
            }

            if (ctrId != null) {
                //Find a prean for the ctr/direction/order or pin
                //If exists and the same modality and transport co - error
                //If exists for a diff modality and/or operator, cancel the previous one

                resolveDuplicatePrean(inEdiPostingContext, inEdiAppointment, basicType, ctrId, gate.getGateId(), landsideCarrierVisit, eqo, apptType);

            }

        } else if (MSG_FUNCTION_UPDATE.equals(msgFunction)) {

            GateAppointment originalPrean = findPrean(inEdiPostingContext, ediCtrApptPriorRefNbr);

            if (originalPrean != null) {

                checkIfCopinoAccepted(inEdiPostingContext, (CarrierVisit) originalPrean.getField(PREAN_LANDSIDE_CARRIER_VISIT));
                inEdiPostingContext.throwIfAnyViolations();

                if (!apptType.equals(originalPrean.getGapptTranType())) {
                    processTranTypeMismatch(inEdiPostingContext, originalPrean);
                    inEdiPostingContext.throwIfAnyViolations();

                }

                if (truckPrean) {

                    originalPreanTvAppt = originalPrean.getGapptTruckVisitAppointment();

                    if (ediTvAppt != null && (originalPreanTvAppt == null || ediTvAppt.getApptNbr() != originalPreanTvAppt.getApptNbr())) {

                        inParms.put("TAR_CHANGED", "YES");
                        boolean useSuppliedTar = false;

                        Date tvApptRequestedDate = trimMillis(ediTvAppt.getTvapptRequestedDate());

                        Map<Date, TimeSlotBean> gapptSlots = _appMgr.getOpenTimeSlotsForDate(gate.getGateGkey(), apptType.getInternalTranSubTypeEnum(), tvApptRequestedDate, false, null, null);
                        if (gapptSlots != null && !gapptSlots.isEmpty()) {
                            TimeSlotBean gapptSlotBean = getExactTimeSlot(gapptSlots, tvApptRequestedDate);
                            useSuppliedTar = gapptSlotBean != null;
                        }
                        if (useSuppliedTar) {

                            generateApptNbrs(1);

                            if (originalPreanTvAppt != null) {
                                originalPrean.disassociateTva();
                                inParms.put("ORIG_TAR", originalPreanTvAppt);
                            }
                        } else {
                            BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("NO_CTR_SLOTS_FOR_TAR_TIME"), (Throwable) null, null, null, null);
                            inEdiPostingContext.addViolation(bv);
                        }
                    } else {

                        if (originalPrean.getGapptRequestedDate() != ediRequestDateTime) {

                            generateApptNbrs(originalPreanTvAppt.getActiveAppointments().size() + 1);

                            originalPrean.disassociateTva();

                            inParms.put("PREAN_REQUESTED_DATE_UPDATE", "YES");
                            inParms.put("ORIG_PREAN", originalPrean);
                            inParms.put("ORIG_TAR", originalPreanTvAppt);

                            changeRequestedTimeIfNeeded(inEdiPostingContext, inEdiAppointment, originalPrean.getGapptRequestedDate(), gate, apptType, inParms, false);

                        }

                    }

                }

                inEdiPostingContext.throwIfAnyViolations();

                Unit oldUnit = originalPrean.getGapptUnit();
                //CSDV-3188
                boolean ctrChangeReqested = oldUnit != null && ctrId != null && !oldUnit.getUnitId().equals(ctrId);

                if (ctrChangeReqested || ediEqoNbr != originalPrean.getFieldString(_panFields.PREAN_EQO_NBR) ||
                        isDifferentCtrType(oldUnit, inEdiAppointment.getContainerType(), inEdiPostingContext) ||
                        line != originalPrean.getGapptLineOperator()) {

                    getLibrary("PANPreanUtils").cleanupPreanUnit(originalPrean);

                }

                inEdiAppointment.setAppointmentId(originalPrean.getApptNbr().toString());
            }

        } else if (MSG_FUNCTION_CANCEL.equals(msgFunction)) {

            prean = findPrean(inEdiPostingContext, ediCtrApptPriorRefNbr);

            if (prean != null) {

                checkIfCopinoAccepted(inEdiPostingContext, (CarrierVisit) prean.getField(PREAN_LANDSIDE_CARRIER_VISIT));
                inEdiPostingContext.throwIfAnyViolations();

                if (truckPrean) {

                    prean.disassociateTva();
                }

                getLibrary("PANPreanUtils").cancelPrean(prean, PREANNOUNCEMENT, "CANCELLED_BY_SENDER", true);

                prean.setFieldValue(EDI_TRANS_REF_NBR, inEdiAppointment.getInterchange().getInterchangeNumber());
                prean.setFieldValue(EDI_TRAN_GKEY, inParms.get(EdiConsts.TRANSACTION_GKEY));

                //if the unit wasn't deleted by cancelAppt, it means it was not created as a part of the prean posting
                // and just needs to be clean-up (Prean Status wiped out, etc)
                /*if (prean.getGapptUnit() != null) {
                 cleanupPreanUnit(prean, basicType, eqo);
                 }*/
            }

        }
        //Barge, Rail only
        else if (MSG_FUNCTION_GET_STATUS_UPDATE.equals(msgFunction)) {
            log("PANApptPostInterceptor if MSG_FUNCTION_GET_STATUS_UPDATE ");

            if (ctrId == null) {
                if (PICKUP.equals(basicType)) {

                    BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_CTR_ID_MISSING"), (Throwable) null, null, null, null);
                    inEdiPostingContext.addViolation(bv);
                    inEdiPostingContext.throwIfAnyViolations();
                }
            }

            generateApptNbrs(1);

            TruckerFriendlyTranSubTypeEnum[] tranTypes = [apptType];
            GateAppointment extPrean = _apptFndr.findAppointmentByContainerId(ctrId, tranTypes, AppointmentStateEnum.CREATED);

            if (extPrean != null) {

                //duplicate prean by different TC must not be cancelled however the COPINO must get Reject and email notification
                // should be sent out
                String ediTrkCmpny = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyId();
                String preanTrkCmpny = extPrean.getGapptTruckingCompany().getBzuId();
                log("ediTrkCmpny:$ediTrkCmpny + preanTrkCmpny:$preanTrkCmpny");
                if (!ediTrkCmpny.equals(preanTrkCmpny)) {
                    String message = "pre-announcement:[" + extPrean.getGapptNbr() + "] for unit:[$ctrId]...already exists.";
                    GeneralReference gr = GeneralReference.findUniqueEntryById("PREANNOUNCEMENT", "EMAIL", "DUPLICATE_PREAN");
                    String emailAddress = gr ? gr.getRefValue1() : "";
                    if (emailAddress) {
                        try {
                            ESBClientHelper.sendEmailAttachments(ContextHelper.getThreadUserContext(),
                                    FrameworkMessageQueues.EMAIL_QUEUE, emailAddress, "noreply@apmterminals.com", message, message, null);
                        }
                        catch (Exception e) {
                            log("Exception:" + e.getMessage());
                        }
                    }
                    Object[] parms = [ctrId];
                    BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_ACTIVE_PREAN_EXISTS_FOR_CTR_TRANS_TYPE"), (Throwable) null, null, null, parms);
                    inEdiPostingContext.addViolation(bv);

                }

                if (STATUS_UPDATE.equals(extPrean.getFieldString(RESPONSE_MSG_TYPE))) {

                    getLibrary("PANPreanUtils").cancelPrean(extPrean, STATUS_UPDATE_REQST, "NEW_RECEIVED", true);

                    HibernateApi.getInstance().flush();
                    inEdiAppointment.setAppointmentId(null);
                } else {
                    //"Active prean for ctr {0} already exists"
                    Object[] parms = [ctrId];
                    BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_ACTIVE_PREAN_EXISTS_FOR_CTR_TRANS_TYPE"), (Throwable) null, null, null, parms);
                    inEdiPostingContext.addViolation(bv);

                }

            }
        }

        inEdiPostingContext.throwIfAnyViolations();

        if (!MSG_FUNCTION_CANCEL.equals(msgFunction)) {

            //Put in data in the map for the after Post to retrieve
            inParms.put(TRUCK_VISIT_APPOINTMENT_KEY, ediTvAppt);
            inParms.put(LANDSIDE_CARRIER_VISIT_KEY, landsideCarrierVisit);
            inParms.put("APPT_NBR", getNextApptNbr());

            //17-05-2017 : Create Prean for Rail ITT gate.
            Set<String> validationError;
            if (RAIL_ITT_GATE.equalsIgnoreCase(gate.getGateId())) {
                if (DROPOFF.equalsIgnoreCase(inEdiAppointment.getAppointmentType())) {
                    validationError = validateRailITTDropOFFCopino(inXmlTransactionDocument, eqo, fcy, inEdiPostingContext, inParms);
                    inEdiPostingContext.throwIfAnyViolations();
                }/*else if(PICKUP.equalsIgnoreCase(inEdiAppointment.getAppointmentType())){
					validationError = validateRailITTPICKUPCopino(inXmlTransactionDocument, eqo, fcy, inEdiPostingContext, inParms);
					inEdiPostingContext.throwIfAnyViolations();
				}*/
            }
            //post

            EdiPoster poster = (EdiPoster) getLibrary("PANAppointmentPoster");
            GateAppointment postedPrean = poster.postToDomainModel(ContextHelper.getThreadEdiPostingContext(), inXmlTransactionDocument, inParms);

            processAfterPost(inXmlTransactionDocument, inParms, originalPreanTvAppt, postedPrean, truckPrean, validationError);

        }

    }

    /*private Set<String> validateRailITTPICKUPCopino(XmlObject inXmlTransactionDocument, EquipmentOrder inEqo, Facility inFcy, EdiPostingContext inEdiPostingContext, Map inParms){
        Set<String> validationsErrors = new HashSet<String>();
        AppointmentTransaction appointmentTransaction = getApptTran(inXmlTransactionDocument);
        Container ctr;
        String containerNbr 			= appointmentTransaction.getContainerId();
        String ediIsoType 				= appointmentTransaction.getContainerType();
        String ediLineOpStr 			= (appointmentTransaction.getEdiShippingLine()!=null? appointmentTransaction.getEdiShippingLine().getShippingLineCode() : null);
        UnitCategoryEnum ediCategory 	= UnitCategoryEnum.getEnum(appointmentTransaction.getCategory());
        FreightKindEnum ediFreightKind 	= FreightKindEnum.getEnum(appointmentTransaction.getFreightKind());

        ScopedBizUnit ediLineOpBiz;
        if(ediLineOpStr != null){
            ediLineOpBiz = LineOperator.findLineOperatorById(ediLineOpStr);
        }
        if(ediIsoType.isEmpty() || (EquipType.findEquipType(ediIsoType)== null) ) {
            ediIsoType = "UNKN";
            appointmentTransaction.setContainerType(ediIsoType);
        }
        Unit activeUnit;
        if(containerNbr != null){
            ctr = Container.findContainer(containerNbr);
            if(ctr != null){
                activeUnit = _unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), Equipment.findEquipment(containerNbr));
                if(activeUnit != null){
                    UnitCategoryEnum unitCategory 	= activeUnit.getUnitCategory();
                    FreightKindEnum unitFreightKind = activeUnit.getUnitFreightKind();
                    ScopedBizUnit lineOperator		= activeUnit.getUnitLineOperator();
                    EquipmentOrder  unitEqo = EquipmentOrder.resolveEqoFromEqbo(activeUnit.getField(UnitField.UNIT_DEPARTURE_ORDER));
                    if(!(unitEqo != null && inEqo !=null && unitEqo.getEqboNbr().equalsIgnoreCase(inEqo.getEqboNbr()) && unitFreightKind.equals(ediFreightKind))){
                        validationsErrors.add("DetailsMismatch")
                    }
                }
            }
        }
        return validationsErrors;
    }*/


    private boolean isTranshipITTExportRecivalFlow(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {
        // Validate if the current EDI transaction is Transhipment + DropOff + Export
        if (inEdiAppointment.getEdiFlexFields() != null && inEdiAppointment.getEdiFlexFields().getUnitFlexString04() != null) {
            return (!RAIL_QUALIFIER_CODES.contains(inEdiAppointment.getEdiFlexFields().getUnitFlexString04())
                    && inEdiAppointment.getAppointmentType() != null && !inEdiAppointment.getAppointmentType().isEmpty()
                    && DROPOFF.equalsIgnoreCase(inEdiAppointment.getAppointmentType())
                    && (inEdiAppointment.getCategory() && UnitCategoryEnum.EXPORT.equals(UnitCategoryEnum.getEnum(inEdiAppointment.getCategory()))));
        }
        return false
    }

    private boolean isTranshipITTEmptyRecivalFlow(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {
        if (inEdiAppointment.getEdiFlexFields() != null && inEdiAppointment.getEdiFlexFields().getUnitFlexString04() != null) {
            return (!RAIL_QUALIFIER_CODES.contains(inEdiAppointment.getEdiFlexFields().getUnitFlexString04())
                    && inEdiAppointment.getAppointmentType() != null && !inEdiAppointment.getAppointmentType().isEmpty()
                    && DROPOFF.equalsIgnoreCase(inEdiAppointment.getAppointmentType())
                    && inEdiAppointment.getFreightKind() != null && FreightKindEnum.MTY.equals(inEdiAppointment.getFreightKind()));
        }
        return false
    }

    private boolean isTranshipITTRecivalFlow(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {
        // Validate if the current EDI transaction is Transhipment + DropOff + Export
        if (inEdiAppointment.getEdiFlexFields() != null && inEdiAppointment.getEdiFlexFields().getUnitFlexString04() != null) {
            return (!RAIL_QUALIFIER_CODES.contains(inEdiAppointment.getEdiFlexFields().getUnitFlexString04())
                    && inEdiAppointment.getAppointmentType() != null && !inEdiAppointment.getAppointmentType().isEmpty()
                    && DROPOFF.equalsIgnoreCase(inEdiAppointment.getAppointmentType()));
        }
        return false
    }

    private boolean isImportITTRecivalFlow(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {
        if (inEdiAppointment.getEdiFlexFields() != null && inEdiAppointment.getEdiFlexFields().getUnitFlexString04() != null) {
            return (inEdiAppointment.getAppointmentType() != null && !inEdiAppointment.getAppointmentType().isEmpty()
                    && DROPOFF.equalsIgnoreCase(inEdiAppointment.getAppointmentType())
                    && (inEdiAppointment.getCategory() && UnitCategoryEnum.IMPORT.equals(UnitCategoryEnum.getEnum(inEdiAppointment.getCategory()))));
        }
        return false
    }

    private Set<String> validateRailITTDropOFFCopino(XmlObject inXmlTransactionDocument, EquipmentOrder inEqo, Facility inFcy, EdiPostingContext inEdiPostingContext, Map inParms) {
        LOGGER.setLevel(Level.DEBUG);
        Set<String> validationsErrors = new HashSet<String>();

        AppointmentTransaction appointmentTransaction = getApptTran(inXmlTransactionDocument);
        Container ctr;
        EquipType equipmentType;
        // Values from EDI -- start
        String containerNbr = appointmentTransaction.getContainerId();
        String ediIsoType = appointmentTransaction.getContainerType();
        String ediLineOpStr = (appointmentTransaction.getEdiShippingLine() != null ? appointmentTransaction.getEdiShippingLine().getShippingLineCode() : null);
        UnitCategoryEnum ediCategory = UnitCategoryEnum.getEnum(appointmentTransaction.getCategory());
        FreightKindEnum ediFreightKind = FreightKindEnum.getEnum(appointmentTransaction.getFreightKind());

        ScopedBizUnit ediLineOpBiz;
        if (ediLineOpStr != null) {
            ediLineOpBiz = LineOperator.findLineOperatorById(ediLineOpStr);
            if (ediLineOpBiz == null) {
                ediLineOpBiz = LineOperator.getUnknownLineOperator();
            }
        }
        if (ediIsoType.isEmpty() || (EquipType.findEquipType(ediIsoType) == null)) {
            ediIsoType = "UNKN";
            appointmentTransaction.setContainerType(ediIsoType);
        }
        LOGGER.debug("Current values from EDI :: Container Nbr :: " + containerNbr + " :: EDI ISO Type :: " + ediIsoType + " :: Edi Line OP Str :: " + ediLineOpStr
                + " :: EDI Line Operator :: " + ediLineOpBiz + " EDI Category :: " + ediCategory + " :: EDi Freight Kind :: " + ediFreightKind);
        // Values from EDI -- end

        //Check for the pre-advised unit for the current container number.
        UnitFacilityVisit preAdvisedUfv;
        if (containerNbr != null) {
            ctr = Container.findContainer(containerNbr);
            if (ctr != null) {
                preAdvisedUfv = _unitFinder.findPreadvisedUnit(inFcy, ctr);
            }
        }

        // Only for the RAIL-ITT gate, check if the current booking is available with the provided
        if (inEqo == null && appointmentTransaction.getOrderNbr() != null) {
            List<Booking> ediBookingList = Booking.findBookingsByNbr(appointmentTransaction.getOrderNbr());
            if (ediBookingList != null && ediBookingList.size() > 0) {
                inEqo = ediBookingList.get(0);
            }
        }
        LOGGER.debug("Container Nbr that is fetched :: " + ctr + " Pre-Advised UFV :: " + preAdvisedUfv);


        if (preAdvisedUfv) {
            // If pre-advised unit available fetch the values from the unit
            String eqpuipmentIsoType = preAdvisedUfv.getUfvUnit().getUnitEquipment().getEqEquipType().getEqtypId();
            UnitCategoryEnum unitCategory = preAdvisedUfv.getUfvUnit().getUnitCategory();
            FreightKindEnum unitFreightKind = preAdvisedUfv.getUfvUnit().getUnitFreightKind();
            ScopedBizUnit lineOperator = preAdvisedUfv.getUfvUnit().getUnitLineOperator();

            //If unit is associated with a booking
            EquipmentOrder unitEqo = EquipmentOrder.resolveEqoFromEqbo(preAdvisedUfv.getUfvUnit().getField(UnitField.UNIT_DEPARTURE_ORDER));

            LOGGER.debug("Current values from Pre-Advised UFV :: Container Nbr :: " + containerNbr + " :: Pre-Advised UFV ISO Type :: " + ediIsoType + " :: Pre-Advised UFV Line OP Str :: " + ediLineOpStr
                    + " :: Pre-Advised UFV Line Operator :: " + ediLineOpBiz + " Pre-Advised UFV Category :: " + ediCategory + " :: Pre-Advised UFV Freight Kind :: " + ediFreightKind
                    + " :: Unit Equipment Order :: " + unitEqo + " :: Edi Equipment Order :: " + inEqo);
            LOGGER.debug(" IS Import ITT :: " + isImportITTRecivalFlow(appointmentTransaction));

            if (!isImportITTRecivalFlow(appointmentTransaction) && !isTranshipITTEmptyRecivalFlow(appointmentTransaction)) {
                if (unitEqo == null) {
                    // inEqo is the booking retrieved from the booking number available in EDI.

                    if (inEqo != null) {
                        boolean isValid = validateBooking(inEqo, eqpuipmentIsoType, lineOperator, unitCategory, unitFreightKind, appointmentTransaction);
                        if (isValid) {
                            unitEqo = inEqo;
                        } else {
                            //Validate if the current booking is matching the edi values when it doesn't match with pre-advised unit
                            isValid = validateBooking(inEqo, ediIsoType, ediLineOpBiz, ediCategory, ediFreightKind, appointmentTransaction);
                            if (isValid) {
                                unitEqo = inEqo;
                            }
                        }
                        //find matching container
                        if (unitEqo != null && isTranshipITTExportRecivalFlow(appointmentTransaction)) {
                            unitEqo = findMatchinBookingFromExistingUnit(eqpuipmentIsoType, unitCategory, unitFreightKind, lineOperator);
                        }
                    } /*else {
						LOGGER.debug("Inside the process flow for pre-advised unit not associated with booking and EDI has no valid boking :: " + isTranshipITTExportRecivalFlow(appointmentTransaction))
						if (isTranshipITTExportRecivalFlow(appointmentTransaction)) {
							unitEqo = findMatchinBookingFromExistingUnit(eqpuipmentIsoType, unitCategory, unitFreightKind, lineOperator);
						}
					}*/
                }
                if (unitEqo != null) {
                    //inParms.put(EDI_ORDER_NBR_KEY, unitEqo.getEqboNbr());
                    inParms.put(EDI_ORDER_KEY, unitEqo);
                    appointmentTransaction.setOrderNbr(unitEqo.getEqboNbr());
                    lineOperator = unitEqo.getEqoLine();
                } else {
                    /*if (inEqo != null) {
                        inParms.put(ORDER_MISMATCH_EQOI, inEqo);
                    }*/
                    //inParms.put(EDI_ORDER_NBR_KEY, null);
                    inParms.put(EDI_ORDER_KEY, null);
                    //prean errors ORN003 (existing) and ORN008 (new) seem to mean the same, so commenting this item.
                    //validationsErrors.add("BookingNotFound");
                }
            }
            //whatever may be the booking, if there is an pre-advised unit, always use the details from Unit.
            updateAppointmentParams(appointmentTransaction, eqpuipmentIsoType, lineOperator, unitCategory, unitFreightKind);
        } else {


            String derEquipment;
            ScopedBizUnit derLineOperator;
            LOGGER.debug("Inside the flow for no preadvised unit ---");
            appointmentTransaction.getEdiFlexFields().setUnitFlexString02("YES");
            LOGGER.debug("Container information :: " + ctr + " pARAMS flag :: " + inParms.get(CAN_BE_RETIRED_PREAN));
            if (ctr != null) {
                if (inEqo != null) {
                    derLineOperator = inEqo.getEqoLine();
                } else if (ctr.getEqEquipType() != null) {
                    derLineOperator = ctr.getEquipmentOperator();
                }
                derEquipment = ctr.getEqEquipType().getEqtypId();

                if ((inEqo == null || !validateBooking(inEqo, derEquipment, derLineOperator, ediCategory, ediFreightKind, appointmentTransaction)) && isTranshipITTExportRecivalFlow(appointmentTransaction) && !isTranshipITTEmptyRecivalFlow(appointmentTransaction) && derEquipment != null) {
                    inEqo = findMatchinBookingFromExistingUnit(derEquipment, ediCategory, ediFreightKind, derLineOperator);
                }
                if (inEqo != null) {
                    updateAppointmentParams(appointmentTransaction, ctr.getEqEquipType().getEqtypId(), inEqo.getEqoLine(), ediCategory, ediFreightKind);
                    derLineOperator = inEqo.getEqoLine();
                } else {
                    updateAppointmentParams(appointmentTransaction, ctr.getEqEquipType().getEqtypId(), ctr.getEquipmentOperator(), ediCategory, ediFreightKind);
                    derLineOperator = ctr.getEquipmentOperator();
                }
            } else {
                if (derEquipment == null) {
                    derEquipment = ediIsoType;
                }
                LOGGER.debug("Container information :: DER Line Op before :: " + derLineOperator)
                if (derLineOperator == null) {
                    derLineOperator = inEqo != null ? inEqo.getEqoLine() : ediLineOpBiz;
                }
                LOGGER.debug("Container information :: DER Line Op after :: " + derLineOperator)

                /*if (inEqo == null && isTranshipITTExportRecivalFlow(appointmentTransaction) & !isTranshipITTEmptyRecivalFlow(appointmentTransaction)) {
                    inEqo = findMatchinBookingFromExistingUnit(ediIsoType, ediCategory, ediFreightKind, derLineOperator);
                }*/
                updateAppointmentParams(appointmentTransaction, ediIsoType, derLineOperator, ediCategory, ediFreightKind);
            }

            LOGGER.debug("Container information :: non preadvised flow Equipment Booking :: " + inEqo)
            if (inEqo != null && !isImportITTRecivalFlow(appointmentTransaction) && !isTranshipITTEmptyRecivalFlow(appointmentTransaction)) {
                boolean isValid = validateBooking(inEqo, derEquipment, derLineOperator, ediCategory, ediFreightKind, appointmentTransaction);
                LOGGER.debug("Container information :: non preadvised flow Equipment Booking valid ?? :: " + isValid)
                if (!isValid) {
                    //inParms.put(EDI_ORDER_NBR_KEY, null);
                    inParms.put(EDI_ORDER_KEY, null);
                    inParms.put(ORDER_MISMATCH_EQOI, inEqo);
                    //validationsErrors.add("BookingNotFound");
                    if (ediLineOpBiz == null || LineOperator.isUnknownLineOperator(ediLineOpBiz)) {
                        validationsErrors.add("LineNotFound");
                    }
                    if (ediIsoType.equalsIgnoreCase("UNKN")) {
                        validationsErrors.add("EquipmentNotFound");
                    }
                } else {
                    //inParms.put(EDI_ORDER_NBR_KEY, inEqo.getEqboNbr());
                    inParms.put(EDI_ORDER_KEY, inEqo);
                }
            } else {
                LOGGER.debug("Container information :: non preadvised flow Equipment Booking not available  :: " + inEqo)
                updateAppointmentParams(appointmentTransaction, ediIsoType, ediLineOpBiz, ediCategory, ediFreightKind);
                /*	if(inEqo == null){
                        validationsErrors.add("BookingNotFound");
                    }*/
                if (ediLineOpBiz == null || LineOperator.isUnknownLineOperator(ediLineOpBiz)) {
                    validationsErrors.add("LineNotFound");

                }
                if (ediIsoType.equalsIgnoreCase("UNKN")) {
                    validationsErrors.add("EquipmentNotFound");
                }
                /*if (!isImportITTRecivalFlow(appointmentTransaction) && !isTranshipITTEmptyRecivalFlow(appointmentTransaction)) {
                    validationsErrors.add("BookingNotFound");
                }*/
            }
        }

        //due to a limitation fetch the value from the untiFlexString05 mapped and set it to unitFlexString07 and nullify unitFlexString05.

        if (appointmentTransaction.getEdiFlexFields().getUnitFlexString05() != null) {
            if (isTranshipITTRecivalFlow(appointmentTransaction)) {
                appointmentTransaction.getEdiFlexFields().setUnitFlexString07(appointmentTransaction.getEdiFlexFields().getUnitFlexString05());
            }
            appointmentTransaction.getEdiFlexFields().setUnitFlexString05(null);
        }

        // Validation1: Set the Gross weight from the booking / equipment tare weight - Start
        // TODO: If the EQO is null then throw a BIZ violation
        if (inEqo != null) {
            for (EqBaseOrderItem eqBaseOrderItem : inEqo.getEqboOrderItems()) {
                EquipmentOrderItem eqOrderItem = (EquipmentOrderItem) HibernateApi.getInstance().downcast(eqBaseOrderItem, EquipmentOrderItem.class)
                if (eqOrderItem.getEqoiSampleEquipType().getEqtypId().equalsIgnoreCase(ediIsoType)
                        && eqOrderItem.getEqoiGrossWeight()) {
                    if (appointmentTransaction.getGrossWeight() == null) {
                        EdiWeight ediWeight = appointmentTransaction.addNewGrossWeight();
                        ediWeight.setWtUnit(WeightUnitEnum.KG.getKey());
                        ediWeight.setWtValue(String.valueOf(eqOrderItem.getEqoiGrossWeight()));
                    }
                    appointmentTransaction.getGrossWeight().setWtUnit(WeightUnitEnum.KG.getKey());
                    appointmentTransaction.getGrossWeight().setWtValue(String.valueOf(eqOrderItem.getEqoiGrossWeight()));
                }
            }
        } else {
            if (ctr != null && ctr.getEqTareWeightKg() != null) {
                if (appointmentTransaction.getGrossWeight() == null) {
                    EdiWeight ediWeight = appointmentTransaction.addNewGrossWeight();
                    ediWeight.setWtUnit(WeightUnitEnum.KG.getKey());
                    ediWeight.setWtValue(String.valueOf(ctr.getEqTareWeightKg()));
                } else {
                    appointmentTransaction.getGrossWeight().setWtUnit(WeightUnitEnum.KG.getKey());
                    appointmentTransaction.getGrossWeight().setWtValue(String.valueOf(ctr.getEqTareWeightKg()));
                }
            }/*else{
				BizViolation bv = BizViolation.create(ArgoPropertyKeys.EQUIPMENT_NOT_FOUND, null, containerNbr);
				inEdiPostingContext.addViolation(bv);
			}*/
        }
        // Validation1: Set the Gross weight from the booking / equipment tare weight - END
        LOGGER.debug("Validation errors :: " + validationsErrors.size());
        if (validationsErrors.size() > 0) {
            for (String validationError : validationsErrors) {
                LOGGER.debug("Validation error :: " + validationError)
            }
        }
        return validationsErrors;

    }

    /*private String deriveIsoTypeFromBooking(Booking inBooking, String inEdiIsoType){
        if(UNKN.equalsIgnoreCase(inEdiIsoType)){
            return UNKN;
        }
        for(EqBaseOrderItem eqBaseOrderItem : inBooking.getEqboOrderItems()){
            EquipmentOrderItem eqOrderItem = (EquipmentOrderItem) HibernateApi.getInstance().downcast(eqBaseOrderItem, EquipmentOrderItem.class)
            if(eqOrderItem.getEqoiSampleEquipType().getEqtypId(){

            }
        }


    }*/

    private EquipmentOrder findMatchinBookingFromExistingUnit(String inIsoType, UnitCategoryEnum inCategory, FreightKindEnum inFreightKind, ScopedBizUnit inLineOperator) {
        LOGGER.setLevel(Level.DEBUG);
        DomainQuery dq = QueryUtils.createDomainQuery("Unit").addDqPredicate(PredicateFactory.isNotNull(UnitField.UNIT_DEPARTURE_ORDER))
                .addDqPredicate(PredicateFactory.eq(UnitField.UNIT_VISIT_STATE, UnitVisitStateEnum.ACTIVE))
                .addDqPredicate(PredicateFactory.eq(UnitField.UNIT_EQTYPE_ID, inIsoType))
                .addDqPredicate(PredicateFactory.eq(UnitField.UNIT_CATEGORY, inCategory))
                .addDqPredicate(PredicateFactory.eq(UnitField.UNIT_FREIGHT_KIND, inFreightKind))
                .addDqPredicate(PredicateFactory.eq(UnitField.UNIT_CURRENT_UFV_TRANSIT_STATE, UfvTransitStateEnum.S20_INBOUND));
        //.addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("unitLineOperator"), inLineOperator));
        List<Unit> currentUnitList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
        if (currentUnitList != null && !currentUnitList.isEmpty()) {
            for (Unit unit : currentUnitList) {
                if (unit.getUnitDepartureOrderItem() != null && inLineOperator != null && inLineOperator.equals(unit.getUnitLineOperator())) {
                    EqBaseOrder eqBaseOrder = unit.getUnitDepartureOrderItem().getEqboiOrder();
                    if (eqBaseOrder != null) {
                        LOGGER.debug("Current unit selected for seding the booking number :: " + unit)
                        return HibernateApi.getInstance().downcast(eqBaseOrder, EquipmentOrder.class);
                    }
                }
            }
        }
        return null
    }

    private boolean validateBooking(EquipmentOrder inEqo, String inIsoType, ScopedBizUnit inLineOp, UnitCategoryEnum inCategoryEnum, FreightKindEnum inFrieghtKindEnum, AppointmentTransactionDocument.AppointmentTransaction inAppointment) {
        LOGGER.debug("validateBooking - BEGIN :: ISO Type :: " + inIsoType + " :: Line Op :: " + inLineOp + " :: Categor :: " + inCategoryEnum + " :: Fregiht Kind :: " + inFrieghtKindEnum);
        if (inEqo != null) {
            Booking booking = Booking.resolveBkgFromEqo(inEqo);
            LOGGER.debug("validateBooking - BEGIN :: Derived Booking from  :: " + inEqo + " Booking :: " + booking);
            if (inEqo != null && inIsoType != null && inLineOp != null && inLineOp.equals(inEqo.getEqoLine()) &&
                    inCategoryEnum != null && booking != null && inCategoryEnum.equals(booking.getEqoCategory()) && inFrieghtKindEnum != null && inFrieghtKindEnum.equals(booking.getEqoEqStatus())) {
                LOGGER.debug("validateBooking - BEGIN :: Checking for the booking order items  :: " + inEqo + " Booking :: " + booking);
                for (EqBaseOrderItem eqBaseOrderItem : inEqo.getEqboOrderItems()) {
                    EquipmentOrderItem eqOrderItem = EquipmentOrderItem.resolveEqoiFromEqboi(eqBaseOrderItem);
                    if (eqOrderItem != null) {
                        LOGGER.debug("validateBooking - BEGIN :: Checking for the booking order items  :: " + eqOrderItem.getEqoiSampleEquipType().getEqtypId() + " :: ISO Type :: " + inIsoType);
                        if (eqOrderItem.getEqoiSampleEquipType().getEqtypId().equals(inIsoType)) {
                            return true;
                        } else {
                            EquipType derEquipType = EquipType.findEquipType(inIsoType);
                            if (derEquipType != null && eqOrderItem.getEqoiSampleEquipType() != null &&
                                    eqOrderItem.getEqoiSampleEquipType().getEqtypArchetype().equals(derEquipType.getEqtypArchetype())) {
                                return true;
                            }
                            if (hasEqTypeEquivalentMatch(inAppointment.getAppointmentType(), inIsoType, inLineOp, eqOrderItem)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        LOGGER.debug("validateBooking - END ");
        return false;
    }

    private boolean hasEqTypeEquivalentMatch(String appType, String inIsoType, ScopedBizUnit inLineOp, EquipmentOrderItem eqOrderItem) {
        List eqGroups;
        if (DROPOFF.equalsIgnoreCase(appType)) {
            eqGroups = _ordersFinder.findSubstGroupByLineEqTypeRcv(inLineOp, EquipType.findEquipType(inIsoType));
        } else if (PICKUP.equalsIgnoreCase(appType)) {
            eqGroups = _ordersFinder.findSubstGroupByLineEqTypeDsp(inLineOp, EquipType.findEquipType(inIsoType));
        }
        if (eqGroups != null && !eqGroups.isEmpty()) {
            for (EqTypeGroup eqTypeGroup : eqGroups) {
                if (eqTypeGroup.containsSzTpHt(eqOrderItem.getEqoiEqSize(), eqOrderItem.getEqoiEqIsoGroup(), eqOrderItem.getEqoiEqHeight())) {
                    return true;
                }
            }
        }
        return false;
    }

    private void updateAppointmentParams(AppointmentTransaction appointmentTransaction, String isoType, ScopedBizUnit lineOp, UnitCategoryEnum categoryEnum, FreightKindEnum frieghtKindEnum) {
        appointmentTransaction.setContainerType(isoType);
        appointmentTransaction.setCategory(categoryEnum.getName());
        appointmentTransaction.setFreightKind(frieghtKindEnum.getName());

        if (lineOp != null) {
            if (lineOp.getBzuScac() != null) {
                appointmentTransaction.getEdiShippingLine().setShippingLineCode(lineOp.getBzuScac());
                appointmentTransaction.getEdiShippingLine().setShippingLineCodeAgency("SCAC");
            } else if (lineOp.getBzuBic() != null) {
                appointmentTransaction.getEdiShippingLine().setShippingLineCode(lineOp.getBzuBic());
                appointmentTransaction.getEdiShippingLine().setShippingLineCodeAgency("BIC");
            } else {
                appointmentTransaction.getEdiShippingLine().setShippingLineCode(lineOp.getBzuId());
            }
            appointmentTransaction.getEdiShippingLine().setShippingLineName(lineOp.getBzuName());
        }
        LOGGER.debug("updateAppointmentParams - END");

    }

    private void processTranTypeMismatch(EdiPostingContext inEdiPostingContext, GateAppointment originalPrean) {

        BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_TRANS_TYPE_MISMATCH"), (Throwable) null, null, null, null);
        inEdiPostingContext.addViolation(bv);

        try {

            GeneralReference emailGf = GeneralReference.findUniqueEntryById("PREANNOUNCEMENT", "EMAIL", "TRANS_TYPE_MISMATCH");
            if (emailGf != null) {

                String emailAddress = emailGf.getRefValue1();
                String fromEmailAddress = emailGf.getRefValue2();
                String subject = "Update COPINO does not match original COPINO tran type";
                String message = "Original Preannouncement EDI Tran Ref Nbr: " + originalPrean.getApptRefNbr();

                ESBClientHelper.sendEmailAttachments(ContextHelper.getThreadUserContext(), FrameworkMessageQueues.EMAIL_QUEUE, emailAddress, fromEmailAddress, subject, message, null);
            } else {
                log(Level.ERROR, "General Reference for PREANNOUNCEMENT/EMAIL/TRANS_TYPE_MISMATCH hasn't been set up");
            }

        } catch (Exception e) {
            log(Level.ERROR, e.getMessage());
        }
    }

    public void processAfterPost(XmlObject inXmlTransactionDocument, Map inParams, TruckVisitAppointment inOriginalPreanTvAppt, GateAppointment inPostedPrean,
                                 boolean inTruckPrean, Set<String> inValidationError) {

        AppointmentTransactionDocument.AppointmentTransaction apptTran;
        apptTran = getApptTran(inXmlTransactionDocument);
        String msgFunction = apptTran.getMsgFunction();

        if (inPostedPrean != null && !getMessageCollector().hasError()) {

            inPostedPrean.setFieldValue(EDI_TRANS_REF_NBR, apptTran.getInterchange().getInterchangeNumber());
            inPostedPrean.setFieldValue(PREAN_LANDSIDE_CARRIER_VISIT, (CarrierVisit) inParams.get(LANDSIDE_CARRIER_VISIT_KEY));


            if (MSG_FUNCTION_GET_STATUS_UPDATE.equals(msgFunction)) {
                inPostedPrean.setFieldValue(RESPONSE_MSG_TYPE, STATUS_UPDATE);
                inPostedPrean.setFieldValue(PREAN_COPINO_MSG_TYPE, "COPINO13");
            }
            LOGGER.debug("Current Values :: ")
            LOGGER.debug("Order number :: " + inParams.get(EDI_ORDER_NBR_KEY))
            LOGGER.debug("Order number Key :: " + inParams.get(EDI_ORDER_KEY))
            LOGGER.debug("Appt State :: " + inPostedPrean.getGapptState())
            LOGGER.debug("ORDER EQOI mismatch :: " + inParams.get(ORDER_MISMATCH_EQOI));
            LOGGER.debug("Validation ID :: " + inParams.get(VALIDATION_RUN_ID_KEY));

            /* if(inParams.get(EDI_ORDER_NBR_KEY) != null){
                 inPostedPrean.setFieldValue(PREAN_EQO_NBR, (String) inParams.get(EDI_ORDER_NBR_KEY));
             }else if(inParams.get(ORDER_MISMATCH_EQOI) != null){
                 EquipmentOrder eqOrder = (EquipmentOrder) inParams.get(ORDER_MISMATCH_EQOI)
                 inPostedPrean.setFieldValue(PREAN_EQO_NBR, eqOrder.getEqboNbr());
             }*/

            //run the validation
            if (AppointmentStateEnum.CREATED.equals(inPostedPrean.getGapptState())) {
                //log("PIN:" + inPostedPrean.getGapptImportReleaseNbr());
                //log("Order:" + inParams.get(EDI_ORDER_KEY));

                inPostedPrean.setPinNumber(inPostedPrean.getGapptImportReleaseNbr());

                if (inParams.get(EDI_ORDER_NBR_KEY) != null && inParams.get(EDI_ORDER_KEY) != null) {
                    inPostedPrean.setGapptOrderNbr((String) inParams.get(EDI_ORDER_NBR_KEY));
                    inPostedPrean.setGapptOrder((EquipmentOrder) inParams.get(EDI_ORDER_KEY));
                }
                if (inParams.get(EDI_ORDER_NBR_KEY) != null) {
                    inPostedPrean.setFieldValue(PREAN_EQO_NBR, (String) inParams.get(EDI_ORDER_NBR_KEY));
                }

                try {
                    inPostedPrean.submit(GateClientTypeEnum.EDI, null);
                } catch (BizViolation bv) {
                    getMessageCollector().appendMessage(bv);
                }
                log("Order after:" + inPostedPrean.getGapptOrder());
            }
            if (!getMessageCollector().hasError()) {

                if (inTruckPrean) {
                    TruckVisitAppointment ediTvAppt = (TruckVisitAppointment) inParams.get(TRUCK_VISIT_APPOINTMENT_KEY);
                    Date origRequestedDate = (Date) inParams.get(EDI_ORIGINAL_REQUESTED_DATETIME_KEY);

                    if (MSG_FUNCTION_CREATE.equals(msgFunction)) {

                        inPostedPrean.setFieldValue(EDI_ORIGINAL_REQUEST_DATETIME, origRequestedDate);
                        associateWithTvAppt(inPostedPrean, ediTvAppt);
                    } else if (MSG_FUNCTION_UPDATE.equals(msgFunction)) {

                        if ("YES".equals(inParams.get("PREAN_REQUESTED_DATE_UPDATE"))) {

                            inPostedPrean.setFieldValue(EDI_ORIGINAL_REQUEST_DATETIME, origRequestedDate);
                            //@todo Null check
                            try {
                                HibernateApi.getInstance().flush();

                            } catch (DataIntegrityViolationException e) {
                                registerError("createNewAndDeleteOldAppointment-1 : cought the exception");
                                throw e;
                            }

                            TruckVisitAppointment newTvappt = associateWithTvAppt(inPostedPrean, null);

                            Set<GateAppointment> gateApptSet = inOriginalPreanTvAppt.getTvapptGateApptSet();

                            try {
                                HibernateApi.getInstance().flush();

                            } catch (DataIntegrityViolationException e) {
                                registerError("createNewAndDeleteOldAppointment0 : cought the exception");
                                throw e;
                            }

                            for (GateAppointment gappt : gateApptSet) {

                                GateAppointment newAppt = createNewAndDeleteOldAppointment(ContextHelper.getThreadEdiPostingContext(), gappt, inPostedPrean.getGapptRequestedDate());

                                if (newAppt != null) {
                                    try {
                                        newAppt.setGapptOrderNbr((newAppt.getFieldString(_panFields.PREAN_EQO_NBR)));
                                        newAppt.setPinNumber((newAppt.getFieldString(_panFields.PREAN_PIN)));

                                        newAppt.submit(GateClientTypeEnum.CLERK, null);
                                        newAppt.setFieldValue(EDI_ORIGINAL_REQUEST_DATETIME, origRequestedDate);
                                        associateWithTvAppt(newAppt, newTvappt);

                                    } catch (BizViolation bv) {
                                        getMessageCollector().appendMessage(bv);
                                    }
                                }
                            }

                            HibernateApi.getInstance().delete(inOriginalPreanTvAppt);
                            HibernateApi.getInstance().flush();
                            newTvappt.setTvapptReferenceNbr(inOriginalPreanTvAppt.getTvapptReferenceNbr());

                        } else if ("YES".equals(inParams.get("TAR_CHANGED"))) {

                            associateWithTvAppt(inPostedPrean, ediTvAppt);

                            TruckVisitAppointment origTvAppt = (TruckVisitAppointment) inParams.get("ORIG_TAR");

                            if (origTvAppt != null) {

                                Set<GateAppointment> gateApptSet = origTvAppt.getTvapptGateApptSet();

                                if (gateApptSet == null || gateApptSet.isEmpty()) {

                                    HibernateApi.getInstance().delete(origTvAppt);
                                }

                            }

                        }

                    }

                }

            }
            RoutingPoint routingPoint;
            Unit preanUnit = inPostedPrean.getGapptUnit();
            if (DROPOFF.equalsIgnoreCase(apptTran.getAppointmentType()) && apptTran.getExportRouting() != null && apptTran.getExportRouting().getLoadPort() != null) {
                routingPoint = extractRoutingPoint(ContextHelper.getThreadEdiPostingContext(), apptTran.getExportRouting().getLoadPort(), UnitField.UNIT_RTG_POL);
            } else if (PICKUP.equalsIgnoreCase(apptTran.getAppointmentType()) && apptTran.getExportRouting() != null && apptTran.getExportRouting().getDischargePort1() != null) {
                routingPoint = extractRoutingPoint(ContextHelper.getThreadEdiPostingContext(), apptTran.getExportRouting().getDischargePort1(), UnitField.UNIT_RTG_POD1);
                if (preanUnit == null && inPostedPrean.getGapptCtrId() != null) {
                    Equipment equipment = Equipment.findEquipment(inPostedPrean.getGapptCtrId());
                    if (equipment != null) {
                        preanUnit = _unitFinder.findAttachedUnit(ContextHelper.getThreadComplex(), equipment);
                    }
                }
            }
            LOGGER.debug("Current Prean Unit selected :: " + preanUnit)
            if (preanUnit != null) {

                if (routingPoint != null) {
                    if (DROPOFF.equalsIgnoreCase(apptTran.getAppointmentType()) && apptTran.getExportRouting() != null && apptTran.getExportRouting().getLoadPort() != null) {
                        preanUnit.getUnitRouting().setRtgPOL(routingPoint);
                    } else if (PICKUP.equalsIgnoreCase(apptTran.getAppointmentType()) && apptTran.getExportRouting() != null && apptTran.getExportRouting().getDischargePort1() != null) {

                        preanUnit.getUnitRouting().setRtgPOD1(routingPoint);
                    }
                }
                LOGGER.debug("Before set the booking to unit EQ NBR :: " + inPostedPrean.getField(MetafieldIdFactory.valueOf("gapptOrder.eqboNbr")));
                LOGGER.debug("Before set the booking to unit Base Order :: " + inPostedPrean.getGapptOrder());
                if (inPostedPrean.getGapptOrder() != null) {
                    for (EqBaseOrderItem eqBaseOrderItem : inPostedPrean.getGapptOrder().getEqboOrderItems()) {
                        LOGGER.debug("Equipment Order Item ISO :: " + EquipmentOrderItem.resolveEqoiFromEqboi(eqBaseOrderItem).getEqoiSampleEquipType() + "VS Prean :: " + inPostedPrean.getGapptCtrEquipType())

                        if (EquipmentOrderItem.resolveEqoiFromEqboi(eqBaseOrderItem).getEqoiSampleEquipType().equals(inPostedPrean.getGapptCtrEquipType())
                                || hasEqTypeEquivalentMatch(apptTran.getAppointmentType(), preanUnit.getUnitEquipment().getEqEquipType().getEqtypId(), preanUnit.getUnitLineOperator(), EquipmentOrderItem.resolveEqoiFromEqboi(eqBaseOrderItem))) {
                            LOGGER.debug("Inside setting the preadvised unit :: ");
                            //preanUnit.reserveForOrder(eqBaseOrderItem, preanUnit.getUnitPrimaryUe(),ContextHelper.getThreadFacility());
                            preanUnit.getUnitPrimaryUe().setUeDepartureOrderItem(eqBaseOrderItem);
                            preanUnit.getUnitPrimaryUe().setUeIsReserved(Boolean.TRUE);

                            // Gopinath Kannappan - WeServe Tech - 07 Sep 2017 -- Added a code fix to update OOG information to UNIT from BOOKING.
                            EquipmentOrderItem eqoItem = EquipmentOrderItem.resolveEqoiFromEqboi(eqBaseOrderItem);
                            if (eqoItem != null && eqoItem.getEqoiIsOog()) {
                                preanUnit.updateOog(eqoItem.getEqoiOogBackCm(), eqoItem.getEqoiOogFrontCm(), eqoItem.getEqoiOogLeftCm(), eqoItem.getEqoiOogRightCm(), eqoItem.getEqoiOogTopCm());
                            }


                        }
                    }
                }

                log("Current value for the prean Unit UFV flex String 07 RTO Category :: " + preanUnit.getUnitActiveUfvNowActive().getUfvFlexString07())
                if (preanUnit.getUnitActiveUfvNowActive().getUfvFlexString07() != null) {
                    apptTran.getEdiFlexFields().setUfvFlexString07(preanUnit.getUnitActiveUfvNowActive().getUfvFlexString07());
                }
                HibernateApi.getInstance().save(preanUnit);
                HibernateApi.getInstance().flush();
            }

            //Check for the validation errors and set the NOK flag and send_msg flag based on the validation errors. Append / add the validation errors.
            String currentPreanStatus = (String) inPostedPrean.getFieldValue(_panFields.PREAN_STATUS);
            String newPreanStatus = currentPreanStatus;
            boolean isStatusChanged = false;
            boolean differentErrorsRecorded = false;
            if (inValidationError != null && !inValidationError.isEmpty()) {
                if (currentPreanStatus != null && !currentPreanStatus.equalsIgnoreCase("NOK")) {
                    isStatusChanged = true;
                } else {
                    isStatusChanged = false;
                }
                inPostedPrean.setFieldValue(PREAN_STATUS, "NOK");
                newPreanStatus = "NOK";
                //differentErrorsRecorded = recordPreanErrors(inPostedPrean, inValidationError)
            } else {
                inPostedPrean.setFieldValue(_panFields.SEND_MSG, "YES");
            }
            if (isStatusChanged || (newPreanStatus.equals("NOK") /*&& differentErrorsRecorded*/)) {
                inPostedPrean.setFieldValue(_panFields.SEND_MSG, "YES");
            }

            /*if (inParams.get(ORDER_MISMATCH_EQOI) != null) {
                EquipmentOrder eqOrder = (EquipmentOrder) inParams.get(ORDER_MISMATCH_EQOI);
                inPostedPrean.setGapptOrderNbr(eqOrder.getEqboNbr());
                inPostedPrean.setGapptOrder(eqOrder);
            }*/

            inPostedPrean.setFieldValue(EDI_TRAN_GKEY, inParams.get(EdiConsts.TRANSACTION_GKEY));

            //Praveen Babu - WF#892222 Logic of setting the validation run id is not required.
            /*if (inParams.get(VALIDATION_RUN_ID_KEY) != null && inPostedPrean.getFieldValue(PREAN_VALIDATION_RUN_ID) != null) {
                Long paramValidationID = inParams.get(VALIDATION_RUN_ID_KEY);
                Long preanValidationID = inPostedPrean.getFieldValue(PREAN_VALIDATION_RUN_ID);
                if (paramValidationID.compareTo(preanValidationID) != 0) {
                    LOGGER.debug("Updating the validation run id");
                    inPostedPrean.setFieldValue(PREAN_VALIDATION_RUN_ID, paramValidationID);
                }
            }*/

            EdiSession ediSession = (EdiSession) HibernateApi.getInstance().load(EdiSession.class, (Long) inParams.get(EdiConsts.SESSION_GKEY));
            inPostedPrean.setFieldValue(EDI_PARTNER_NAME, ediSession.getEdisessTradingPartner().getNaturalKey());
        }
    }

    private boolean recordPreanErrors(GateAppointment inGappt, Set<String> inErrorCodeSet) {
        Long validationRunId = inGappt.getFieldValue(PREAN_VALIDATION_RUN_ID);
        if (validationRunId == null) {
            validationRunId = new Date().getTime();
        }

        String tranType;
        TranSubTypeEnum currentTranSubType = TransactionTypeUtil.getTranSubTypeEnum(inGappt.getGapptTranType());
        if (currentTranSubType != null) {
            tranType = TransactionTypeUtil.isReceival(currentTranSubType) ? "RECEIVAL" : "DELIVERY";
        }
        if (tranType == null) {
            tranType = "RECEIVAL";
        }
        for (String inErrorCode : inErrorCodeSet) {
            GeneralReference errRef = GeneralReference.findUniqueEntryById("PREAN_ERR_CONDITION", tranType, inErrorCode);
            _preanErrorUtil.recordError(inGappt, errRef.getRefValue1(), errRef.getRefValue2(), validationRunId);
        }

        inGappt.setFieldValue(PREAN_VALIDATION_RUN_ID, validationRunId);
        String currErrorAperakCodes = _preanErrorUtil.getErrRefAperakCodesByPreanAndValidationRunId(inGappt.getGapptGkey(), validationRunId);
        return _preanErrorUtil.diffErrorsRecorded(inGappt.getGapptGkey(), validationRunId, currErrorAperakCodes);
    }

    private AppointmentTransactionDocument.AppointmentTransaction getApptTran(XmlObject inXmlTransactionDocument) {

        if (AppointmentTransactionsDocument.class.isAssignableFrom(inXmlTransactionDocument.getClass())) {
            AppointmentTransactionsDocument appointmentDoc = (AppointmentTransactionsDocument) inXmlTransactionDocument;
            return appointmentDoc.getAppointmentTransactions().getAppointmentTransactionArray(0);
        }
        return null;
    }


    private ScopedBizUnit getLine(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, EdiPostingContext inEdiPostingContext, Gate inGate) {
        ScopedBizUnit line = null;
        String lineCode = inEdiAppointment.getEdiShippingLine().getShippingLineCode();
        String lineCodeAgency = inEdiAppointment.getEdiShippingLine().getShippingLineCodeAgency();
        if (lineCode != null) {
            line = ScopedBizUnit.resolveScopedBizUnit(lineCode, lineCodeAgency, BizRoleEnum.LINEOP);
            if (line == null && !RAIL_ITT_GATE.equalsIgnoreCase(inGate.getGateId())) {
                BizViolation bv = BizViolation.create(ArgoPropertyKeys.INVALID_OPERATOR_ID, null, lineCode);
                inEdiPostingContext.addViolation(bv);
            }
        }
        return line;
    }

    private UnitCategoryEnum deriveCategory(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
                                            EquipmentOrder inEqo, Facility inFcy) throws BizViolation {

        String basicType = inEdiAppointment.getAppointmentType();

        UnitCategoryEnum category;
        FreightKindEnum freightKind = FreightKindEnum.getEnum(inEdiAppointment.getFreightKind());


        if (PICKUP.equals(basicType)) {
            if (FreightKindEnum.MTY.equals(freightKind)) {
                category = UnitCategoryEnum.STORAGE;
            } else {
                //@todo - revisit
                Unit unit = findActiveOrAdvisedUnitForDelivery(inEdiAppointment, inFcy);
                if (unit != null) {
                    category = unit.getUnitCategory();
                } else {
                    category = UnitCategoryEnum.IMPORT;
                }

            }

        }
        //Dropoff
        else {
            if (FreightKindEnum.FCL.equals(freightKind)) {

                category = UnitCategoryEnum.EXPORT;
                Container ctr = Container.findContainer(inEdiAppointment.getContainerId());

                if (ctr != null) {

                    Unit unit = _unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), ctr);

                    if (unit != null) {

                        if (UnitCategoryEnum.IMPORT.equals(unit.getUnitCategory()) && DrayStatusEnum.DRAYIN.equals(unit.getUnitDrayStatus())) {
                            category = UnitCategoryEnum.IMPORT;
                        }

                    }
                }

            } else {
                if (inEqo == null) {
                    category = UnitCategoryEnum.STORAGE;
                } else {
                    if (inEqo instanceof Booking || inEqo instanceof RailOrder) {
                        category = UnitCategoryEnum.EXPORT;
                    } else {
                        category = UnitCategoryEnum.STORAGE;
                    }

                }

            }

        }

        return category;
    }


    private GateAppointment findPrevMatchingPrean(String ediEqoNbr, String inCtrId, EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {

        boolean matchFound = false;

        TruckerFriendlyTranSubTypeEnum apptType = determineAppointmentType(inEdiPostingContext, inEdiAppointment);
        TruckerFriendlyTranSubTypeEnum[] tranTypes = [apptType];

        GateAppointment prevPrean = _apptFndr.findAppointmentByContainerId(inCtrId, tranTypes, AppointmentStateEnum.CREATED);

        //If found, check if order/pin is matching
        if (prevPrean != null) {
            if (STATUS_UPDATE.equals(prevPrean.getFieldString(RESPONSE_MSG_TYPE))) {

                matchFound = true;
            } else {
                if (ediEqoNbr != null) {

                    if (ediEqoNbr.equals(prevPrean.getFieldString(PREAN_EQO_NBR))) {
                        matchFound = true;
                    }
                } else {
                    String ediReleaseNbr = inEdiAppointment.getReleaseNbr();
                    if (ediReleaseNbr != null) {
                        String preanImpReleaseNbr = prevPrean.getGapptImportReleaseNbr();
                        if (ediReleaseNbr.equals(preanImpReleaseNbr)) {
                            matchFound = true;
                        }
                    }
                }

            }

        }

        if (!matchFound) {
            prevPrean = null;
        }

        return prevPrean;
    }

    private boolean sameOperator(GateAppointment inPrevPrean, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, CarrierVisit inLandsideCarrierVisit) {

        boolean result = false;

        ScopedBizUnit prevPreanOperator = null;
        CarrierVisit cv = null;
        ScopedBizUnit newPreanOperator = null;

        if (BARGE_GATE.equals(inPrevPrean.getGapptGate().getGateId()) || RAIL_GATE.equals(inPrevPrean.getGapptGate().getGateId())) {
            cv = (CarrierVisit) inPrevPrean.getField(PREAN_LANDSIDE_CARRIER_VISIT);
            prevPreanOperator = cv.getCarrierOperator();

            if (inLandsideCarrierVisit != null) {

                newPreanOperator = inLandsideCarrierVisit.getCarrierOperator();
            }

        } else {
            prevPreanOperator = inPrevPrean.getGapptTruckingCompany();

            if (inEdiAppointment.getEdiTruckingCompany() != null) {
                String truckCompId = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyId();

                if (truckCompId != null) {
                    newPreanOperator = TruckingCompany.findOrCreateTruckingCompany(truckCompId);
                }
            }
        }

        if (prevPreanOperator != null && newPreanOperator != null) {
            result = prevPreanOperator.getBzuGkey() == newPreanOperator.getBzuGkey();
        }

        return result;
    }

    private ScopedBizUnit findBargeOperator(EdiPostingContext inEdiPostingContext, ShippingLine inEdiVesselVisitLine) {

        ScopedBizUnit line = null;

        String lineCode = inEdiVesselVisitLine.getShippingLineCode();
        String lineCodeAgency = inEdiVesselVisitLine.getShippingLineCodeAgency();
        if (lineCode != null) {
            line = ScopedBizUnit.resolveScopedBizUnit(lineCode, lineCodeAgency, BizRoleEnum.LINEOP);
            if (line == null) {
                BizViolation bv = BizViolation.create(ArgoPropertyKeys.INVALID_OPERATOR_ID, null, lineCode);
                inEdiPostingContext.addViolation(bv);

            }

        }
        return line;
    }

    private ScopedBizUnit findTrainOperator(EdiPostingContext inEdiPostingContext, RailRoad inEdiRailRoad) {

        ScopedBizUnit railRoad = null;

        String railRoadCode = inEdiRailRoad.getRailRoadCode();
        String railRoadAgency = inEdiRailRoad.getRailRoadCodeAgency();
        if (railRoadCode != null) {
            railRoad = ScopedBizUnit.resolveScopedBizUnit(railRoadCode, railRoadAgency, BizRoleEnum.RAILROAD);
            if (railRoad == null) {
                BizViolation bv = BizViolation.create(ArgoPropertyKeys.INVALID_OPERATOR_ID, null, railRoadCode);
                inEdiPostingContext.addViolation(bv);

            }

        }
        return railRoad;
    }

    private CarrierVisit findBargeVisit(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {

        EdiVesselVisit ediBargeVisit = null;
        CarrierVisit bargeVisit = null;

        if (DROPOFF.equals(inEdiAppointment.getAppointmentType())) {
            ediBargeVisit = inEdiAppointment.getEdiInboundVesselVisit();
        } else {
            ediBargeVisit = inEdiAppointment.getEdiOutboundVesselVisit();
        }
        if (ediBargeVisit != null) {
            bargeVisit = findBargeVisit(inEdiPostingContext, ediBargeVisit, inEdiAppointment.getLoadFacility());

            if (bargeVisit == null) {
                //"Barge visit {0} not found"
                Object[] parms = ediBargeVisit.getVisitId();
                BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_BARGE_VISIT_NOT_FOUND"), (Throwable) null, null, null, parms);
                inEdiPostingContext.addViolation(bv);
            }

        } else {
            //"Barge visit data missing in copino"
            BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_BARGE_VISIT_MISSING"), (Throwable) null, null, null, null);
            inEdiPostingContext.addViolation(bv);
        }
        return bargeVisit;
    }

    private CarrierVisit findBargeVisit(EdiPostingContext inEdiPostingContext, EdiVesselVisit inBargeVisit, EdiFacility inEdiFacility) {

        if (inBargeVisit == null) {
            return null;
        }

        RoutingPoint pol = extractRoutingPoint(inEdiPostingContext, inBargeVisit.getLoadPort(), OrdersField.EQO_POL);

        CarrierVisit cv = null;
        try {
            ScopedBizUnit line = findBargeOperator(inEdiPostingContext, inBargeVisit.getShippingLine());
            if (line != null) {
                ArgoEdiFacade argoEdiFacade = (ArgoEdiFacade) Roastery.getBean(ArgoEdiFacade.BEAN_ID);
                cv = argoEdiFacade.findVesselVisit(inEdiPostingContext, inEdiFacility, inBargeVisit, line, pol, false);
            }

        } catch (BizViolation bv) {
            inEdiPostingContext.addViolation(bv);
        }
        return cv;
    }


    private TimeSlotBean getExactTimeSlot(Map<Date, TimeSlotBean> inSlots, Date inRequestedDate) {
        Collection<TimeSlotBean> slotbeans = inSlots.values();
        for (TimeSlotBean bean : slotbeans) {
            if (inRequestedDate.equals(bean.getStartDate()) || inRequestedDate.equals(bean.getEndDate()) ||
                    (inRequestedDate.after(bean.getStartDate()) && inRequestedDate.before(bean.getEndDate()))) {
                return bean;
            }
        }
        return null;
    }

    private TruckVisitAppointment findActiveTruckVisitAppointment(EdiPostingContext inEdiPostingContext, String inTvApptRefNbr) {

        HibernateApi.getInstance().setEntityScoping(false);

        DomainQuery dq = QueryUtils.createDomainQuery(RoadApptsEntity.TRUCK_VISIT_APPOINTMENT)
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.TVAPPT_REFERENCE_NBR, inTvApptRefNbr))
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.TVAPPT_STATE, AppointmentStateEnum.CREATED));

        TruckVisitAppointment tvAppt = (TruckVisitAppointment) Roastery.getHibernateApi().getUniqueEntityByDomainQuery(dq);

        HibernateApi.getInstance().setEntityScoping(true);

        if (tvAppt == null) {
            //"Active TAR {0} not found"
            Object[] parms = [inTvApptRefNbr];
            BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_ACTIVE_TAR_NOT_FOUND"), (Throwable) null, null, null, parms);
            inEdiPostingContext.addViolation(bv);
        }

        return tvAppt;

    }

    private void setEdiAppointmentDateTimeToEta(Gate inGate, CarrierVisit inLandsideCarrierVisit, EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {

        Date landsideCvEta = getLandsideCvEta(inGate, inLandsideCarrierVisit, inEdiPostingContext);

        if (landsideCvEta != null) {
            Calendar landsideCvEtaCal = Calendar.getInstance(inEdiPostingContext.getTimeZone());
            landsideCvEtaCal.setTime(ArgoUtils.convertDateToLocalDateTime(landsideCvEta, inEdiPostingContext.getTimeZone()));

            inEdiAppointment.setAppointmentDate(landsideCvEtaCal);
            inEdiAppointment.setAppointmentTime(landsideCvEtaCal);

        }
    }

    private Date getLandsideCvEta(Gate inGate, CarrierVisit inLandsideCarrierVisit, EdiPostingContext inEdiPostingContext) {

        Date landsideCvEta = null;

        if (inLandsideCarrierVisit != null) {

            landsideCvEta = inLandsideCarrierVisit.getCvCvd().getCvdETA();

            if (landsideCvEta == null) {
                BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_BARGE_TRAIN_ETA_NOT_SET"), (Throwable) null, null, null, null);
                inEdiPostingContext.addViolation(bv);

            } else {

                Date today = new Date();
                if (landsideCvEta.after(today)) {

                    return landsideCvEta;

                } else {
                    //Barge/Train has already arrived or ETA is wrong (set to before today)
                    BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_BARGE_TRAIN_ETA_IS_WRONG"), (Throwable) null, null, null, null);
                    inEdiPostingContext.addViolation(bv);

                }

            }

            return landsideCvEta;
        }

        return null;
    }


    private void changeRequestedTimeIfNeeded(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
                                             Date InPreanRequestDate, Gate inGate, TruckerFriendlyTranSubTypeEnum inApptType, Map inParms, Boolean inNeedToTryToFindNewTime) throws BizViolation {

        Calendar originalEdiApptTime = inEdiAppointment.getAppointmentTime();

        Date loopUntil = null;


        if (originalEdiApptTime != null) {


            Date ediRequestDateTime = mergeDateTime(inEdiPostingContext, inEdiAppointment);


            if (InPreanRequestDate != null && ediRequestDateTime != InPreanRequestDate) {
                if (DateUtil.isSameDay(ediRequestDateTime, InPreanRequestDate, ContextHelper.getThreadUserTimezone())) {
                    if (InPreanRequestDate.after(ediRequestDateTime)) {
                        loopUntil = InPreanRequestDate;
                    }
                }
            }

            HibernateApi.getInstance().setEntityScoping(false);
            Map<Date, TimeSlotBean> tvApptSlots = _appMgr.getOpenTimeSlotsForDate(inGate.getGateGkey(), null, ediRequestDateTime, false, null, null);
            HibernateApi.getInstance().setEntityScoping(true);


            if (tvApptSlots != null && !tvApptSlots.isEmpty()) {

                Map<Date, TimeSlotBean> gapptSlots = _appMgr.getOpenTimeSlotsForDate(inGate.getGateGkey(), inApptType.getInternalTranSubTypeEnum(), ediRequestDateTime, false, null, null);

                if (gapptSlots != null && !gapptSlots.isEmpty()) {

                    TimeSlotBean tvApptSlotBean = getExactTimeSlot(tvApptSlots, ediRequestDateTime);

                    if (tvApptSlotBean != null) {
                        TimeSlotBean gapptSlotBean = getExactTimeSlot(gapptSlots, ediRequestDateTime);

                        if (gapptSlotBean != null) {
                            //Found both gate appt and tv appt matching available slots, no need to change the requested time
                            return;
                        }
                    }

                    if (inNeedToTryToFindNewTime) {

                        Date newRequestedDateTime = findNewRequestedDateTime(tvApptSlots, gapptSlots, ediRequestDateTime, loopUntil);

                        if (newRequestedDateTime != null) {

                            setNewEdiApptTime(newRequestedDateTime, originalEdiApptTime, inEdiAppointment);
                            inParms.put(EDI_ORIGINAL_REQUESTED_DATETIME_KEY, ediRequestDateTime);

                        }

                    }
                }

            }
        }
    }

    private Date findNewRequestedDateTime(Map<Date, TimeSlotBean> tvApptSlots, Map<Date, TimeSlotBean> gapptSlots, Date inRequestedDateTime,
                                          Date inLoopUntil
    ) {

        Date newRequestedDateTime = null;

        Collection<Date> slotbeanDates = tvApptSlots.keySet();
        for (Date slotDate : slotbeanDates) {
            if (inLoopUntil == null || inLoopUntil.after(slotDate)) {
                if (slotDate.after(inRequestedDateTime)) {
                    newRequestedDateTime = slotDate;
                    TimeSlotBean gapptTimeSlot = getExactTimeSlot(gapptSlots, newRequestedDateTime);
                    if (gapptTimeSlot != null) {
                        break;
                    }
                }
            }
        }
        return newRequestedDateTime;
    }

    private void setNewEdiApptTime(Date newRequestedDateTime, Calendar inEdiApptTime, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {

        inEdiApptTime.set(Calendar.HOUR_OF_DAY, newRequestedDateTime.getHours());
        inEdiApptTime.set(Calendar.MINUTE, newRequestedDateTime.getMinutes());

        inEdiAppointment.setAppointmentTime(inEdiApptTime);

        PropertyKey CUSTOM_COPINO_REQUESTED_APPOINTMENT_TIME_CHANGED = PropertyKeyFactory.valueOf("CUSTOM_COPINO_REQUESTED_APPOINTMENT_TIME_CHANGED");
        getMessageCollector().appendMessage(MessageLevel.WARNING, CUSTOM_COPINO_REQUESTED_APPOINTMENT_TIME_CHANGED, "Requested appointment time has been changed", null);

    }


    private TruckVisitAppointment associateWithTvAppt(GateAppointment inGateAppt, TruckVisitAppointment inTvAppt) {

        TruckVisitAppointment newTvappt = inTvAppt;

        if (inGateAppt.getGapptTruckVisitAppointment() == null) {
            try {
                //Create tv appt if needed
                if (inTvAppt == null) {

                    newTvappt = createTvAppt(inGateAppt);

                    //set a dummy nbr here, so that ArgoSequenceProvider.getNextSeqValue is not invoked (which would cause a start of a new transaction and saving
                    //of the associate prean to the database w/o possiblity of a rollback on error
                    //that nunber is then set to random 7 digit nbr in TruckVisitAppointment interceptor
                    newTvappt.setTvapptNbr(getNextApptNbr());

                    HibernateApi.getInstance().save(newTvappt);
                    //HibernateApi.getInstance().flush();
                }
                GateAppointment[] gappts = [inGateAppt];
                newTvappt.associateGateAppointments(gappts, GateClientTypeEnum.EDI, null);

            } catch (BizViolation bv) {
                throw BizFailure.create(bv.getMessage());
            }

        }
        return newTvappt;
    }

    private TruckVisitAppointment createTvAppt(GateAppointment inGateAppt) throws BizViolation {
        Date requestedDate = inGateAppt.getGapptRequestedDate();
        TruckVisitAppointment tvAppt = null;
        //CSDV-1657
        //AppointmentTimeSlot tvApptTimeSlot = _appMgr.getAppointmentTimeSlot(inGateAppt.getGapptGate().getPrimaryKey(), null, requestedDate, null, true);
        AppointmentTimeSlot tvApptTimeSlot = getAppointmentTimeSlot(inGateAppt.getGapptGate().getPrimaryKey(), null, requestedDate, null, true, null);

        try {
            if (tvApptTimeSlot != null) {
                tvAppt = TruckVisitAppointment.createTruckVisitAppointment(inGateAppt.getGapptGate(),
                        requestedDate,
                        tvApptTimeSlot);

                if (tvAppt != null) {
                    tvAppt.setTruckingCompany(inGateAppt.getGapptTruckingCompany());
                }
            }
        } catch (ConstraintViolationException conEx) {
            throw BizFailure.create("conEx.getConstraintName(): " + conEx.getConstraintName());
        }

        return tvAppt;
    }

    private CarrierVisit determineLandsideVisit(Gate inGate, EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {

        String msgFunction = inEdiAppointment.getMsgFunction();
        CarrierVisit landSideCarrierVisit = null;

        if (!MSG_FUNCTION_GET_STATUS_UPDATE.equals(msgFunction)) {

            if (BARGE_GATE.equals(inGate.getGateId())) {
                landSideCarrierVisit = findBargeVisit(inEdiPostingContext, inEdiAppointment);

            } else if (RAIL_GATE.equals(inGate.getGateId())) {
                landSideCarrierVisit = findTrainVisit(inEdiPostingContext, inEdiAppointment, inGate.getGateFacility());

            }

            String gateId = inGate.getGateId();
            if ((BARGE_GATE.equals(gateId) || RAIL_GATE.equals(gateId)) && landSideCarrierVisit == null) {
                BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_LANDSIDE_CARRIER_VISIT_NOT_FOUND"), (Throwable) null, null, null, null);
                inEdiPostingContext.addViolation(bv);
            }

        }

        return landSideCarrierVisit;
    }

    private CarrierVisit findTrainVisit(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
                                        Facility inFacility) {

        EdiTrainVisit ediTrainVisit = inEdiAppointment.getEdiTrainVisit();
        CarrierVisit trainVisit = null;
        if (ediTrainVisit != null) {
            String trainId = ediTrainVisit.getTrainId();
            RailRoad trainOperator = ediTrainVisit.getTrainOperator();
            //ScopedBizUnit railRoad = findTrainOperator(inEdiPostingContext,trainOperator);
            //if (trainOperator != null) {
            if (trainId != null) {
                trainVisit = CarrierVisit.findTrainVisit(ContextHelper.getThreadComplex(), inFacility, trainId);
                if (trainVisit == null) {
                    BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_TRAIN_VISIT_NOT_FOUND"), (Throwable) null, null, null, null);
                    inEdiPostingContext.addViolation(bv);
                } else {
                    TrainVisitDetails tvd = TrainVisitDetails.resolveTvdFromCv(trainVisit);
                    String basicType = inEdiAppointment.getAppointmentType();
                    if ((TrainDirectionEnum.INBOUND.equals(tvd.getRvdtlsdDirection()) && PICKUP.equals(basicType)) ||
                            (TrainDirectionEnum.OUTBOUND.equals(tvd.getRvdtlsdDirection()) && DROPOFF.equals(basicType))) {

                        BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_TRAIN_VISIT_NOT_FOUND"), (Throwable) null, null, null, null);
                        inEdiPostingContext.addViolation(bv);

                    }
                }
            }
            //}
        } else {
            BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_TRAIN_VISIT_MISSING"), (Throwable) null, null, null, null);
            inEdiPostingContext.addViolation(bv);
        }
        return trainVisit;
    }

    private TruckVisitAppointment validateTar(EdiPostingContext inEdiPostingContext,
                                              AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, String inEdiTvApptRefNbr) {

        TruckVisitAppointment tvAppt = null;

        if (inEdiTvApptRefNbr != null) {
            tvAppt = findActiveTruckVisitAppointment(inEdiPostingContext, inEdiTvApptRefNbr);
            if (tvAppt != null) {
                validatePreanTruckCoAgainstTvApptTruckCo(inEdiPostingContext, inEdiAppointment, tvAppt);
            }
        }
        /*else if (MSG_FUNCTION_UPDATE.equals(inEdiAppointment.getMsgFunction())) {
         inEdiPostingContext.addViolation(BizViolation.create(PropertyKeyFactory.valueOf("TAR nbr is missing in a Truck COPINO Update msg"), (Throwable) null, null, null, null));
         }*/
        return tvAppt;
    }

    private void validatePreanTruckCoAgainstTvApptTruckCo(EdiPostingContext inEdiPostingContext,
                                                          AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
                                                          TruckVisitAppointment inTvAppt) {
        if (inEdiAppointment.getEdiTruckingCompany() != null) {
            String truckCompId = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyId();
            TruckingCompany tvTruckingCompany = inTvAppt.getTruckingCompany();
            String tvTruckCoId = tvTruckingCompany.getBzuId();
            if (tvTruckCoId != null && !tvTruckCoId.equals(truckCompId)) {
                BizViolation bv = BizViolation.create(RoadPropertyKeys.GATE__APPT_TRUCK_CO_DIFFERS_FROM_TRANS_TRUCK_CO, (Throwable) null, null, null, null);
                inEdiPostingContext.addViolation(bv);
            }
        }
    }

    private void resolveDuplicatePrean(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
                                       String inBasicType, String inCtrId, String inGateId, CarrierVisit inLandsideCarrierVisit, EquipmentOrder inEdiEqo,
                                       TruckerFriendlyTranSubTypeEnum inApptType) throws BizViolation {

        //GateAppointment prevPrean = findPrevMatchingPrean(inEdiEqoNbr, inCtrId,inEdiPostingContext, inEdiAppointment);

        log("resolveDuplicatePrean start");

        TruckerFriendlyTranSubTypeEnum[] tranTypes = [inApptType];
        GateAppointment prevPrean = _apptFndr.findAppointmentByContainerId(inCtrId, tranTypes, AppointmentStateEnum.CREATED);

        if (prevPrean != null) {
            log("prevPrean found:" + prevPrean.getGapptNbr());
            /*if (STATUS_UPDATE.equals(prevPrean.getFieldString(RESPONSE_MSG_TYPE))) {
             HibernateApi.getInstance().delete(prevPrean);
             }*/
            //duplicate prean by different TC must not be cancelled however the COPINO must get Reject and email notification
            // should be sent out
            String ediTrkCmpny = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyId();
            String preanTrkCmpny = prevPrean.getGapptTruckingCompany().getBzuId();
            log("ediTrkCmpny:$ediTrkCmpny + preanTrkCmpny:$preanTrkCmpny");
            if (!ediTrkCmpny.equals(preanTrkCmpny)) {
                String message = "pre-announcement:[" + prevPrean.getGapptNbr() + "] for unit:[$inCtrId]...already exists.";
                GeneralReference gr = GeneralReference.findUniqueEntryById("PREANNOUNCEMENT", "EMAIL", "DUPLICATE_PREAN");
                String emailAddress = gr ? gr.getRefValue1() : "";
                if (emailAddress) {
                    try {
                        ESBClientHelper.sendEmailAttachments(ContextHelper.getThreadUserContext(),
                                FrameworkMessageQueues.EMAIL_QUEUE, emailAddress, "noreply@apmterminals.com", message, message, null);
                    }
                    catch (Exception e) {
                        log("Exception:" + e.getMessage());
                    }

                }

                Object[] parms = [inCtrId];
                BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINO_ACTIVE_PREAN_EXISTS_FOR_CTR_TRANS_TYPE"), (Throwable) null, null, null, parms);
                inEdiPostingContext.addViolation(bv);

            }
            GeneralReference dupPreanActionGf = GeneralReference.findUniqueEntryById("PREANNOUNCEMENT", "DUPLICATE", "ACTION", "SAME_OPERATOR");

            String dupPreanAction = dupPreanActionGf == null ? "CANCEL" : (String) dupPreanActionGf.getRefValue1();

            if ("CANCEL".equals(dupPreanAction)) {
                cancelPrevPrean(prevPrean, inEdiAppointment);
            } else {

                if (inGateId.equals(prevPrean.getGapptGate().getGateId()) && sameOperator(prevPrean, inEdiAppointment, inLandsideCarrierVisit)) {

                    Object[] parms = [prevPrean.getGapptCtrId(), prevPrean.getGapptTranType().getKey(), inGateId];
                    BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_DUPLICATE_PREAN"), (Throwable) null, null, null, parms);
                    inEdiPostingContext.addViolation(bv);

                } else {
                    //different gate & same operator, same gate & different operator, different gate and different operator
                    cancelPrevPrean(prevPrean, inEdiAppointment);

                }

            }

        }
        log("resolveDuplicatePrean end");
    }

    private void cancelPrevPrean(GateAppointment prevPrean, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {
        log("cancelPrevPrean start");

        prevPrean.disassociateTva();

        getLibrary("PANPreanUtils").cancelPrean(prevPrean, PREANNOUNCEMENT, PREV_PREAN_CANCELLED, true);

        HibernateApi.getInstance().flush();
        inEdiAppointment.setAppointmentId(null);

        log("cancelPrevPrean end");
    }

    private GateAppointment findPrean(EdiPostingContext inEdiPostingContext, String inEdiCtrApptPriorRefNbr) {
        GateAppointment gappt;
        gappt = _apptFndr.findGateAppointmentByExternalReferenceNbr(inEdiCtrApptPriorRefNbr, AppointmentStateEnum.CREATED);

        if (gappt == null) {
            BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_ACTIVE_PREAN_WITH_REF_NBR_NOT_FOUND"), (Throwable) null, null, null, null);
            inEdiPostingContext.addViolation(bv);
        }

        return gappt;
    }

    //Methods copied from AppointmentPosterPea

    private Facility determineFacility(EdiPostingContext inEdiPostingContext,
                                       AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {

        String fcyId = inEdiAppointment.getAppointmentFacilityId();
        Complex complex = ContextHelper.getThreadComplex();
        Facility fcy = Facility.findFacility(fcyId, complex);

        if (fcy == null) {
            BizViolation bv = BizViolation.create(ArgoPropertyKeys.INVALID_FACILITY_ID, null, fcyId, complex.getCpxName());
            inEdiPostingContext.addViolation(bv);
        }
        return fcy;
    }

    private Gate determineGate(EdiPostingContext inEdiPostingContext,
                               AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, Facility inFcy) {

        String gateId = inEdiAppointment.getAppointmentGateId();

        Gate gate = Gate.findGateById(gateId);

        if (gate == null) {
            BizViolation bv = BizViolation.create(RoadPropertyKeys.GATE__INVALID_GATE_ID, null, gateId);
            inEdiPostingContext.addViolation(bv);
        }

        return gate;
    }

    private Date mergeDateTime(EdiPostingContext inEdiPostingContext,
                               AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {
        Date apptDate = ArgoEdiUtils.convertLocalToUtcDate(inEdiAppointment.getAppointmentDate(), inEdiPostingContext.getTimeZone());
        Date apptTime = ArgoEdiUtils.convertLocalToUtcDate(inEdiAppointment.getAppointmentTime(), inEdiPostingContext.getTimeZone());
        Calendar convertedApptDt = ArgoEdiUtils.mergeDateAndTime(apptDate, apptTime);
        return convertedApptDt.getTime();
    }

    private TruckerFriendlyTranSubTypeEnum determineAppointmentType(EdiPostingContext inEdiPostingContext,
                                                                    AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {
        String basicType = inEdiAppointment.getAppointmentType();
        String eqClassStr = inEdiAppointment.getAppointmentEqClass();
        String categoryStr = inEdiAppointment.getCategory();
        String freightKindStr = inEdiAppointment.getFreightKind();

        UnitCategoryEnum category = UnitCategoryEnum.getEnum(categoryStr);
        FreightKindEnum freightKind = FreightKindEnum.getEnum(freightKindStr);
        EquipClassEnum eqClass = EquipClassEnum.getEnum(eqClassStr);
        eqClass = EquipClassEnum.CONTAINER;

        TruckerFriendlyTranSubTypeEnum apptType = null;
        if (DROPOFF.equals(basicType)) {
            if (EquipClassEnum.CHASSIS.equals(eqClass)) {
                apptType = TruckerFriendlyTranSubTypeEnum.DOC;
            } else if (UnitCategoryEnum.EXPORT.equals(category)) {
                apptType = TruckerFriendlyTranSubTypeEnum.DOE;
            } else if (UnitCategoryEnum.STORAGE.equals(category)) {
                if (FreightKindEnum.MTY.equals(freightKind)) {
                    apptType = TruckerFriendlyTranSubTypeEnum.DOM;
                }
            } else if (UnitCategoryEnum.IMPORT.equals(category)) {
                apptType = TruckerFriendlyTranSubTypeEnum.DOI;
            } else if (UnitCategoryEnum.TRANSSHIP.equals(category)) {
                apptType = TruckerFriendlyTranSubTypeEnum.DOI;
            }
        } else if (PICKUP.equals(basicType)) {
            if (EquipClassEnum.CHASSIS.equals(eqClass)) {
                apptType = TruckerFriendlyTranSubTypeEnum.PUC;
            } else if (UnitCategoryEnum.EXPORT.equals(category)) {
                apptType = TruckerFriendlyTranSubTypeEnum.PUE;
            } else if (UnitCategoryEnum.STORAGE.equals(category)) {
                if (FreightKindEnum.MTY.equals(freightKind)) {
                    apptType = TruckerFriendlyTranSubTypeEnum.PUM;
                }
            } else if (UnitCategoryEnum.IMPORT.equals(category)) {
                apptType = TruckerFriendlyTranSubTypeEnum.PUI;
            } else if (UnitCategoryEnum.TRANSSHIP.equals(category)) {
                apptType = TruckerFriendlyTranSubTypeEnum.PUE;
            }
        }

        if (apptType == null) {
            Object[] parms = [basicType, eqClassStr, categoryStr, freightKindStr];
            BizViolation bv = BizViolation.create(RoadApptsPropertyKeys.ERROR_CANNOT_DETERMINE_TYPE, (Throwable) null, null, null,
                    parms);
            inEdiPostingContext.addViolation(bv);
        }
        return apptType;
    }


    protected RoutingPoint extractRoutingPoint(EdiPostingContext inEdiPostingContext, Port inPort, MetafieldId inPointField) {
        // rewritten by JJS, 2005-11-05
        RoutingPoint rp = null;

        if (inPort != null) {
            String pointId = inPort.getPortId();
            if (pointId != null) {
                pointId = pointId.trim();
                if (!StringUtils.isEmpty(pointId)) {

                    if (pointId.length() == 5) {
                        rp = RoutingPoint.resolveRoutingPointFromUnLoc(pointId);

                        // 2006-06-22 ARGO-4395 ramsatis v1.1.3.0 - Just reject the container if there is no routing point
                        // with the unloc code found in the baplie. todo RoutingPoint.resolveRoutingPointFromUnLoc() might be
                        // the right place to fix(?)
                        if (rp != null && !ObjectUtils.equals(pointId, rp.getPointUnLoc().getUnlocId())) {
                            BizViolation bv = BizViolation.createFieldViolation(ArgoPropertyKeys.ENTRY_INVALID, null, inPointField, pointId);
                            inEdiPostingContext.addViolation(bv);
                            return null;
                        }
                    } else {
                        // ARGO-16376: We need to consider port convention also here: Schedule D Code, Schedule K Code and SPLC Code
                        String portIdConvention = inPort.getPortIdConvention();

                        if (StringUtils.isEmpty(portIdConvention)) {
                            rp = RoutingPoint.resolveRoutingPointFromPortCode(pointId); // original statement
                        } else {
                            rp = RoutingPoint.resolveRoutingPointFromEncoding(portIdConvention, pointId);
                            // updated statement for port id convention
                        }
                    }

                    if (rp == null) {
                        BizViolation bv = BizViolation.createFieldViolation(ArgoPropertyKeys.ENTRY_INVALID, null, inPointField, pointId);
                        inEdiPostingContext.addViolation(bv);
                    }
                }
            }
        }
        return rp;
    }


    private EquipmentOrder findOrder(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
                                     ScopedBizUnit inLine, String inBasicType, FreightKindEnum inFreightKind) {

        String ediEqoNbr = inEdiAppointment.getOrderNbr();
        EquipmentOrder eqo = null;



        if (ediEqoNbr != null && ediEqoNbr.length() > 0 && inLine != null) {

            if (DROPOFF.equals(inBasicType)) {
                try {

                    eqo = Booking.findBookingByUniquenessCriteria(ediEqoNbr, inLine, null);

                    if (FreightKindEnum.MTY.equals(inFreightKind)) {

                        if (eqo != null && FreightKindEnum.FCL.equals(eqo.getEqoEqStatus())) {

                            eqo = null;
                        }

                        if (eqo == null) {

                            eqo = EquipmentReceiveOrder.findEroByUniquenessCriteria(ediEqoNbr, inLine);
                        }
                    } else {
                        if (eqo != null && FreightKindEnum.MTY.equals(eqo.getEqoEqStatus())) {

                            eqo = null;
                        }

                    }

                    if (eqo == null) {
                        eqo = RailOrder.findRailOrder(ediEqoNbr, inLine);

                    }

                } catch (BizViolation bv) {
                    inEdiPostingContext.addViolation(bv);
                }
            }
            //empties only - the full ones always have a pin
            else {

                try {

                    //eqo =  Booking.findBookingByUniquenessCriteria(ediEqoNbr, inLine, null);

                    //if (eqo == null) {

                    eqo = EquipmentDeliveryOrder.findEquipmentDeliveryOrder(ediEqoNbr, inLine);
                    //}

                    /*if (eqo == null) {
                     eqo = RailOrder.findRailOrder(ediEqoNbr, inLine);
                     }*/

                } catch (BizViolation bv) {
                    inEdiPostingContext.addViolation(bv);
                }

            }

        }

        /*if (eqo == null) {
         BizViolation bv = BizViolation.create(RoadPropertyKeys.GATE__ORDER_NOT_FOUND_FOR_LINE, null, ediEqoNbr, inLine.getBzuId());
         inEdiPostingContext.addViolation(bv);
         }*/

        return eqo;
    }

    private Unit findActiveOrAdvisedUnitForDelivery(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, Facility inFacility)
            throws BizViolation {
        Container ctr = Container.findContainer(inEdiAppointment.getContainerId());
        Unit unit = null;
        if (ctr != null) {

            unit = _unitFinder.findActiveUnit(inFacility.getFcyComplex(), ctr);
            FreightKindEnum freightKind = FreightKindEnum.getEnum(inEdiAppointment.getFreightKind());

            if (unit == null || !freightKind.equals(unit.getUnitFreightKind())) {
                LocTypeEnum[] locTypes = [LocTypeEnum.TRAIN, LocTypeEnum.VESSEL];
                List<Unit> unitList = _unitFinder.findAdvisedUnits(inFacility.getFcyComplex(), ctr, locTypes);
                for (Unit advisedUnit : unitList) {
                    if (freightKind.equals(advisedUnit.getUnitFreightKind())) {
                        unit = advisedUnit;
                        break;
                    }
                }
            }
        }
        return unit;
    }

    private void validateTranRefNbr(String inEdiTranRefNbr, EdiPostingContext inEdiPostingContext) {
        if (isRefNbrAlreadyUsed(inEdiTranRefNbr)) {
            BizViolation bv = BizViolation.create(RoadApptsPropertyKeys.GATE__APPOINTMENT_EXIST_FOR_EXTERNAL_REF_NBR, null, inEdiTranRefNbr);
            inEdiPostingContext.addViolation(bv);
        }

    }

    private boolean isRefNbrAlreadyUsed(String inEdiTranRefNbr) {
        DomainQuery dq = QueryUtils.createDomainQuery(RoadApptsEntity.GATE_APPOINTMENT)
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_STATE, AppointmentStateEnum.CREATED))
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_REFERENCE_NBR, inEdiTranRefNbr));

        return HibernateApi.getInstance().existsByDomainQuery(dq);
    }

    private Date trimMillis(Date inDate) {

        Calendar cal = new GregorianCalendar(ContextHelper.getThreadUserTimezone());
        cal.setTime(inDate);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();

    }

    //CSDV-1657

    public AppointmentTimeSlot getAppointmentTimeSlot(
            Serializable inGateGkey,
            TranSubTypeEnum inTransType,
            Date inRequestedDateTime,
            Serializable inCurrentSlotGkey,
            Boolean inExactTime,
            Element inOutResponse
    ) throws BizViolation {

        Date appointmentDate = inRequestedDateTime;

        Map<Date, TimeSlotBean> slots = _appMgr.getOpenTimeSlotsForDate(inGateGkey,
                inTransType,
                inRequestedDateTime,
                Boolean.TRUE,                   // error if no slots
                inCurrentSlotGkey,
                null
        );

        TimeSlotBean slotBean;

        if (!inExactTime) {
            slotBean = (TimeSlotBean) slots.values().toArray()[0];
        } else {
            slotBean = getExactTimeSlot(slots, appointmentDate);
            if (slotBean == null) {
                if (inOutResponse != null) {
                    for (TimeSlotBean bean : slots.values()) {
                        Element availableSlots = new Element(AVAILABLE_SLOTS);
                        availableSlots.setAttribute(SLOT_START, AppointmentApiXmlUtil.getDateFormattedForXml(bean.getStartDate()));
                        availableSlots.setAttribute(SLOT_END, AppointmentApiXmlUtil.getDateFormattedForXml(bean.getEndDate()));
                        inOutResponse.addContent(availableSlots);
                    }
                }
                throw BizViolation.create(RoadApptsPropertyKeys.ERROR_NO_SLOTS_FOR_DATE_MESSAGE, null);
            }
        }
        Serializable slotGkey = slotBean.getSlotGkey();
        AppointmentTimeSlot slot;
        if (slotGkey == null) {
            Serializable quotaRuleGkey = slotBean.getQuotaRuleGkey();
            AppointmentQuotaRule rule = (AppointmentQuotaRule) HibernateApi.getInstance().load(AppointmentQuotaRule.class, quotaRuleGkey);

            //slot = AppointmentTimeSlot.findOrCreateAppointmentTimeSlot(rule, slotBean.getStartDate(), slotBean.getTransactionType(),slotBean.getEndDate());


            slot = AppointmentTimeSlot.findAppointmentTimeSlot(rule.getAruleGkey(), slotBean.getStartDate(), slotBean.getEndDate(), slotBean.getTransactionType());
            if (slot == null) {

                slot = AppointmentTimeSlot.createAppointmentTimeSlot(rule, slotBean.getStartDate(), slotBean.getTransactionType(), slotBean.getEndDate());
            }


        } else {
            slot = (AppointmentTimeSlot) HibernateApi.getInstance().load(AppointmentTimeSlot.class, slotGkey);
        }
        return slot;
    }


    String SLOT_START = "slot-start";
    String SLOT_END = "slot-end";
    String AVAILABLE_SLOTS = "available-slots";

    // End if CSDV-1657


    private GateAppointment createNewAndDeleteOldAppointment(EdiPostingContext inEdiPostingContext,
                                                             GateAppointment inOldAppointment,
                                                             Date inApptDate) {
        GateAppointment newAppointment = createNewAppointment(inEdiPostingContext, inOldAppointment, inApptDate);

        if (newAppointment != null) {
            String eqId = inOldAppointment.getGapptCtrId();
            HibernateApi.getInstance().delete(inOldAppointment);
            HibernateApi.getInstance().flush();

            newAppointment.setGapptReferenceNbr(inOldAppointment.getGapptReferenceNbr());
            //inEdiPostingContext.addWarning(BizWarning.create(RoadApptsPropertyKeys.DUPLICATE_APPT_DELETED, null, inOldAppointment.getGapptTranType(), eqId));
        }
        return newAppointment;
    }

    private GateAppointment createNewAppointment(EdiPostingContext inEdiPostingContext,
                                                 GateAppointment inOldAppointment,
                                                 Date inApptDate) {
        AppointmentTimeSlot slot;
        GateAppointment newAppt = new GateAppointment();

        IAppointmentManager appMgr = (IAppointmentManager) Roastery.getBean(IAppointmentManager.BEAN_ID);
        try {
            slot = appMgr.getAppointmentTimeSlot(inOldAppointment.getGapptGate().getGateGkey(), inOldAppointment.getGapptTranType().getInternalTranSubTypeEnum(), inApptDate, null, true);
        } catch (BizViolation bv) {
            inEdiPostingContext.addViolation(bv);
            return null;
        }
        try {
            HibernateApi.getInstance().flush();

        } catch (DataIntegrityViolationException e) {
            registerError("createNewAndDeleteOldAppointment1 : cought the exception");
            throw e;
        }

        copy(newAppt, inOldAppointment);

        newAppt.setGapptRequestedDate(inApptDate);
        newAppt.setGapptTimeSlot(slot);
        newAppt.setGapptNbr(getNextApptNbr());

        HibernateApi.getInstance().save(newAppt);
        try {
            HibernateApi.getInstance().flush();

        } catch (DataIntegrityViolationException e) {
            registerError("createNewAndDeleteOldAppointment2 : cought the exception");
            throw e;
        }
        newAppt.recordEvent(EventEnum.APPT_CREATE, null, null, null);

        return newAppt;
    }

    private void copy(GateAppointment newGappt, GateAppointment oldGappt) {

        newGappt.setFieldValue(RoadApptsField.GAPPT_STATE, oldGappt.getFieldValue(RoadApptsField.GAPPT_STATE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_TRAN_TYPE, oldGappt.getFieldValue(RoadApptsField.GAPPT_TRAN_TYPE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_FREIGHT_KIND, oldGappt.getFieldValue(RoadApptsField.GAPPT_FREIGHT_KIND));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_ID, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_ID));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_ACCESSORY_ID, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_ACCESSORY_ID));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_GROSS_WEIGHT, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_GROSS_WEIGHT));

        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_SEAL_NBR1, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_SEAL_NBR1));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_SEAL_NBR2, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_SEAL_NBR2));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_SEAL_NBR3, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_SEAL_NBR3));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_SEAL_NBR4, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_SEAL_NBR4));

        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_TRUCK_POSITION, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_TRUCK_POSITION));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_DOOR_DIRECTION, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_DOOR_DIRECTION));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CHASSIS_ID, oldGappt.getFieldValue(RoadApptsField.GAPPT_CHASSIS_ID));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CHS_IS_OWNERS, oldGappt.getFieldValue(RoadApptsField.GAPPT_CHS_IS_OWNERS));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CHASSIS_ACCESSORY_ID, oldGappt.getFieldValue(RoadApptsField.GAPPT_CHASSIS_ACCESSORY_ID));
        newGappt.setFieldValue(RoadApptsField.GAPPT_TRUCK_LICENSE_NBR, oldGappt.getFieldValue(RoadApptsField.GAPPT_TRUCK_LICENSE_NBR));
        newGappt.setFieldValue(RoadApptsField.GAPPT_TRUCK_ID, oldGappt.getFieldValue(RoadApptsField.GAPPT_TRUCK_ID));
        newGappt.setFieldValue(RoadApptsField.GAPPT_IMPORT_RELEASE_NBR, oldGappt.getFieldValue(RoadApptsField.GAPPT_IMPORT_RELEASE_NBR));
        newGappt.setFieldValue(RoadApptsField.GAPPT_IS_XRAY_REQUIRED, oldGappt.getFieldValue(RoadApptsField.GAPPT_IS_XRAY_REQUIRED));
        //newGappt.gapptReferenceNbr));			oldGappt.gapptReferenceNbr;
        newGappt.setFieldValue(RoadApptsField.GAPPT_NOTES, oldGappt.getFieldValue(RoadApptsField.GAPPT_NOTES));

        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING01, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING01));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING02, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING02));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING03, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING03));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING04, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING04));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING05, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING05));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING06, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING06));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING07, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING07));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING08, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING08));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING09, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING09));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING10, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING10));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING11, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING11));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING12, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING12));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING13, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING13));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING14, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING14));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING15, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT_FLEX_STRING15));

        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING01, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING01));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING02, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING02));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING03, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING03));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING04, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING04));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING05, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING05));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING06, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING06));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING07, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING07));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING08, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING08));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING09, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING09));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING10, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_STRING10));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_DATE01, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_DATE01));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UFV_FLEX_DATE02, oldGappt.getFieldValue(RoadApptsField.GAPPT_UFV_FLEX_DATE02));
        //newGappt.gapptRequestedDate));			oldGappt.gapptRequestedDate;
        newGappt.setFieldValue(RoadApptsField.GAPPT_IS_PEAK_HOUR, oldGappt.getFieldValue(RoadApptsField.GAPPT_IS_PEAK_HOUR));
        newGappt.setFieldValue(RoadApptsField.GAPPT_IS_REUSED, oldGappt.getFieldValue(RoadApptsField.GAPPT_IS_REUSED));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CANCEL_DATE, oldGappt.getFieldValue(RoadApptsField.GAPPT_CANCEL_DATE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_ORIGIN, oldGappt.getFieldValue(RoadApptsField.GAPPT_ORIGIN));
        newGappt.setFieldValue(RoadApptsField.GAPPT_DESTINATION, oldGappt.getFieldValue(RoadApptsField.GAPPT_DESTINATION));
        newGappt.setFieldValue(RoadApptsField.GAPPT_START_SLACK_OVERRIDE, oldGappt.getFieldValue(RoadApptsField.GAPPT_START_SLACK_OVERRIDE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_END_SLACK_OVERRIDE, oldGappt.getFieldValue(RoadApptsField.GAPPT_END_SLACK_OVERRIDE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_GATE, oldGappt.getFieldValue(RoadApptsField.GAPPT_GATE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_TRUCKING_COMPANY, oldGappt.getFieldValue(RoadApptsField.GAPPT_TRUCKING_COMPANY));
        newGappt.setFieldValue(RoadApptsField.GAPPT_LINE_OPERATOR, oldGappt.getFieldValue(RoadApptsField.GAPPT_LINE_OPERATOR));
        newGappt.setFieldValue(RoadApptsField.GAPPT_UNIT, oldGappt.getFieldValue(RoadApptsField.GAPPT_UNIT));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CTR_EQUIP_TYPE, oldGappt.getFieldValue(RoadApptsField.GAPPT_CTR_EQUIP_TYPE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CHASSIS_EQUIP_TYPE, oldGappt.getFieldValue(RoadApptsField.GAPPT_CHASSIS_EQUIP_TYPE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_ORDER, oldGappt.getFieldValue(RoadApptsField.GAPPT_ORDER));
        newGappt.setFieldValue(RoadApptsField.GAPPT_ORDER_ITEM, oldGappt.getFieldValue(RoadApptsField.GAPPT_ORDER_ITEM));
        newGappt.setFieldValue(RoadApptsField.GAPPT_TRUCK_DRIVER, oldGappt.getFieldValue(RoadApptsField.GAPPT_TRUCK_DRIVER));
        //newGappt.GAPPT_TIME_SLOT));
        //newGappt.GAPPT_TRUCK_VISIT_APPOINTMENT));
        newGappt.setFieldValue(RoadApptsField.GAPPT_AGENT1, oldGappt.getFieldValue(RoadApptsField.GAPPT_AGENT1));
        newGappt.setFieldValue(RoadApptsField.GAPPT_AGENT2, oldGappt.getFieldValue(RoadApptsField.GAPPT_AGENT2));
        newGappt.setFieldValue(RoadApptsField.GAPPT_VESSEL_VISIT, oldGappt.getFieldValue(RoadApptsField.GAPPT_VESSEL_VISIT));
        newGappt.setFieldValue(RoadApptsField.GAPPT_POL, oldGappt.getFieldValue(RoadApptsField.GAPPT_POL));
        newGappt.setFieldValue(RoadApptsField.GAPPT_POD1, oldGappt.getFieldValue(RoadApptsField.GAPPT_POD1));
        newGappt.setFieldValue(RoadApptsField.GAPPT_POD2, oldGappt.getFieldValue(RoadApptsField.GAPPT_POD2));
        newGappt.setFieldValue(RoadApptsField.GAPPT_POD1_OPTIONAL, oldGappt.getFieldValue(RoadApptsField.GAPPT_POD1_OPTIONAL));
        newGappt.setFieldValue(RoadApptsField.GAPPT_POD2_OPTIONAL, oldGappt.getFieldValue(RoadApptsField.GAPPT_POD2_OPTIONAL));
        newGappt.setFieldValue(RoadApptsField.GAPPT_CONSIGNEE, oldGappt.getFieldValue(RoadApptsField.GAPPT_CONSIGNEE));
        newGappt.setFieldValue(RoadApptsField.GAPPT_SHIPPER, oldGappt.getFieldValue(RoadApptsField.GAPPT_SHIPPER));
        newGappt.setFieldValue(RoadApptsField.GAPPT_ORIGINAL_TIME_SLOT, oldGappt.getFieldValue(RoadApptsField.GAPPT_ORIGINAL_TIME_SLOT));

        newGappt.setFieldValue(PREAN_LANDSIDE_CARRIER_VISIT, oldGappt.getFieldValue(PREAN_LANDSIDE_CARRIER_VISIT));
        newGappt.setFieldValue(PREAN_STATUS, oldGappt.getFieldValue(PREAN_STATUS));
        newGappt.setFieldValue(PREAN_EQO_NBR, oldGappt.getFieldValue(PREAN_EQO_NBR));
        //newGappt.setFieldValue(RESPONSE_MSG_TYPE, oldGappt.getFieldValue(RESPONSE_MSG_TYPE));
        newGappt.setFieldValue(EDI_TRAN_GKEY, oldGappt.getFieldValue(EDI_TRAN_GKEY));
        //newGappt.setFieldValue(PREAN_VALIDATION_RUN_ID, oldGappt.getFieldValue(PREAN_VALIDATION_RUN_ID));
        //newGappt.setFieldValue(EDI_ORIGINAL_REQUEST_DATETIME, oldGappt.getFieldValue(EDI_ORIGINAL_REQUEST_DATETIME));
        //newGappt.setFieldValue(PREAN_LAST_VALIDATION_DATE, oldGappt.getFieldValue(PREAN_LAST_VALIDATION_DATE));
        //newGappt.setFieldValue(SEND_MSG, oldGappt.getFieldValue(SEND_MSG));
        newGappt.setFieldValue(EDI_PARTNER_NAME, oldGappt.getFieldValue(EDI_PARTNER_NAME));


    }

    private void generateApptNbrs(int inCounter) {

        while (inCounter > 0) {

            generateApptNbr();
            inCounter--;
        }

    }

    private void generateApptNbr() {

        AppointmentIdProvider idProvider = new AppointmentIdProvider();

        Long id = idProvider.getNextAppointmentId();
        _apptNbrs.add(id);

    }

    private Long getNextApptNbr() {

        Long nextNbr = null;

        Iterator it = _apptNbrs.iterator();

        if (it.hasNext()) {
            nextNbr = (Long) it.next();
            it.remove();
        }

        return nextNbr;
    }

    void setEdiCtrId(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {

        if (DUMMYCTRID.equals(inEdiAppointment.getContainerId())) {
            inEdiAppointment.setContainerId(null);
        }

    }

    private void checkIfCopinoAccepted(EdiPostingContext inEdiPostingContext, CarrierVisit inLandsideCarrierVisit) {

        if (inLandsideCarrierVisit != null) {
            VisitDetails visitDtls = inLandsideCarrierVisit.getCvCvd();
            if (visitDtls != null) {
                Boolean stopAccCopinos = (Boolean) visitDtls.getField(STOP_ACCEPTING_COPINOS);
                if (stopAccCopinos) {
                    BizViolation bv = BizViolation.create(PropertyKeyFactory.valueOf("CUSTOM_COPINOS_NOT_ACCEPTED_FOR_CARRIER_VISIT"), (Throwable) null, null, null, null);
                    inEdiPostingContext.addViolation(bv);
                }

            }
        }

    }

    private boolean isDifferentCtrType(Unit inUnit, String inCtrTypeId, EdiPostingContext inEdiPostingContext) {

        boolean isDifferentCtrType = false;

        if (inUnit != null && inCtrTypeId != null) {

            EquipType ctrType = EquipType.findEquipType(inCtrTypeId);
            if (ctrType == null) {
                BizViolation bv = BizViolation.create(ArgoPropertyKeys.EQUIPMENT_NOT_FOUND, null, inCtrTypeId);
                inEdiPostingContext.addViolation(bv);
                ctrType = EquipType.getUnknownEquipType();
            }

            isDifferentCtrType = inUnit.getPrimaryEq().getEqEquipType() != ctrType;
        }

        return isDifferentCtrType;


    }

    List<Long> _apptNbrs;
    Iterator _apptNbrIterator;


    private static String PICKUP = "PICKUP";
    private static String DROPOFF = "DROPOFF";
    private static String UNKN = "UNKN";

    private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
    private static
    def _preanErrorUtil = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANPreanErrorUtil");

    //Prean Fields
    public
    static MetafieldId PREAN_LANDSIDE_CARRIER_VISIT = MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFlandsideCV");
    public static MetafieldId PREAN_EQO_NBR = _panFields.PREAN_EQO_NBR;
    public static MetafieldId RESPONSE_MSG_TYPE = _panFields.RESPONSE_MSG_TYPE;

    public static MetafieldId EDI_TRAN_GKEY = _panFields.EDI_TRAN_GKEY;
    public static MetafieldId EDI_PARTNER_NAME = _panFields.EDI_PARTNER_NAME;

    public static MetafieldId PREAN_VALIDATION_RUN_ID = _panFields.PREAN_VALIDATION_RUN_ID;


    public static MetafieldId EDI_TRANS_REF_NBR = RoadApptsField.GAPPT_REFERENCE_NBR;
    public static MetafieldId EDI_ORIGINAL_REQUEST_DATETIME = _panFields.EDI_ORIGINAL_REQUEST_DATETIME;
    public static MetafieldId SEND_MSG = _panFields.SEND_MSG;

    public static MetafieldId PREAN_STATUS = _panFields.PREAN_STATUS;
    //Unit/Ufv Fields
    public static MetafieldId UFV_PREAN_RECEIVAL_STATUS = UnitField.UFV_FLEX_STRING01;
    public static MetafieldId UFV_PREAN_DELIVERY_STATUS = UnitField.UFV_FLEX_STRING02;

    private static String EDI_ORDER_NBR_KEY = "EDI_ORDER_NBR_KEY";
    private static String EDI_ORDER_KEY = "EDI_ORDER_KEY";
    private static String ORDER_MISMATCH_EQOI = "ORDER_MISMATCH_EQOI";
    public static String VALIDATION_RUN_ID_KEY = "validationRunId";
    private static String CAN_BE_RETIRED_PREAN = "CAN_BE_RETIRED_PREAN";

    private static String EDI_ORIGINAL_REQUESTED_DATETIME_KEY = "EDI_ORIGINAL_REQUESTED_DATETIME_KEY";
    private static String TRUCK_VISIT_APPOINTMENT_KEY = "TRUCK_VISIT_APPOINTMENT_KEY";
    private static String LANDSIDE_CARRIER_VISIT_KEY = "LANDSIDE_CARRIER_VISIT_KEY";

    private static String BARGE_GATE = "BARGE";
    private static String RAIL_GATE = "RAIL";
    //16-May-2017 : Create the Prean for the RAIL ITT gate.
    private static String RAIL_ITT_GATE = "RAIL_ITT";
    private static String RAIL_CARRIER_QUALIFIER = "2-26";
    private static String TRANSHIP_CARRIER_QUALIFIER = "1-27";

    private
    static List<String> TRANSHIP_QUALIFIER_CODES = new ArrayList<String>(Arrays.asList("DDE", "DDN", "DDW", "DMU", "DBF"));
    private
    static List<String> RAIL_QUALIFIER_CODES = new ArrayList<String>(Arrays.asList("ORT", "DRB", "RTW", "KRM", "RCT", "DRN"));

    private static String STATUS_UPDATE_REQST = "STATUS_UPDATE_REQST";
    private static String STATUS_UPDATE = "STATUS_UPDATE";


    private static String MSG_FUNCTION_CREATE = "O"; //9
    private static String MSG_FUNCTION_UPDATE = "X";   //5
    private static String MSG_FUNCTION_CANCEL = "D";   //1
    private static String MSG_FUNCTION_GET_STATUS_UPDATE = "S"; //13

    private static AppointmentFinder _apptFndr = (AppointmentFinder) Roastery.getBean(AppointmentFinder.BEAN_ID);
    private static IAppointmentManager _appMgr = (IAppointmentManager) Roastery.getBean(IAppointmentManager.BEAN_ID);
    private static OrdersFinder _ordersFinder = (OrdersFinder) Roastery.getBean("ordersFinder");


    private static Map<String, String> DEFAULT_CANCEL_NOTES = new HashMap();

    private static String PREV_PREAN_CANCELLED = "PREV_PREAN_CANCELLED";

    static {
        DEFAULT_CANCEL_NOTES.put("CANCELLED_BY_SENDER", "Cancelled by sender");
        DEFAULT_CANCEL_NOTES.put(PREV_PREAN_CANCELLED, "Cancelled by the terminal");
    }

    private static String DUMMYCTRID = "DUMMYCTRID";

    private static UnitFinder _unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);

    private
    static MetafieldId STOP_ACCEPTING_COPINOS = MetafieldIdFactory.valueOf("customFlexFields.cvdCustomDFFstopAcceptCopinos");

    private static MetafieldId PREAN_COPINO_MSG_TYPE = MetafieldIdFactory.valueOf("gapptChassisId");

    private static String PREANNOUNCEMENT = "PREANNOUNCEMENT";

    private final Logger LOGGER = Logger.getLogger(PANApptPostInterceptor.class);

}
