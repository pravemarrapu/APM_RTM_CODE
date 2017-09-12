package com.weserve.APM.EDI

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.jdom.Attribute;
import org.jdom.Element;

import java.text.SimpleDateFormat;


import com.navis.argo.ContextHelper;
import com.navis.argo.business.api.ServicesManager;
import com.navis.argo.business.atoms.EventEnum;
import com.navis.argo.business.model.CarrierVisit;
import com.navis.argo.util.XmlUtil;
import com.navis.edi.business.api.EdiCompoundField;
import com.navis.edi.business.entity.EdiTransaction;
import com.navis.external.edi.entity.AbstractEdiExtractInterceptor;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.framework.business.Roastery;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.query.common.api.QueryResult;
import com.navis.framework.util.BizFailure;
import com.navis.framework.util.ValueHolder;
import com.navis.road.RoadApptsEntity;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.appointment.model.TruckVisitAppointment;
import com.navis.road.business.atoms.AppointmentStateEnum;
import com.navis.services.business.event.Event
import com.navis.edi.business.entity.EdiInterchange
import com.navis.edi.business.entity.EdiBatch

import com.navis.road.business.appointment.model.AppointmentTimeSlot
import com.navis.argo.business.model.GeneralReference;

/*
 *  04-11-2013 CSDV-1503 Add all errors with the same code to the message
 *  CSDV-1530 Comment out adding Landside Carrier Element - it is done in mapping
 *
 *  Date: 26/02/2014 CSDV-1656  Fixed processCancelledPrean to set message status to APPROVED when COPINO was cancelled by sender
 *
 *  Date: 06/03/2014 CSDV-1630 - Fixed findPreanByEditran domain query
 *
 *  Date 25/06/14 CSDV-2152 use GateAppointment dynamic flex fields instead of gapptUfv and gapptUnit flex fields for prean specific values
 *
 *  Date 27/06/14 CSDV-2156 set ediAckTvapptNbr to Tva Ref Nbr
 *
 *  Date  04/09/14 CSDV-2238 for a cancelled prean set ediAckErrCode to CTV for all cancellations exept done by COPINO Cancel message
 *
 *  Date 04-05-2015 CSDV-2832 moved getCancelNotes to PreanUtils
 *
 *  Date 18-08-2017 CSDV-###  BGM segment appears with 660 code for receival and 661 for delivery prean
 *
 *  Date 27-08-2017 CSDV-### Address null RFF-CAO being sent
 *  Modified By: Praveen Babu
 *  Date 06/09/2017 APMT #28 - Issue related to derive the prean when multiple preans are posted and one of the prean in that is failed.

 */

public class PANNonStdAperakExtractInterceptor extends AbstractEdiExtractInterceptor {

    public Element beforeEdiMap(Map inParams) {

        LOGGER.setLevel(Level.DEBUG)
        Element tranElement = (Element) inParams.get("XML_TRANSACTION");
        Element ackElement = tranElement.getChild("ediAcknowledgement", XmlUtil.ARGO_NAMESPACE);
        EdiInterchange ediInterchange = (EdiInterchange) inParams.get("ENTITY");

        LOGGER.debug("Extract interceptor :: "+ediInterchange);

        Set batchSet = ediInterchange.getEdiBatchSet();
        EdiBatch ediBatch = (EdiBatch) batchSet.iterator().next();
        Set tranSet = ediBatch.getEdiTransactionSet();
        //EdiTransaction ediTran = (EdiTransaction) tranSet.iterator().next();
        EdiTransaction ediTran = fetchMatchinEdiTran(tranSet, tranElement);
        validateAckInterchangeNbr(tranSet, tranElement);

        LOGGER.debug("Edi Transaction :: "+ediTran)

        GateAppointment prean = findPreanByEditran(ediTran.getEditranGkey());

        if (prean != null) {

            //addLandsideCarrierElement(tranElement, prean);

            TruckVisitAppointment tvappt = prean.getGapptTruckVisitAppointment();

            if (tvappt != null) {
                addTvapptElement(ackElement, tvappt);
            }

            AppointmentStateEnum preanState = prean.getApptState();

            if (AppointmentStateEnum.CREATED.equals(preanState)) {

                processActivePrean(tranElement, ackElement, prean);
            } else if (AppointmentStateEnum.CANCEL.equals(preanState)) {

                processCancelledPrean(tranElement, ackElement, prean);

            }

            inParams.put("PREAN", prean);
        } else {
            registerError("prean for COPINO tran " + ediTran.getFieldString(EdiCompoundField.EDITRAN_BATCH_INTERCHANGE_NUMBER) + " not found");
        }

        return tranElement;
    }

    private EdiTransaction fetchMatchinEdiTran(Set inEdiTransactionSet, Element inTranElement) {
        Element ackElement = inTranElement.getChild("ediAcknowledgement", XmlUtil.ARGO_NAMESPACE);
        Element ediAckReferenceNbr = ackElement.getChild("ediAckReferenceNbr", XmlUtil.ARGO_NAMESPACE);
        Iterator txnSetIterator = inEdiTransactionSet.iterator();
        while (txnSetIterator.hasNext()) {
            EdiTransaction ediTransaction = (EdiTransaction) txnSetIterator.next();
            if(ediTransaction.getEditranPrimaryKeywordValue().equalsIgnoreCase(ediAckReferenceNbr.getValue())) {
                return ediTransaction;
            }
        }
    }
    private void validateAckInterchangeNbr(Set inEdiTransactionSet, Element inTranElement) {

        Element ackElement = inTranElement.getChild("ediAcknowledgement", XmlUtil.ARGO_NAMESPACE);
        Element ediAckReferenceNbr = ackElement.getChild("ediAckReferenceNbr", XmlUtil.ARGO_NAMESPACE);
        Iterator txnSetIterator = inEdiTransactionSet.iterator();
        while (txnSetIterator.hasNext()) {
            EdiTransaction ediTransaction = (EdiTransaction) txnSetIterator.next();
            if (ediTransaction.getEditranPrimaryKeywordValue().equalsIgnoreCase(ediAckReferenceNbr.getValue())) {
                String interchangeNbr = inTranElement.getAttributeValue("ediAckInterchangeNbr", XmlUtil.ARGO_NAMESPACE);
                log("Pre processing logic :: " + inTranElement.getAttributeValue("ediAckInterchangeNbr", XmlUtil.ARGO_NAMESPACE))
                if (interchangeNbr.contains("-")) {
                    String prefixInterchange = interchangeNbr.substring(0, interchangeNbr.indexOf("-") + 1)
                    interchangeNbr = interchangeNbr.substring(interchangeNbr.indexOf("-") + 1);
                    String ediTransactionValue = String.valueOf(ediTransaction.getEditranSeqWithinBatch());
                    if (!ediTransactionValue.equalsIgnoreCase(interchangeNbr)) {
                        interchangeNbr = prefixInterchange + ediTransaction.getEditranSeqWithinBatch();
                        inTranElement.setAttribute("ediAckInterchangeNbr", interchangeNbr, XmlUtil.ARGO_NAMESPACE);
                    }
                }
            }
        }
        log("Post processing logic :: " + inTranElement.getAttributeValue("ediAckInterchangeNbr", XmlUtil.ARGO_NAMESPACE))
    }

    private void addLandsideCarrierElement(Element inTranElement, GateAppointment inPrean) {

        String landsideCarrierId = null;

        String gateId = inPrean.getGapptGate().getGateId();

        if ("BARGE".equals(gateId) || "RAIL".equals(gateId)) {
            CarrierVisit landsideCarrierVisit = (CarrierVisit) inPrean.getField(_panFields.PREAN_LANDSIDE_CARRIER_VISIT);
            if (landsideCarrierVisit != null) {
                landsideCarrierId = landsideCarrierVisit.getCarrierOperator().getBzuId();
            }
        } else {
            if (inPrean.getTruckingCompany() != null) {
                landsideCarrierId = inPrean.getTruckingCompany().getBzuId();
            }
        }

        if (landsideCarrierId != null) {
            registerError("Landside Carrier Id is unavailable");
        } else {
            Element landsideCarrierElement = new Element("ediAckLandsideCarrierId", XmlUtil.ARGO_NAMESPACE);
            landsideCarrierElement.setText(landsideCarrierId);
            inTranElement.addContent(landsideCarrierElement);
        }


    }

    private void addTvapptElement(Element inAckElement, TruckVisitAppointment inTvAppt) {

        Element ediTarElement = new Element("ediAckTvappt", XmlUtil.ARGO_NAMESPACE);

        //TAR nbr attribute
        //@todo fix mapping
        Attribute ediAckTvapptNbrAttribute = new Attribute("ediAckTvapptNbr", String.valueOf(inTvAppt.getTvapptReferenceNbr()), XmlUtil.ARGO_NAMESPACE);
        ediTarElement.setAttribute(ediAckTvapptNbrAttribute);

        //Slot Attributes
        AppointmentTimeSlot tvapptTimeSlot = inTvAppt.getTvapptTimeSlot();

        Attribute ediAckTvapptStartAttribute = new Attribute("ediAckTvapptStart", _dateFormat.format(tvapptTimeSlot.getAtslotStartDate()), XmlUtil.ARGO_NAMESPACE);
        ediTarElement.setAttribute(ediAckTvapptStartAttribute);

        Attribute ediAckTvapptEndAttribute = new Attribute("ediAckTvapptEnd", _dateFormat.format(tvapptTimeSlot.getAtslotEndDate()), XmlUtil.ARGO_NAMESPACE);
        ediTarElement.setAttribute(ediAckTvapptEndAttribute);


        inAckElement.addContent(ediTarElement);
    }

    private void processCancelledPrean(Element tranElement, Element inAckElement, GateAppointment prean) {

        Event cancelEvnt = (Event) _sm.getMostRecentEvent(EventEnum.APPT_CANCEL, prean);
        String evntNote = cancelEvnt.getEventNote();
        Attribute ediAckStatus;

        String msgType = STATUS_UPDATE.equals(prean.getField(_panFields.RESPONSE_MSG_TYPE)) ? STATUS_UPDATE_REQST : PREANNOUNCEMENT;

        String cancelledBySender = getLibrary("PANPreanUtils").getCancelNote(msgType, CANCELLED_BY_SENDER);

        if (cancelledBySender.equals(evntNote)) {

            addEdiAckStatus(tranElement, EDI_ACK_STATUS_APPROVED);
            addEdiAckErrorElement(inAckElement, "COK", null);
        } else {
            addEdiAckStatus(tranElement, EDI_ACK_STATUS_REJECTED);
            addEdiAckErrorElement(inAckElement, "CTV", evntNote);
        }


    }

    private void addEdiAckStatus(Element inTranElement, String inAckStatus) {
        Attribute ediAckStatus;
        ediAckStatus = new Attribute("ediAckStatus", inAckStatus, XmlUtil.ARGO_NAMESPACE);
        inTranElement.setAttribute(ediAckStatus);
    }

    private void processActivePrean(Element inTranElement, Element inAckElement, GateAppointment inPrean) {

        String preanStatus = (String) inPrean.getField(_panFields.PREAN_STATUS);

        addEdiAckStatus(inTranElement, EDI_ACK_STATUS_APPROVED);

        if (PREAN_STATUS_OK.equals(preanStatus)) {
            addEdiAckErrorElement(inAckElement, "COK", null);
        } else if (PREAN_STATUS_NOK.equals(preanStatus)) {

            addPreanValidationErrorsToAknXml(inAckElement, inPrean);

        } else if (PREAN_STATUS_OK_TOO_LATE.equals(preanStatus)) {

            addPreanValidationErrorsToAknXml(inAckElement, inPrean);

        }

    }

    private void addPreanValidationErrorsToAknXml(Element inAckElement, GateAppointment inPrean) {

        QueryResult qr = getErrs(inPrean.getApptGkey(), inPrean.getField(_panFields.PREAN_VALIDATION_RUN_ID));

        /*for (Map.Entry<String,String> err : errs.entrySet()) {
          addEdiAckErrorElement(inAckElement,err.getKey(),err.getValue());
        }*/

        for (int i = 0; i < qr.getCurrentResultCount(); i++) {
            ValueHolder errVao = qr.getValueHolder(i);
            addEdiAckErrorElement(inAckElement, errVao.getFieldValue(_preanErrorUtil.ERR_APERAK_CODE), errVao.getFieldValue(_preanErrorUtil.ERR_APERAK_MSG));
        }

    }

    private void addEdiAckErrorElement(Element inEdiAcknowledgement, String inErrCode, String inErrDesc) {

        Element ediAckError = new Element("ediAckError", XmlUtil.ARGO_NAMESPACE);

        Attribute ediAckErrCode = new Attribute("ediAckErrCode", inErrCode, XmlUtil.ARGO_NAMESPACE);
        if (inErrDesc != null) {

            String cleanedUpErrDesc = inErrDesc.replace("'", "");
            cleanedUpErrDesc = cleanedUpErrDesc.replace(":", "");
            cleanedUpErrDesc = cleanedUpErrDesc.replace("+", "");
            cleanedUpErrDesc = cleanedUpErrDesc.replace("_", " ");
            cleanedUpErrDesc = cleanedUpErrDesc.replace(" \"{0}\"", " ");

            Attribute ediAckErrDescription = new Attribute("ediAckErrDescription", cleanedUpErrDesc, XmlUtil.ARGO_NAMESPACE);
            ediAckError.setAttribute(ediAckErrDescription);
        }


        ediAckError.setAttribute(ediAckErrCode);


        inEdiAcknowledgement.addContent(ediAckError);
    }

    /**
     * Callback method for Edi Extract. Method gets called after EDI Map
     *
     * @param inParams -  Input Parameters
     */

    public String afterEdiMap(Map inParams) {

        log("afterEdiMap")
        String ediFile = (String) inParams.get("EDI_TRANSACTION");

        GateAppointment prean = (GateAppointment) inParams.get("PREAN");
        log("prean: " + prean)

        if (prean != null) {
            if (prean.getGapptGate() != null && prean.getGapptGate().getGateId().equals(RAIL_ITT_GATE)) {
                int startIndex = ediFile.indexOf("BGM+");
                int endIndex = ediFile.indexOf("+", startIndex + "BGM+".length());
                log("startIndex: " + startIndex + ", endIndex: " + endIndex);
                if (startIndex > 0 && endIndex > 0) {
                    String ediTranTypeCode = ediFile.substring(startIndex, endIndex);
                    log("ediTranTypeCode: " + ediTranTypeCode);
                    String tranCode = "660";
                    if (prean.isDelivery()) {
                        tranCode = "661";
                    }
                    ediFile = ediFile.replace((ediTranTypeCode + "+"), ("BGM+" + tranCode + "+"));
                }

                log("prean unit: " + prean.getGapptUnit());
                if (prean.getGapptUnit() != null) {
                    log("unitflexstr06: " + prean.getGapptUnit().getUnitFlexString06());
                    String rffCaoSegment = "RFF+CAO:";
                    String flexString06 = (prean.getGapptUnit().getUnitFlexString06() != null) ? prean.getGapptUnit().getUnitFlexString06() : null;
                    if (flexString06 != null) {
                        if (ediFile.indexOf(rffCaoSegment) < 0) {
                            if (ediFile.indexOf("NAD+MS") > 0) {
                                //ediFile = ediFile.replace("NAD+MS", (rffCaoSegment + prean.getGapptUnit().getUnitFlexString06() + "'\n" + "NAD+MS") );
                                ediFile = ediFile.replace("NAD+MS", (rffCaoSegment + flexString06 + "'\n" + "NAD+MS"));
                            }
                        } else {
                            //ediFile = ediFile.replace(rffCaoSegment, rffCaoSegment + prean.getGapptUnit().getUnitFlexString06());
                            ediFile = ediFile.replace(rffCaoSegment, rffCaoSegment + flexString06);
                        }
                    }
                }

            } else {
                log("eqNbr: " + prean.getEqNbr());
                ediFile = ediFile.replace("RFF+EQD", "RFF+EQD:" + prean.getEqNbr());
            }
        }

        return ediFile;
    }


    private GateAppointment findPreanByEditran(Long inEdiTranGkey) {

        DomainQuery dq = QueryUtils.createDomainQuery(RoadApptsEntity.GATE_APPOINTMENT)
                .addDqPredicate(PredicateFactory.eq(_panFields.EDI_TRAN_GKEY, inEdiTranGkey));

        List<GateAppointment> appointments = Roastery.getHibernateApi().findEntitiesByDomainQuery(dq);

        if (!appointments.isEmpty()) {
            if (appointments.size() == 1) {
                return appointments.get(0);
            } else {
                throw BizFailure.create("More then one prean with tran gkey exists");
            }

        }
        return null;
    }


    public Map getErrsOld(Long inPreanKey, Long inValidationRunId) {

        Map<String, String> errs = new HashMap();

        DomainQuery dq = QueryUtils.createDomainQuery(_preanErrorUtil.PREAN_VALIDATION_ERROR)
                .addDqField(_preanErrorUtil.ERR_APERAK_CODE)
                .addDqField(_preanErrorUtil.ERR_APERAK_MSG)
                .addDqPredicate(PredicateFactory.eq(_preanErrorUtil.PREAN_KEY, inPreanKey))
                .addDqPredicate(PredicateFactory.eq(_preanErrorUtil.VALIDATION_RUN_ID, inValidationRunId));

        QueryResult values = Roastery.getHibernateApi().findValuesByDomainQuery(dq);

        for (int i = 0; i < values.getCurrentResultCount(); i++) {
            ValueHolder vao = values.getValueHolder(i);
            errs.put(vao.getFieldValue(_preanErrorUtil.ERR_APERAK_CODE), vao.getFieldValue(_preanErrorUtil.ERR_APERAK_MSG));

        }
        return errs;
    }

    public QueryResult getErrs(Long inPreanKey, Long inValidationRunId) {

        Map<String, String> errs = new HashMap();

        DomainQuery dq = QueryUtils.createDomainQuery(_preanErrorUtil.PREAN_VALIDATION_ERROR)
                .addDqField(_preanErrorUtil.ERR_APERAK_CODE)
                .addDqField(_preanErrorUtil.ERR_APERAK_MSG)
                .addDqPredicate(PredicateFactory.eq(_preanErrorUtil.PREAN_KEY, inPreanKey))
                .addDqPredicate(PredicateFactory.eq(_preanErrorUtil.VALIDATION_RUN_ID, inValidationRunId));

        QueryResult values = Roastery.getHibernateApi().findValuesByDomainQuery(dq);

        return values;
    }


    private static ServicesManager _sm = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
    private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
    private static
    def _preanErrorUtil = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANPreanErrorUtil");

    //Prean Status Constants
    private static String PREAN_STATUS_OK = "OK";
    private static String PREAN_STATUS_NOK = "NOK";
    private static String PREAN_STATUS_OK_TOO_LATE = "OK_TOO_LATE";

    //Cancellation Note Constants
    private static String CANCELLED_BY_SENDER = "CANCELLED BY SENDER";
    private static String STATUS_UPDATE = "STATUS_UPDATE";
    private static String PREANNOUNCEMENT = "PREANNOUNCEMENT";
    private static String STATUS_UPDATE_REQST = "STATUS_UPDATE_REQST";

    //XML Message Constants
    private static String EDI_ACK_STATUS_APPROVED = "APPROVED";
    private static String EDI_ACK_STATUS_REJECTED = "REJECTED";
    private static String EDI_ACK_STATUS_ERRORS = "ERRORS";
    private static String RAIL_ITT_GATE = "RAIL_ITT";
    private static final Logger LOGGER = Logger.getLogger(this.class)

    SimpleDateFormat _dateFormat = new SimpleDateFormat("yyMMddHHmm");

}