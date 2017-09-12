/*
 * Copyright (c) 2015 Navis LLC. All Rights Reserved.
 *
 */


import com.navis.argo.*;
import com.navis.argo.BookingTransactionDocument.BookingTransaction;
import com.navis.argo.BookingTransactionDocument.BookingTransaction.EdiBookingItem;
import com.navis.argo.BookingTransactionsDocument.BookingTransactions;
import com.navis.argo.business.api.ArgoEdiUtils;
import com.navis.argo.business.api.ArgoUtils;
import com.navis.argo.business.api.ServicesManager;
import com.navis.argo.business.atoms.*;
import com.navis.argo.business.model.*;
import com.navis.argo.business.reference.*;
import com.navis.edi.business.edimodel.EdiConsts;
import com.navis.edi.business.entity.EdiSession;
import com.navis.edi.business.entity.EdiTradingPartner;
import com.navis.external.edi.entity.AbstractEdiPostInterceptor;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.persistence.HibernatingEntity;
import com.navis.framework.portal.Ordering;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.AggregateFunctionType;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.query.common.api.QueryResult;
import com.navis.framework.util.*;
import com.navis.framework.util.internationalization.PropertyKey;
import com.navis.framework.util.internationalization.PropertyKeyFactory;
import com.navis.framework.util.message.MessageCollector;
import com.navis.framework.util.message.MessageLevel;
import com.navis.inventory.InventoryEntity;
import com.navis.inventory.InventoryField;
import com.navis.inventory.InventoryPropertyKeys;
import com.navis.inventory.business.api.UnitField;
import com.navis.inventory.business.api.UnitFinder;
import com.navis.inventory.business.api.UnitManager;
import com.navis.inventory.business.atoms.UfvTransitStateEnum;
import com.navis.inventory.business.atoms.UnitVisitStateEnum;
import com.navis.inventory.business.imdg.HazardousGoods;
import com.navis.inventory.business.imdg.ImdgClass;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.orders.OrdersEntity;
import com.navis.orders.OrdersField;
import com.navis.orders.OrdersPropertyKeys;
import com.navis.orders.business.api.EquipmentOrderManager;
import com.navis.orders.business.api.OrdersFinder;
import com.navis.orders.business.atoms.EqTypeGroupKindEnum;
import com.navis.orders.business.eqorders.Booking;
import com.navis.orders.business.eqorders.BookingPosterPea;
import com.navis.orders.business.eqorders.EquipmentOrderItem;
import com.navis.orders.business.eqorders.EquipmentReceiveOrder;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.xmlbeans.XmlObject;
import org.codehaus.xfire.spring.SpringUtils;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.*;

/**
 * Created by kasinra on 18-05-2015.
 * Modified by - Pradeep Arya
 * WF#792214 - Make COPARN work for BOOKING/EDO/ERO with single EDI session
 * In case of ERO run everything through groovy otherwise it should run through N4 EDI engine
 * WF#686616 - Merged the change from BookingEdiPostInterceptor to this groovy to make hazardous fix work with
 * Bookings
 *
 * WeServe team : Added validate methods for POD, gross weight and Trading partner allowed to create bookings for
 * other line operators
 *
 */
public class ApmtEroEdiPostInterceptor extends AbstractEdiPostInterceptor {

    private static final String MSG_FUNCTION_ORIGINAL = "O";
    private static final Logger LOGGER = Logger.getLogger(ApmtEroEdiPostInterceptor.class);
    private static final String ERO_TYPE = "ERO";
    private static final String BOOKING_TYPE = "BOOKING";
    private static final String AGENCY_BIC = "BIC";
    private static final String AGENCY_SCAC = "SCAC";
    MessageCollector collector = getMessageCollector();

    private createBizFaiureMessage(PropertyKey inPropertyKey, Map inParams, String errorMessage){
        BizFailure bv;
        if(inPropertyKey){
            bv = BizFailure.create(inPropertyKey, null);
        }else if(errorMessage){
            bv = BizFailure.createProgrammingFailure(errorMessage)
        }

        ContextHelper.getThreadMessageCollector().appendMessage(bv);
        inParams.put(EdiConsts.SKIP_POSTER, Boolean.TRUE);
    }

    private static long getBkgItemMaxSeqNbr(Set<EquipmentOrderItem> inEqoiSet) {

        //step1: find max of seqNbr from eqoi (received in edi)
        Long ediMaxseqNbr = (long) 0;
        for (EquipmentOrderItem eqoi : inEqoiSet) {
            Long seqNbr = eqoi.getEqoiSeqNbr();
            if (seqNbr != null && seqNbr > ediMaxseqNbr) {
                ediMaxseqNbr = seqNbr;
            }
        }
        //step2: find max of seqNbr from eqoi table
        Long tableMaxseqNbr = (long) 0;
        DomainQuery dq = QueryUtils.createDomainQuery(OrdersEntity.EQUIPMENT_ORDER_ITEM)
                .addDqAggregateField(AggregateFunctionType.MAX, OrdersField.EQOI_SEQ_NBR);
        QueryResult qr = HibernateApi.getInstance().findValuesByDomainQuery(dq);
        if (qr.getTotalResultCount() > 0) {
            Object valueObj = qr.getValue(0, 0);
            if (valueObj != null) {
                tableMaxseqNbr = (Long) valueObj;
            }
        }
        //step3: return whichever is bigger.
        return tableMaxseqNbr > ediMaxseqNbr ? tableMaxseqNbr : ediMaxseqNbr;
    }

    public static int getMaxValue(int[] inNumbers) {
        int maxValue = inNumbers[0];
        for (int i = 1; i < inNumbers.length; i++) {
            if (inNumbers[i] > maxValue) {
                maxValue = inNumbers[i];
            }
        }
        return maxValue;
    }

    @Nullable
    private static ScopedBizUnit validateLineOperator(EdiOperator inEdiOperator, EdiVesselVisit inEdiVesselVisit) throws BizViolation {
        ScopedBizUnit inLine = getLineOperator(inEdiOperator, inEdiVesselVisit);
        if (inLine == null) {
            throw BizViolation.create(InventoryPropertyKeys.UNKNOWN_CONTAINER_OPERATOR, null);
        }
        return inLine;
    }

    private static ScopedBizUnit getLineOperator(EdiOperator inEdiOperator, EdiVesselVisit inEdiVesselVisit) {
        ScopedBizUnit lineOperator = null;
        String lineCode = null;
        String lineCodeAgency = null;
        if (inEdiOperator != null) {
            lineCode = inEdiOperator.getOperator();
            lineCodeAgency = inEdiOperator.getOperatorCodeAgency();
            LOGGER.debug("lineCode: "+lineCode);
            LOGGER.debug("lineCodeAgency: "+lineCodeAgency);

            lineOperator = ScopedBizUnit.resolveScopedBizUnit(lineCode, lineCodeAgency, BizRoleEnum.LINEOP);
            LOGGER.debug("lineOperator: "+lineOperator);
        }

        if (lineOperator == null && inEdiVesselVisit != null && inEdiVesselVisit.getShippingLine() != null) {
            lineCode = inEdiVesselVisit.getShippingLine().getShippingLineCode();
            lineCodeAgency = inEdiVesselVisit.getShippingLine().getShippingLineCodeAgency();
            lineOperator = ScopedBizUnit.resolveScopedBizUnit(lineCode, lineCodeAgency, BizRoleEnum.LINEOP);
        }

        return lineOperator;

    }

    private static EquipmentOrderManager getEquipmentOrderManager() {
        return (EquipmentOrderManager) Roastery.getBean(EquipmentOrderManager.BEAN_ID);
    }

    private
    static EquipmentOrderItem findEroOrderItem(EquipmentReceiveOrder inEro, EquipType inEqType) throws BizViolation {
        EquipmentOrderItem eqoi = null;
        try {
            eqoi = inEro.findMatchingItemDsp(inEqType, false, false);
            if (eqoi != null) {
                eqoi.validateEmptyDispatch(true, true);
            }
        } catch (BizViolation inBv) {
            if (OrdersPropertyKeys.ERRKEY__CANNOT_DELIVER_CONTAINERS_BOOKING_QTY_MET.equals(inBv.getMessageKey())
                    || OrdersPropertyKeys.ERRKEY__CANNOT_DELIVER_CONTAINERS_BOOKING_RECEIVE_QTY_MET.equals(inBv.getMessageKey())) {
                getEquipmentOrderManager().updateEqoiQuantity(eqoi, true, eqoi.getEqoiQty() + 1);
            } else if (!OrdersPropertyKeys.ERRKEY__MATCHING_EQOI_NOT_FOUND.equals(inBv.getMessageKey())) {
                throw inBv;
            }
        }
        return eqoi;
    }

    private static Facility findFacility(EdiFacility inEdiFacility) throws BizViolation {
        Facility facility = null;
        if (inEdiFacility != null) {
            String facilityId = inEdiFacility.getFacilityId();
            if (!StringUtils.isEmpty(facilityId)) {
                Complex cpx = ContextHelper.getThreadComplex();
                facility = Facility.findFacility(facilityId, cpx);
                if (facility == null) {
                    throw BizViolation.create(ArgoPropertyKeys.INVALID_FACILITY_ID, null, facilityId, cpx.getCpxId());
                }
            }
        }
        return facility;
    }

    @Override
    public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
        //public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams, Serializable inSessionGkey) {
        TimeZone timeZone = ContextHelper.getThreadUserTimezone();
        Date timeNow = ArgoUtils.convertDateToLocalDateTime(ArgoUtils.timeNow(), timeZone);
        LOGGER.setLevel(Level.DEBUG);
        LOGGER.debug("ApmtEroEdiPostInterceptor.beforeEdiPost()--");
        log("ApmtEroEdiPostInterceptor.beforeEdiPost() Starts " + timeNow);

        //Added by Pradeep to make it work for all orders
        if (inXmlTransactionDocument == null || !BookingTransactionsDocument.class.isAssignableFrom(inXmlTransactionDocument.getClass())) {
            LOGGER.error("inXmlTransactionDocument is not of type BookingTransactionsDocument");
            return;
        }
        EdiPostingContext postingContext = ContextHelper.getThreadEdiPostingContext();
        BookingTransactionsDocument bkgTransDocument = (BookingTransactionsDocument) inXmlTransactionDocument;
        BookingTransactions bkgTrans = bkgTransDocument.getBookingTransactions();
        BookingTransaction[] bkgArray = bkgTrans.getBookingTransactionArray();

        if (bkgArray == null || bkgArray.length == 0) {
            LOGGER.error("Booking transaction Array is NULL in after EDI post method");
            return;
        }

        //hazardous validation added here for the bookings
        BookingTransaction bkgTransaction = bkgArray[0];
        List<com.navis.argo.BookingTransactionDocument.BookingTransaction.EdiBookingEquipment> ediBkEqList = bkgTransaction.getEdiBookingEquipmentList();
        //Added by weserve to throw an error if there is no POD or Grossweight in the EDI message
		List<com.navis.argo.BookingTransactionDocument.BookingTransaction.EdiBookingItem> ediBkItemList = bkgTransaction.getEdiBookingItemList();
        try {
            validatePODandGrossweight(bkgTransaction, inParams);
        } catch (BizViolation inBizViolation) {
            TransactionParms transactionParms = TransactionParms.getBoundParms();
            transactionParms.getMessageCollector().registerExceptions(inBizViolation);
        }


        //validateTradingPartner(bkgTransaction, inSessionGkey);
        Serializable sessionGkey = (ContextHelper.getThreadEdiPostingContext()!=null)? ContextHelper.getThreadEdiPostingContext().getSessionGkey() : null;
        validateTradingPartner(bkgTransaction, sessionGkey, inParams);

        if (BOOKING_TYPE.equalsIgnoreCase(bkgTransaction.getEdiBookingType())) {
            this.processEdiBookingHazards(bkgTransaction);
            //CSDV-824 Process hazardous updates at ediBookingEquipment element
            if (ediBkEqList != null && ediBkEqList.size() > 0) {

                this.processEdiBookingEquipmentHazards(ediBkEqList);
            }
			if(ediBkItemList != null && ediBkItemList.size() > 0){
				this.processEdiBookingItemHazards(ediBkItemList);
			}
        } else if (ERO_TYPE.equalsIgnoreCase(bkgTransaction.getEdiBookingType())) {
            inParams.put(EdiConsts.SKIP_POSTER, Boolean.TRUE);
        }

        timeNow = ArgoUtils.convertDateToLocalDateTime(ArgoUtils.timeNow(), timeZone);
        log("ApmtEroEdiPostInterceptor.beforeEdiPost(): execution  ends " + timeNow);
    }

    @Override
    public void afterEdiPost(XmlObject inXmlTransactionDocument, HibernatingEntity inHibernatingEntity, Map inParams) {
        LOGGER.debug("after edi post")
        if (inXmlTransactionDocument == null || !BookingTransactionsDocument.class.isAssignableFrom(inXmlTransactionDocument.getClass())) {
            LOGGER.error("inXmlTransactionDocument is not of type BookingTransactionsDocument");
            return;
        }
        EdiPostingContext postingContext = ContextHelper.getThreadEdiPostingContext();
        BookingTransactionsDocument bkgTransDocument = (BookingTransactionsDocument) inXmlTransactionDocument;
        BookingTransactions bkgTrans = bkgTransDocument.getBookingTransactions();
        BookingTransaction[] bkgArray = bkgTrans.getBookingTransactionArray();

        if (bkgArray == null || bkgArray.length == 0) {
            LOGGER.error("Booking transaction Array is NULL in after EDI post method");
            return;
        }

        BookingTransaction bkgTransaction = bkgArray[0];

        if (ERO_TYPE.equalsIgnoreCase(bkgTransaction.getEdiBookingType())) {
            try {
                postERO(postingContext, bkgTransaction);
            } catch (BizViolation inBizViolation) {
                TransactionParms transactionParms = TransactionParms.getBoundParms();
                transactionParms.getMessageCollector().registerExceptions(inBizViolation);
            }
            log("postToDomainModel: imported  EDI for EDO : ");
        }
    }
	
	private void processEdiBookingItemHazards(List<com.navis.argo.BookingTransactionDocument.BookingTransaction.EdiBookingItem> inEdiBkItemList){
		LOGGER.setLevel(Level.DEBUG);
		LOGGER.debug("Inside edi BookingItem Hazards")
		bookingItemHazardList.clear();
		if(inEdiBkItemList !=null && inEdiBkItemList.size() > 1){
			for(EdiBookingItem ediBookingItem : inEdiBkItemList){
				//set the ediHazards from the list available only once.
				LOGGER.debug("Inside edi BookingItem Hazards :: Size of common hazard List Before :: "+bookingItemHazardList.size())
				if(ediBookingItem.getEdiHazardList() != null && ediBookingItem.getEdiHazardList().size()>0 && bookingItemHazardList.size() == 0 ){
					bookingItemHazardList.addAll(ediBookingItem.getEdiHazardList());
				}
				LOGGER.debug("Inside edi BookingItem Hazards :: Size of common hazard List After :: "+bookingItemHazardList.size())
			}
			for(EdiBookingItem ediBookingItem : inEdiBkItemList){
				for(EdiHazard masterHazard : bookingItemHazardList){
					//LOGGER.debug("Inside edi BookingItem Hazards :: Current Hazard :: "+masterHazard + " with label as :: "+masterHazard.getHazardLabel1() + " against :: "+ediBookingItem.getSequenceNumber())
					if(masterHazard.getHazardLabel1() != null && masterHazard.getHazardLabel1().equalsIgnoreCase(ediBookingItem.getSequenceNumber())){
						LOGGER.debug("Size of current Hazards :: "+ediBookingItem.getEdiHazardList().size());
						if(ediBookingItem.getEdiHazardList().size() == 0){
							ediBookingItem.addNewEdiHazard();
							ediBookingItem.getEdiHazardList().set(0, masterHazard);
						}else{
							for(int i=0; i < ediBookingItem.getEdiHazardList().size(); i++){
								if(!ediBookingItem.getEdiHazardList().get(i).equals(masterHazard)){
									ediBookingItem.getEdiHazardList().remove(i);
								}
							}
						}
						LOGGER.debug("Size of current Hazards After calculation :: "+ediBookingItem.getEdiHazardList().size());
						break;
					}
				}
			}
		}
	}

    private EquipmentReceiveOrder postERO(EdiPostingContext inPc, BookingTransaction inBkgTrans) throws BizViolation {

        // check for the line
        final EdiOperator ediOperator = inBkgTrans.getLineOperator();
        final EdiVesselVisit ediVesselVisit = inBkgTrans.getEdiVesselVisit();

        ScopedBizUnit line = validateLineOperator(ediOperator, ediVesselVisit);
        Facility eroFacility = findFacility(inBkgTrans.getRelayFacility());
        if (eroFacility == null) {
            throw BizViolation.create(PropertyKeyFactory.valueOf("Facility is manadary for posting ERO"), null);
        }
        inPc.setFacility(eroFacility);
        String msgFunction = inBkgTrans.getMsgFunction();
        if (ArgoEdiUtils.isNotEmpty(MSG_FUNCTION_ORIGINAL) && !MSG_FUNCTION_ORIGINAL.equals(msgFunction)) {
            throw BizViolation
                    .create(PropertyKeyFactory.valueOf("Currently ApmtEroEdiPostInterceptor does not accept Message function[" + msgFunction +
                            "]. Only Original message function alone is supported"), null);
        }
        // Create or Replace the Edo Details
        EquipmentReceiveOrder ero = createOrReplaceEroDetails(inPc, inBkgTrans, line, msgFunction, eroFacility);

        return ero;
    }

    public void validatePODandGrossweight(BookingTransaction bkgTransaction, Map inParams) throws BizViolation {
        LOGGER.debug("In validatePODandGrossweight")
        BookingTransaction.EdiBookingItem[] bkgitemArray = bkgTransaction.getEdiBookingItemArray();
        EdiBooking inEdiBooking = bkgTransaction.getEdiBooking();
        String edibookingNbr = inEdiBooking.getBookingNbr();
        EdiOperator ediOperator = bkgTransaction.getLineOperator();
        EdiVesselVisit ediVesselVisit = bkgTransaction.getEdiVesselVisit();
        BookingPosterPea bookingPosterPea = new BookingPosterPea();
        Port dichargePort = bkgTransaction.getDischargePort1();
        LOGGER.debug("Dichargeport: "+dichargePort)
        if (dichargePort == null || dichargePort.getPortId() == null) {
            //collector.appendMessage(BizViolation.create(AllOtherFrameworkPropertyKeys.ERROR__NULL_MESSAGE, null, "POD not found."));
            createBizFaiureMessage(null, inParams, "POD not found");
        }
        ScopedBizUnit line = validateLineOperator(ediOperator, ediVesselVisit);
        String msgFunction = bkgTransaction.getMsgFunction();
        for (int count = 0; count < bkgitemArray.length; count++) {
            try {
                String eqIso = bkgitemArray[count].getISOcode();
                if (ArgoEdiUtils.isEmpty(eqIso)) {
                    continue;
                }
                EquipType eqType = findActiveEquipType(eqIso);
                if (eqType == null) {
                    throw BizViolation.create(InventoryPropertyKeys.CAN_NOT_FIND_EQUIP_TYPE, null, eqIso);
                }
                Double eqTypeSafeWeight = eqType.getEqtypSafeWeightKg();
                Booking booking = Booking.findBookingWithoutVesselVisit(edibookingNbr, line);
                String bkgitemGrossWeight = bkgitemArray[count].getGrossWeight();
                if (bkgitemGrossWeight == null) {
                    //collector.appendMessage(BizViolation.create(AllOtherFrameworkPropertyKeys.ERROR__NULL_MESSAGE, null, "gross weight not found."));
                    createBizFaiureMessage(null, inParams, "Gross weight not found");
                }

                String grossWeightUnit = bkgitemArray[count].getGrossWtUnit();
                Double grossWt;
                try {
                    grossWt = bookingPosterPea.convertWtToKg(bkgitemGrossWeight, grossWeightUnit, InventoryField.UNIT_GOODS_AND_CTR_WT_KG);
                } catch (BizViolation inBizViolation) {
                    collector.appendMessage( BizViolation.create(InventoryPropertyKeys.EDI_BOOKING_UNKNOWN_WEIGHT_UNIT, (BizViolation)null, grossWeightUnit, InventoryField.UNIT_GOODS_AND_CTR_WT_KG, inBizViolation));
                }
                if(grossWt>eqTypeSafeWeight){
                    //collector.appendMessage(BizViolation.create(AllOtherFrameworkPropertyKeys.ERROR__NULL_MESSAGE, null, "Gross weight exceeds safeweight."));
                    createBizFaiureMessage(null, inParams, "Gross weight exceeds safeweight");
                }
            }
            catch (Exception e) {
                LOGGER.warn(" error in finding booking eqo" + e);

            }
        }

    }

    public void validateTradingPartner(BookingTransaction inBookingTransaction, Serializable inSessionGkey, Map inParams) {
        LOGGER.debug("In validateTradingPartner")
        EdiSession session = (EdiSession) HibernateApi.getInstance().load(EdiSession.class, inSessionGkey);
        EdiTradingPartner tradingPartner = session.getEdisessTradingPartner();
        ScopedBizUnit tradingPartnerBiz = tradingPartner.getEdiptnrBusinessUnit();
        Boolean isValidPartner = Boolean.TRUE;

        LOGGER.debug(" trading partner:::"+tradingPartner);
        if (tradingPartner == null) {
            isValidPartner = Boolean.FALSE;
        } else {
            //String partnerId = tradingPartnerBiz.getBzuId();
            String partnerId = tradingPartner.getEdiptnrName();
            EdiOperator ediOperator = inBookingTransaction.getLineOperator();
            EdiVesselVisit ediVesselVisit = inBookingTransaction.getEdiVesselVisit();
            ScopedBizUnit lineOp = getLineOperator(ediOperator, ediVesselVisit);
            if (lineOp != null && !lineOp.equals(tradingPartnerBiz)) {
                LOGGER.debug("lineOp: "+lineOp.getBzuId() + ", tradingPartnerBiz: "+tradingPartnerBiz.getBzuId() + ", partnerId: "+partnerId)
                GeneralReference tradingPartnerGR = GeneralReference.findUniqueEntryById("EDI BOOKING", partnerId);
                LOGGER.debug("tradingPartnerGR: "+tradingPartnerGR);
                if (tradingPartnerGR == null) {
                    isValidPartner = Boolean.FALSE;
                } else {
                    String lineId = lineOp.getBzuId();
                    LOGGER.debug("lineid inside trading partner"+lineId);
                    if ( !(tradingPartnerGR &&((tradingPartnerGR.getRefValue1()!= null && tradingPartnerGR.getRefValue1().contains(lineId))
                        || (tradingPartnerGR.getRefValue2()!= null && tradingPartnerGR.getRefValue2().contains(lineId))
                    || (tradingPartnerGR.getRefValue3()!= null && tradingPartnerGR.getRefValue3().contains(lineId)) ))) {
                        isValidPartner = Boolean.FALSE;
                    }
                }
            }
        }

        if (!isValidPartner) {
            //collector.appendMessage(BizViolation.create(OrdersPropertyKeys.ERRKEY__LINE_DOESNT_SUPPORT_BOOKINGS, null, "Please configure trading partner in General Reference."));
            createBizFaiureMessage(null, inParams, "Trading partner should match with LineOperator, else use 'EDI BOOKING' general reference to configure");
        }

    }

    private EquipmentReceiveOrder createOrReplaceEroDetails(EdiPostingContext inPc, BookingTransaction inBkgTrans, ScopedBizUnit inLine,
                                                            String inMsgFunction, Facility inFacility)
            throws BizViolation {

        EquipmentReceiveOrder ero = createOrUpdateEro(inBkgTrans, inLine, inMsgFunction);
        if (inFacility != null) {
            ero.setFieldValue(OrdersField.EQO_FACILITY, inFacility);
        }

        Set eqoiSet = new HashSet();
        BookingTransaction.EdiBookingItem[] bkgitemArray = inBkgTrans.getEdiBookingItemArray();
        if (bkgitemArray != null && bkgitemArray.length > 0) {
            eqoiSet = createOrUpdateEqOrderItems(inPc, bkgitemArray, ero);
        }

        BookingTransaction.EdiBookingEquipment[] bkgEqArray = inBkgTrans.getEdiBookingEquipmentArray();
        if (bkgEqArray != null && bkgEqArray.length > 0) {
            eqoiSet.addAll(createOrUpdateEroUnits(inPc, bkgEqArray, ero, inFacility));
        }

        return ero;
    }

    private EquipmentReceiveOrder createOrUpdateEro(BookingTransaction inBkgTrans, ScopedBizUnit inLine, String inMsgFunction)
            throws BizViolation {
        String inEqoNbr = null;
        if (inBkgTrans.getEdiBooking() != null) {
            inEqoNbr = inBkgTrans.getEdiBooking().getBookingNbr();
        }
        if (ArgoEdiUtils.isEmpty(inEqoNbr)) {
            throw BizViolation.create(OrdersPropertyKeys.ERRKEY__MISSING_ORDER_NUMBER, null);
        }
        EquipmentReceiveOrder ero = EquipmentReceiveOrder.findEroByUniquenessCriteria(inEqoNbr, inLine);
        if (ero != null && MSG_FUNCTION_ORIGINAL.equals(inMsgFunction)) {
            throw BizViolation.create(PropertyKeyFactory
                    .valueOf("ERO already exist with Nbr[" + inEqoNbr + "]. override is not allowed through code extension"), null);
        }
        if (ero == null) {
            ero = EquipmentReceiveOrder.findOrCreateEquipmentReceiveOrder(inEqoNbr, inLine, FreightKindEnum.MTY);
        }

        return updateERO(inBkgTrans, ero);
    }

    @Nullable
    private EquipmentReceiveOrder updateERO(BookingTransaction inBkgTrans, EquipmentReceiveOrder inEro) throws BizViolation {
        if (inEro != null) {
            TimeZone tz = ContextHelper.getThreadUserTimezone();
            BookingAgent bkgAgent = inBkgTrans.getEdiBooking().getEdiBookingAgent();
            if (bkgAgent != null) {
                ScopedBizUnit bookingAgent = findOrCreateBookingAgent(bkgAgent);
                inEro.setFieldValue(OrdersField.EQO_AGENT, bookingAgent);
            }
            String refnbr = inBkgTrans.getEdiBooking().getExportReferenceNbr();
            String notes = inBkgTrans.getEdiBooking().getBookingHandlingInstructions();
            inEro.updateNotesAndClientRef(notes, refnbr);
            Object latestDeliveryDt = inBkgTrans.getEdiBooking().getLatestDeliveryDate();
            if (latestDeliveryDt instanceof Calendar) {
                inEro.setFieldValue(OrdersField.EQO_LATEST_DATE, ArgoEdiUtils.convertLocalToUtcDate(latestDeliveryDt, tz));
            }

            Object earliestDeliveryDt = inBkgTrans.getEdiBooking().getEarliestDeliveryDate();
            if (earliestDeliveryDt instanceof Calendar) {
                inEro.setFieldValue(OrdersField.EQO_EARLIEST_DATE, ArgoEdiUtils.convertLocalToUtcDate(earliestDeliveryDt, tz));
            }

            Object estimatedDeliveryDt = inBkgTrans.getEdiBooking().getEstimatedDeliveryDate();
            if (estimatedDeliveryDt instanceof Calendar) {
                inEro.setFieldValue(OrdersField.EQO_ESTIMATED_DATE, ArgoEdiUtils.convertLocalToUtcDate(estimatedDeliveryDt, tz));
            }
        }
        return inEro;
    }

    private Set createOrUpdateEroUnits(EdiPostingContext inPc, BookingTransaction.EdiBookingEquipment[] inBkgEqArray, EquipmentReceiveOrder inEro,
                                       Facility inFacility)
            throws BizViolation {
        Set eqoiSet = new HashSet();
        BizViolation bv = null;

        for (int count = 0; count < inBkgEqArray.length; count++) {
            try {
                EdiContainer ediContainer = inBkgEqArray[count].getEdiContainer();
                String eqIso = ediContainer.getContainerISOcode();
                // Find or create  the BookingItem with respect to Container type and Booking
                if (ArgoEdiUtils.isEmpty(eqIso)) {
                    continue;
                }
                EquipType eqType = findActiveEquipType(eqIso);
                if (eqType == null) {
                    throw BizViolation.create(InventoryPropertyKeys.CAN_NOT_FIND_EQUIP_TYPE, null, eqIso);
                }

                // Find the eqoi and if found update the quantity
                EquipmentOrderItem eqoi = findEroOrderItem(inEro, eqType);
                Long qty = safeGetLong(inBkgEqArray[count].getQuantity(), OrdersField.EQOI_QTY);

                if (eqoi != null) {
                    if (qty != null) {
                        eqoi.setFieldValue(OrdersField.EQOI_QTY, qty);
                        HibernateApi.getInstance().update(eqoi);
                    }
                } else {
                    //else create new eqoi
                    if (qty == null) {
                        qty = (long) 1;
                    }
                    eqoi = EquipmentOrderItem.createOrderItem(inEro, qty, eqType);
                }

                String eqId = ediContainer.getContainerNbr();
                if (eqId != null && StringUtils.isEmpty(eqId.trim())) {
                    throw BizViolation.createFieldViolation(ArgoPropertyKeys.CONTAINER_ID_REQUIRED, null, null);
                }

                // Find or create a container with respect to Id and IsoCode(here it means EqtypId)
                Container container = Container.findContainer(eqId);
                Unit unit = null;
                if (container != null) {
                    container.setFieldValue(ArgoRefField.EQ_RFR_TYPE, eqType.getEqtypRfrType());
                    unit = findEroUnit(inEro, container, inFacility);
                }

                if (unit == null) {
                    unit = createPreadvisedStorageEmptyUnitForEro(eqoi, eqId, inFacility);
                    HibernateApi.getInstance().flush();
                }
                UnitFacilityVisit ufv = null;

                UnitVisitStateEnum unitVisitState = unit.getUnitVisitState();
                if (!UnitVisitStateEnum.ADVISED.equals(unitVisitState)) {// Departed
                    //Add Warning and ignore container update
                    throw BizViolation.create(PropertyKeyFactory.valueOf("ERO is posted for non advised unit" + unit.getUnitId()), null);
                } else {
                    if ((ufv == null) && (inFacility != null)) {
                        ufv = unit.getUfvForFacilityNewest(inFacility);
                    }
                    eqoi.reserveEq(Equipment.findEquipment(eqId), inFacility);

                    eqoiSet.add(eqoi);
                }
            } catch (Exception e) {
                LOGGER.error(e);
                if (CarinaUtils.unwrap(e) instanceof BizViolation) {
                    BizViolation inBv = (BizViolation) CarinaUtils.unwrap(e);
                    // EDO should update even if unit is already in yard so making this BizViolation to warning
                    if (inBv.getMessageKey().equals(OrdersPropertyKeys.PREADVISE_CONTAINER_ALREADY_RECEIVED)) {
                        inBv.setSeverity(MessageLevel.WARNING);
                        getMessageCollector().appendMessage(inBv);
                    }
                    bv = inBv.appendToChain(bv);
                } else {
                    BizViolation inBv = BizViolation.create(OrdersPropertyKeys.CREATE_OR_UPDATE_BKG_ITEMS_FAILED, null,
                            CarinaUtils.getStackTrace(e));
                    bv = inBv.appendToChain(bv);
                }
            }
        }
        if (bv != null) {
            throw bv;
        }
        updateBkgItemWithSeqNbr(inPc, eqoiSet);

        return eqoiSet;
    }

    public Unit createPreadvisedStorageEmptyUnitForEro(EquipmentOrderItem inEqoi, String inCtrId, Facility inFacility) throws BizViolation {

        ScopedBizUnit lineOp = (ScopedBizUnit) inEqoi.getEqboiOrder().getField(OrdersField.EQO_LINE);
        EquipType eqtyp = inEqoi.getEqoiSampleEquipType();
        if (eqtyp == null) {
            eqtyp = EquipType.findEquipType(inEqoi.getEqoiEqHeight(), inEqoi.getEqoiEqIsoGroup(), inEqoi.getEqoiEqSize(), EquipClassEnum.CONTAINER);
        }
        Equipment eq = Equipment.findEquipment(inCtrId);
        if (eq != null) {
            inEqoi.validateEquipmentConformsToOrderItem(eq, EqTypeGroupKindEnum.RECEIVE);
            // to avoid eqtyp being over written for known equipment.
            eqtyp = eq.getEqEquipType();
            UnitFinder finder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
            UnitFacilityVisit preAdvUfv = finder.findPreadvisedUnit(inFacility, eq);
            if (preAdvUfv != null && UnitCategoryEnum.STORAGE.equals(preAdvUfv.getUfvUnit().getUnitCategory())) {
                throw BizViolation.create(OrdersPropertyKeys.ERRKEY__ALREADY_PREADVISED, null, null);
            }
        }
        Complex complex = inFacility.getFcyComplex();

        CarrierVisit ibTruckVisit = CarrierVisit.getGenericTruckVisit(complex);

        UnitFacilityVisit ufv = getUnitManager().findOrCreatePreadvisedUnit(inFacility, inCtrId, eqtyp, UnitCategoryEnum.STORAGE, FreightKindEnum.MTY,
                lineOp, ibTruckVisit, CarrierVisit.getGenericCarrierVisit(complex), DataSourceEnum.EDI_BKG, null, null, null, null, null, false);

        // Populate response
        Unit unit = ufv.getUfvUnit();
        // Check for Quantity Reached
        Boolean isQtyReached = inEqoi.isPreadvisedAndReceiveLimitReached();
        if (isQtyReached) {
            throw BizViolation.create(OrdersPropertyKeys.ERRKEY__TRYING_TO_PREADVISE_WITH_EXCEEDED_QTY, null, null);
        }

        inEqoi.preadviseUnit(unit);
        ServicesManager srvcMgr = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
        srvcMgr.recordEvent(EventEnum.UNIT_PREADVISE, null, null, null, unit);
        return unit;
    }

    private Set createOrUpdateEqOrderItems(EdiPostingContext inPc, BookingTransaction.EdiBookingItem[] inBkgitemArray, EquipmentReceiveOrder inEro)
            throws BizViolation {
        BizViolation bv = null;
        Set eqoiSet = new HashSet();
        for (int count = 0; count < inBkgitemArray.length; count++) {
            try {
                String eqIso = inBkgitemArray[count].getISOcode();
                if (ArgoEdiUtils.isEmpty(eqIso)) {
                    continue;
                }
                EquipType eqType = findActiveEquipType(eqIso);
                if (eqType == null) {
                    throw BizViolation.create(InventoryPropertyKeys.CAN_NOT_FIND_EQUIP_TYPE, null, eqIso);
                }

                // Find the eqoi and if found update the quantity
                OrdersFinder ordersFinder = (OrdersFinder) Roastery.getBean(OrdersFinder.BEAN_ID);
                String ediSeqNbr = inBkgitemArray[count].getSequenceNumber();
                Long seqNumber = safeGetLong(ediSeqNbr, OrdersField.EQOI_SEQ_NBR);
                EquipmentOrderItem eqoi;
                if (seqNumber == null) {
                    eqoi = ordersFinder.findEqoItemByEqType(inEro, eqType);
                } else {
                    eqoi = EquipmentOrderItem.findEqoItemBySequenceNbr(inEro, seqNumber);
                    if (eqoi != null && eqType != null) {
                        updateEqType(eqoi, eqType);
                    }
                }
                Long qty = safeGetLong(inBkgitemArray[count].getQuantity(), OrdersField.EQOI_QTY);
                if (qty == null) {
                    BizViolation warning = BizWarning.create(OrdersPropertyKeys.ERRKEY_EQOI_QTY_IS_REQUIRED, null);
                    LOGGER.warn(warning);

                    qty = new Long(0);
                }

                if (eqoi != null) {
                    eqoi.setFieldValue(OrdersField.EQOI_QTY, qty);
                    HibernateApi.getInstance().save(eqoi);
                } else {
                    //else create new eqoi
                    eqoi = EquipmentOrderItem.createOrderItem(inEro, qty, eqType);
                }
                // Set the Equipment Order Item sequence number
                if (seqNumber != null) {
                    eqoi.setFieldValue(OrdersField.EQOI_SEQ_NBR, seqNumber);
                }
                updateEqoiRemarks(inBkgitemArray[count], eqoi);
                eqoiSet.add(eqoi);
            } catch (Exception e) {
                if (CarinaUtils.unwrap(e) instanceof BizViolation) {
                    BizViolation inBv = (BizViolation) CarinaUtils.unwrap(e);
                    bv = inBv.appendToChain(bv);
                }
            }
        }

        if (bv != null) {
            throw bv;
        }
        updateBkgItemWithSeqNbr(inPc, eqoiSet);
        return eqoiSet;
    }

    private void updateBkgItemWithSeqNbr(EdiPostingContext inPc, Set<EquipmentOrderItem> inEqoiSet) {
        if (inEqoiSet != null && !inEqoiSet.isEmpty()) {
            if (ArgoEdiUtils.getBooleanConfigValue(inPc.getConfigValue(ArgoConfig.BKG_AUTO_GENERATE_BKGITEM_SEQ_NBR))) {
                Long maxSeqNbr = null;
                for (EquipmentOrderItem eqoi : inEqoiSet) {
                    if (eqoi.getEqoiSeqNbr() == null) {
                        if (maxSeqNbr == null) {
                            maxSeqNbr = getBkgItemMaxSeqNbr(inEqoiSet);
                        }
                        eqoi.setFieldValue(OrdersField.EQOI_SEQ_NBR, ++maxSeqNbr);
                    }
                }
            }
        }
    }

    private Unit findEroUnit(EquipmentReceiveOrder inEro, Container inContainer, Facility inFacility) {
        Unit unit = null;
        Collection units = getUnitFinder().findUnitsAdvisedOrReceivedForOrder(inEro);
        if (units != null && !units.isEmpty()) {
            for (Iterator it = units.iterator(); it.hasNext();) {
                unit = (Unit) it.next();
                if (unit.getUnitPrimaryUe().getUeEquipment().equals(inContainer)) {
                    return unit;
                }
            }
            unit = null;
        }
        UnitFacilityVisit ufv = findUfv(inContainer, inFacility, UnitCategoryEnum.STORAGE);
        if (ufv != null) {
            unit = ufv.getUfvUnit();
        }
        return unit;
    }

    private void updateEqoiRemarks(BookingTransaction.EdiBookingItem inEdiBookingItem, EquipmentOrderItem inEqoi) {
        String remarks = inEdiBookingItem.getRemarks();
        if (ArgoEdiUtils.isNotEmpty(remarks)) {
            inEqoi.updateRemarks(remarks);
        }
    }

    private void updateEqType(EquipmentOrderItem inItem, EquipType inEqType) {
        inItem.setFieldValue(OrdersField.EQOI_EQ_HEIGHT, inEqType.getEqtypNominalHeight());
        inItem.setFieldValue(OrdersField.EQOI_EQ_ISO_GROUP, inEqType.getEqtypIsoGroup());
        inItem.setFieldValue(OrdersField.EQOI_EQ_SIZE, inEqType.getEqtypNominalHeight());
        inItem.setFieldValue(OrdersField.EQOI_SAMPLE_EQUIP_TYPE, inEqType);
    }

    @Nullable
    private Agent findOrCreateBookingAgent(BookingAgent inEdiBookingAgent) {
        String agentId;
        String agentName;
        String agentCode;
        String agentCodeAgency;
        Agent agent = null;
        if (inEdiBookingAgent != null) {
            agentId = inEdiBookingAgent.getAgentId();
            agentName = inEdiBookingAgent.getAgentName();
            agentCode = inEdiBookingAgent.getAgentCode();
            agentCodeAgency = inEdiBookingAgent.getAgentCodeAgency();
            ScopedBizUnit bzu = ScopedBizUnit.resolveScopedBizUnit(agentId, agentName, agentCode, agentCodeAgency, BizRoleEnum.AGENT);
            if (bzu != null) {
                agent = (Agent) HibernateApi.getInstance().downcast(bzu, Agent.class);
            } else {
                if (agentId != null || agentName != null) {
                    if (agentId == null) {
                        agentId = agentName;
                    }
                    agent = Agent.createAgent(agentId);
                    agentName = trimIfMaxLengthExceeded(agentName, "AgentName", 80);
                    agent.setFieldValue(ArgoRefField.BZU_NAME, agentName);
                    if (AGENCY_SCAC.equals(agentCodeAgency)) {
                        agent.setFieldValue(ArgoRefField.BZU_SCAC, agentCode);
                    } else if (AGENCY_BIC.equals(agentCodeAgency)) {
                        agent.setFieldValue(ArgoRefField.BZU_BIC, agentCode);
                    }
                    BizViolation warning = BizWarning.create(OrdersPropertyKeys.EDI_BOOKING_AGENT_CREATED, null, agentId);
                    LOGGER.warn(warning);
                }
            }
        }
        return agent;
    }

    protected String trimIfMaxLengthExceeded(String inId, String inFieldName, int inMaxLength) {
        if (ArgoEdiUtils.isNotEmpty(inId)) {
            if (inId.length() > inMaxLength) {
                String trimmedValue = inId.substring(0, inMaxLength);
                BizViolation warning = BizWarning.create(InventoryPropertyKeys.TRIMMED_THE_VALUE_TO_MAX_LENGTH, null, inFieldName, inMaxLength);
                LOGGER.warn(warning);
                return trimmedValue;
            }
        }
        return inId;
    }

    private UnitFacilityVisit findUfv(Equipment inEquipment, Facility inFacility,
                                      UnitCategoryEnum inCategory) {

        DomainQuery dq = QueryUtils.createDomainQuery(InventoryEntity.UNIT_FACILITY_VISIT)
                .addDqPredicate(PredicateFactory.ne(UnitField.UFV_VISIT_STATE, UnitVisitStateEnum.RETIRED))
                .addDqPredicate(PredicateFactory.ne(UnitField.UFV_VISIT_STATE, UnitVisitStateEnum.DEPARTED))
                .addDqPredicate(PredicateFactory.eq(UnitField.UFV_PRIMARY_EQ, inEquipment.getEqGkey()))
                .addDqPredicate(PredicateFactory.eq(UnitField.UFV_FREIGHT_KIND, FreightKindEnum.MTY))
                .addDqOrdering(Ordering.asc(UnitField.UFV_VISIT_STATE));

        if (inCategory != null) {
            dq.addDqPredicate(PredicateFactory.eq(UnitField.UFV_UNIT_CATEGORY, inCategory));
        }

        if (inFacility != null) {
            dq.addDqPredicate(PredicateFactory.eq(UnitField.UFV_FACILITY, inFacility.getFcyGkey()));
        } else {
            dq.addDqPredicate(PredicateFactory.eq(UnitField.UFV_TRANSIT_STATE, UfvTransitStateEnum.S10_ADVISED));
        }

        List matches = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
        return (!matches.isEmpty()) ? (UnitFacilityVisit) matches.get(0) : null;
    }

    protected Long safeGetLong(String inNumberString, MetafieldId inField) {
        Long longObject = null;
        if (!StringUtils.isEmpty(inNumberString)) {
            try {
                longObject = new Long(inNumberString);
            } catch (NumberFormatException e) {
                LOGGER.error("safeGetLong: bad value for " + inField + ": " + inNumberString);
            }
        }
        return longObject;
    }

    protected EquipType findActiveEquipType(String inEqIso) throws BizViolation {
        EquipType equipType = null;
        if (ArgoEdiUtils.isNotEmpty(inEqIso)) {
            equipType = EquipType.findEquipType(inEqIso);
            if (equipType != null) {
                if (!equipType.isActive()) {
                    throw BizViolation.createFieldViolation(ArgoPropertyKeys.EQUIPTYPE_IS_OBSOLETE, null, null, equipType.getEqtypId());
                }
            }
        }
        return equipType;
    }

    protected UnitFinder getUnitFinder() {
        return (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
    }

    protected UnitManager getUnitManager() {
        return (UnitManager) Roastery.getBean(UnitManager.BEAN_ID);
    }

    /**
     *
     * @param hazList
     */
    private void processEdiBookingHazards(BookingTransactionDocument.BookingTransaction bookingTran) {
        //this.log("In processEdiBookingHazards");
        List<EdiHazard> hazList = bookingTran.getEdiHazardList();
        if (hazList == null || hazList.size() <= 0) {
            return;
        }

        EdiHazard ediHazard = null;
        for (int i = 0; i < hazList.size(); i++) {
            ediHazard = hazList[i];
            if (!this.hasErrorLogged(ediHazard)) {
                //this.log("ediHazard.getHazardId()1= " + ediHazard.getHazardId());
                //this.log("ediHazard.getImdgClass()1= " + ediHazard.getImdgClass());
                bookingTran.setEdiHazardArray(i, ediHazard);
            }
        }

    }

    /**
     *
     * @param ediBkEqList
     */
    private void processEdiBookingEquipmentHazards(List<com.navis.argo.BookingTransactionDocument.BookingTransaction.EdiBookingEquipment> ediBkEqList) {
        //this.log("In processEdiBookingEquipmentHazards");
        if (ediBkEqList == null || ediBkEqList.size() <= 0) {
            return;
        }

        BookingTransactionDocument.BookingTransaction.EdiBookingEquipment ediBkEq = null;
        for (int i = 0; i < ediBkEqList.size(); i++) {
            ediBkEq = ediBkEqList[i];
            if (ediBkEq == null) {
                continue;
            }

            List<EdiHazard> eqHazList = ediBkEq.getEdiHazardList();
            if (eqHazList.size() <= 0) {
                continue;
            }
            for (int j = 0; j < eqHazList.size(); j++) {
                EdiHazard ediEqHazard = eqHazList[j];
                if (ediEqHazard == null) {
                    continue;
                }
                if (!this.hasErrorLogged(ediEqHazard)) {
                    //this.log("ediHazard.getHazardId()2= " + ediEqHazard.getHazardId());
                    //this.log("ediHazard.getImdgClass()2= " + ediEqHazard.getImdgClass());
                    ediBkEq.setEdiHazardArray(j, ediEqHazard);
                }
            }
        }

    }

    /**
     *
     * @param inEdiHazard
     * @return
     */
    private boolean hasErrorLogged(EdiHazard inEdiHazard) {

        boolean hasError = true;
        if (inEdiHazard == null) {
            return false;
        }

        String unNumber = inEdiHazard.getUnNbr();
        if (StringUtils.isBlank(unNumber)) {
            registerError("The UN Number is blank!");
            return hasError;
        }

        HazardousGoods hzGoods = HazardousGoods.findHazardousGoods(unNumber);
        if (hzGoods == null) {
            registerError("No Hazardous Goods defined for UN Number: " + unNumber);
            return hasError;
        }

        ImdgClass imdgClass = hzGoods.getHzgoodsImdgClass();
        if (imdgClass == null) {
            registerError("No IMDG Class defined in Hazardous Goods for UN number: " + unNumber);
            return hasError;
        }

        //2012.12.07 Validate EDI ImdgClass
        String hazImdgClass = imdgClass.getKey();
        //CSDV-824 2013.02.27 do not validate the imdg class comes with EDI file
        //but set the imdg class getting from reference table
        inEdiHazard.setImdgClass(hazImdgClass);
        inEdiHazard.setHazardProperName(hzGoods.getHzgoodsProperShippingName());
        //this.log("hazImdgClass set= " + hazImdgClass);

        return false;
    }
	
	private static List<EdiHazard> bookingItemHazardList = new ArrayList<EdiHazard>();
}
