/*
 * $Id: PANCreateContainerPreadvisedVisit.groovy 234401 2015-11-25 10:24:11Z extroberso $
 *
 * Copyright (c) 2014 Navis LLC. All Rights Reserved.
 *
 */

/*  CSDV-1197 for barge/rail transaction set the declared inbound visit to the barge/rail visit
 *  CSDV-1536 Temperary work around the product bug (the task is not being instantiated every time the busness flow is
*  executed, so errors from previous runs hang on).
*
*  Date 19-05-2014 CSDV-1896 modified pre-process: made RE ob carrier visit generic if the carrier visit facility is non operational
*  Date 21-05-2014 CSDV-1896 moved the logic to assignOrder
*  Date 05/06/14 CSDV-1244   record EQ_PREADVISE_NN event after preadvising unit to an order
*  Date 02/09/2014 CSDV-2230 Barge/Rail receivals: don't update Arrival Position if it's already been populated by CONSIST/STOWPLAN
*  Date 09/09/2014 CSDV-2234 (1) when a re-validation is triggered by an event, the task currently is executed only if the event is EQ_UPDATE_BKG/ERO/RO;
*                            (2) Book OOG and reefer details are applied to the unit in accordance with edi posting rules.
*                            (3) calls PANCreateContainerPreadvisedVisitHelper instead of executing internal CreateContainerPreadvisedVisit
*  Date 28/11/2014 CSDV-2519 for RE: if current ufv OB cv is not generic and facility is non-operational, set it to generic cv
*  Date 23/Jan/2014 CSDV-2234 apply eqoi gross weight to unit with edi posting rules
*  Date 28/01/2014 CSDV-2234  apply booking dtls, such as origin, shipper, etc.
*  Date 29/01/2014 CSDV-2234  Turn off checking posting rules for now
*
*  Date 24/04/2015 CSDV-2832  do not assign to order if prean is of the STATUS_UPDATE type (COPINO 13)
*                             removed if user = edi condition when assigning the order (and removed assigning the order on the helper)
*                             set unit/ufv inbound carrier to GEN_BARGE or GEN_TRAIN for COPINO 13 prean
*                             do not overwrite intended i/b carrier if it was updated manually
*
*  Date  29/06/2015 CSDV-3041/Mantis-6008 Modified setObCarrier: set OB carrier to prean order OB carrier if ufv current OB carrier is generic OR was previously set by prean
*  Date  02/09/2015 CSDV-3177 Modified method buildRoute. Verified Gate Appointment has Carrier visit
*  Date  14/10/2015 CSDV-3188 When a prean creats a unit, set Can Be Retired By Prean to YES
*  Date  25/11/2015 CSDV-3188 unitFlexString02 is used instead of unitFlexString01 as Can be deleted by prean flag
*
*/


import org.apache.commons.math.exception.util.ArgUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.navis.argo.ContextHelper;
import com.navis.argo.business.api.ArgoEdiFacade;
import com.navis.argo.business.api.ArgoUtils;
import com.navis.argo.business.atoms.EdiMessageClassEnum;
import com.navis.argo.business.atoms.PropertyGroupEnum;
import com.navis.argo.business.model.CarrierVisit;
import com.navis.argo.business.model.EdiPostingContext;
import com.navis.argo.business.model.ICallBack;
import com.navis.argo.business.model.LocPosition;
import com.navis.argo.business.reference.Commodity;
import com.navis.argo.business.reference.EquipType;
import com.navis.argo.business.reference.RoutingPoint;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.external.road.AbstractGateTaskInterceptor;
import com.navis.external.road.EGateTaskInterceptor;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.util.BizViolation;
import com.navis.inventory.InventoryField;
import com.navis.inventory.business.api.UnitField;
import com.navis.inventory.business.units.EqBaseOrderItem;
import com.navis.inventory.business.units.GoodsBase;
import com.navis.inventory.business.units.ReeferRecord;
import com.navis.inventory.business.units.ReeferRqmnts;
import com.navis.inventory.business.units.Routing;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.TranSubTypeEnum;
import com.navis.road.business.model.TruckTransaction;
import com.navis.road.business.util.TransactionTypeUtil;
import com.navis.road.business.workflow.IUnitBean;
import com.navis.road.business.workflow.TransactionAndVisitHolder;
import com.navis.services.business.event.Event;
import com.navis.services.business.rules.EventType
import com.navis.argo.business.atoms.DataSourceEnum
import com.navis.argo.business.model.PropertySource;

class PANCreateContainerPreadvisedVisit extends AbstractGateTaskInterceptor implements EGateTaskInterceptor{

  public void preProcess(TransactionAndVisitHolder inWfCtx) {

	log("PANCreateContainerPreadvisedVisit.preProcess");

	_shouldProceed = shouldProceed(inWfCtx);

	if (!_shouldProceed) {
	  return;
	}

	TruckTransaction tran = inWfCtx.getTran();

	if (tran != null) {

	  Unit unit = tran.getTranUnit();

	  if (unit!= null && tran.getTranEqoItem() != null) {

		_unitCurrentDepartureItem = unit.getUnitPrimaryUe().getUeDepartureOrderItem();
		_unitCurrentArrivalItem =   unit.getUnitPrimaryUe().getUeArrivalOrderItem();

	  }

	  if (TranSubTypeEnum.RE.equals(tran.getTranSubType())) {

		CarrierVisit genVisit = CarrierVisit.getGenericCarrierVisit(ContextHelper.getThreadComplex());

		if (tran.getTranCarrierVisit() == null){
		  if(tran.getTranEqo() == null) {
			tran.setTranCarrierVisit(genVisit);
		  }
		}
	  }
	}

  }
  public void execute(TransactionAndVisitHolder inWfCtx) {

	if (!_shouldProceed) {
	  return;
	}

	try {
	  getLibrary("PANCreateContainerPreadvisedVisitHelper").execute(inWfCtx);

	}catch (BizViolation bv) {
	  getMessageCollector().appendMessage(bv);
	}


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

  public void postProcess(TransactionAndVisitHolder inWfCtx) {

	log("PANCreateContainerPreadvisedVisit.postProcess Start");

	if (!_shouldProceed) {
	  return;
	}

	TruckTransaction tran = inWfCtx.getTran();
	Unit unit = tran.getTranUnit();

	if (unit != null) {
		if (wasUnitJustCreatedByPrean(unit)) {
			unit.setFieldValue(CAN_BE_DELETED_BY_PREAN,YES_VALUE);
		}

	  //Associate unit with Order and set OB Carrier - used to be done in CreateContainerPreadvisedVisit
	  //but  CreateContainerPreadvisedVisit logic is skipped for EDI now
	  //if (EDI_UID.equals(ContextHelper.getThreadUserContext().getUserId())){

	  if (!STATUS_UPDATE.equals(inWfCtx.getAppt().getFieldString(_panFields.RESPONSE_MSG_TYPE))) {

		assignToOrder(tran, unit);
	  }

	  //}

	  if (DataSourceEnum.USER_LCL.equals(PropertySource.findDataSourceForProperty(unit, PropertyGroupEnum.ROUTING))) {
		log("Routing was updated manully (USER_LCL), so OB Carrier and Routing are not updated here");
	  }
	  else {

		setObCarrier(tran);

		unit.setUnitRouting(buildRoute(tran, unit));

	  }

	  unit.modifyGoodsDetails(tran.getTranShipper(), tran.getTranConsignee(),
			  tran.getTranOrigin(), tran.getTranDestination());


	  setUnitlandsideCarrierFields(inWfCtx);

	  if (tran.getTranEqoItem() != null) {

		if ((_unitCurrentDepartureItem != unit.getUnitPrimaryUe().getUeDepartureOrderItem() && unit.getUnitPrimaryUe().getUeDepartureOrderItem() != null) ||
				(_unitCurrentArrivalItem != unit.getUnitPrimaryUe().getUeArrivalOrderItem() && unit.getUnitPrimaryUe().getUeArrivalOrderItem() != null))

		{

		  String evntTypeID = EQ_PREADVISE_EVENT_TYPES.get(tran.getTranEqo().getEqboSubType().getKey());

		  Event evt = (Event)tran.getTranEqoItem().getEqboiOrder().recordOrderEvent(EventType.findEventType(evntTypeID), null, unit.getUnitId());
		  evt.setFieldValue(EVENT_APPLIED_BY_PROCESS, "PREAN");
		}

		applyBookItemDetails(tran, inWfCtx, unit);

	  }

	}

	log("PANCreateContainerPreadvisedVisit.postProcess End");
  }

  private void applyBookItemDetails(TruckTransaction tran, TransactionAndVisitHolder inWfCtx, Unit unit) {
	log("applyBookItemDetails");
	log("unit Id "+unit.getUnitId());

	if (tran.getTranSubType().equals(TranSubTypeEnum.RE)) {

	  /*EdiPostingContext ediPostingContext = ContextHelper.getThreadEdiPostingContext();

	  if (ediPostingContext == null) {

		  ArgoEdiFacade argoEdiFacade = (ArgoEdiFacade) Roastery.getBean(ArgoEdiFacade.BEAN_ID);
		  ediPostingContext = argoEdiFacade.prepareEdiPostingContext(ContextHelper.getThreadComplex(),
				  EdiMessageClassEnum.APPOINTMENT, inWfCtx.getTran().getTranLine(), inWfCtx.getAppt().getField(_panFields.EDI_PARTNER_NAME));

		  ediPostingContext.setIncomingDataSource(ContextHelper.getThreadDataSource());
	  }

	  ediPostingContext.setUfvTransitState(tran.getTranUfv().getUfvTransitState());
	  ediPostingContext.setUnitCategory(unit.getUnitCategory());
	  ediPostingContext.setUnitFreightKind(unit.getUnitFreightKind());
	  */

	  //Turn posting rules off for now
	  EdiPostingContext ediPostingContext  = null;


	  applyOOGs(tran, unit, ediPostingContext);
	  applyReeferDetails(tran, unit, ediPostingContext);
	  applyGrossWeight(tran, unit, ediPostingContext);
	}
  }

  public void postProcessError(TransactionAndVisitHolder inWfCtx) {
	log("PANCreateContainerPreadvisedVisit.postProcessError");
	postProcess(inWfCtx);
  }

  public void postProcessSuccess(TransactionAndVisitHolder inWfCtx) {
	log("PANCreateContainerPreadvisedVisit.postProcessSuccess");
	postProcess(inWfCtx);
  }


  private void assignToOrder(TruckTransaction inTran, Unit inUnit) {

	log("assignToOrder Start");

	if (inUnit != null) {
	  log("eqItem:" + inTran.getTranEqoItem());
	  if (inTran.getTranEqoItem() != null) {

		try {
		  inUnit.assignToOrder(inTran.getTranEqoItem(), inTran.getTranContainer());
		  //inTran.getTranEqoItem().getEqboiOrder().recordOrderEvent(EventEnum.EQ_RECEIVE_BKG, null, inUnit.getUnitId());

		}catch(BizViolation bv) {
		  getMessageCollector().appendMessage(bv);
		  log("Caught BizViolation :"+bv.getMessage());
		}

	  }

	}
	log("assignToOrder End");
  }

  private void setObCarrier(TruckTransaction inTran) {
	UnitFacilityVisit ufv = inTran.getTranUfv();

	if (ufv != null) {

	  if (TranSubTypeEnum.RE.equals(inTran.getTranSubType()) && inTran.getTranCarrierVisit() != null) {

		CarrierVisit ufvCurrentObCv = ufv.getUfvObCv();

		if (!inTran.getTranCarrierVisit().isGenericCv()) {

		  if (inTran.getTranCarrierVisit().getCvFacility().isFcyNonOperational()) {

			if (!ufvCurrentObCv.isGenericCv()) {

			  ufv.updateObCv(CarrierVisit.getGenericCarrierVisit(ContextHelper.getThreadComplex()));
			}

		  }
		  else {

			if (ufvCurrentObCv.isGenericCv() || DataSourceEnum.EDI_APPOINTMENT.equals(PropertySource.findDataSourceForProperty(ufv, PropertyGroupEnum.INTENDED_CV))) {
			  ufv.updateObCv(inTran.getTranCarrierVisit());
			}
		  }
		}

	  }

	}
  }



  public void setUnitlandsideCarrierFields(TransactionAndVisitHolder inWfCtx) {

	GateAppointment prean = inWfCtx.getAppt();
	TruckTransaction tran = inWfCtx.getTran();

	if (prean != null &&  tran != null) {

	  CarrierVisit landsideCarrierVisit = getLandsideCarrierVisit(prean, tran);

	  if (landsideCarrierVisit != null) {

		Unit unit = tran.getTranUnit();
		UnitFacilityVisit ufv = tran.getTranUfv();

		if (ufv != null){

		  if (DataSourceEnum.USER_LCL.equals(PropertySource.findDataSourceForProperty(ufv, PropertyGroupEnum.ACTUAL_IB_CV))) {
			log("Actual IB CV was updated manully (USER_LCL), so not updated here");
		  }

		  else {

			if (landsideCarrierVisit != ufv.getUfvActualIbCv() && (!landsideCarrierVisit.isGenericCv() || (landsideCarrierVisit.isGenericCv() && ufv.getUfvActualIbCv().isGenericCv()))){
			  ufv.updatePositionForArrivingUnit(LocPosition.createLocPosition(landsideCarrierVisit,null,null));
			  ufv.updateActualIbCv(landsideCarrierVisit);
			}


		  }
		}
		if (landsideCarrierVisit != unit.getUnitDeclaredIbCv() && (!landsideCarrierVisit.isGenericCv() || (landsideCarrierVisit.isGenericCv() && unit.getUnitDeclaredIbCv().isGenericCv()))) {
		  unit.updateDeclaredIbCv(landsideCarrierVisit);
		}

	  }

	}

  }

  private CarrierVisit getLandsideCarrierVisit(GateAppointment prean, TruckTransaction tran) {

	CarrierVisit landsideCarrierVisit = (CarrierVisit) prean.getField(_panFields.PREAN_LANDSIDE_CARRIER_VISIT);

	if (landsideCarrierVisit == null) {

	  if ("BARGE".equals(prean.getGapptGate().getGateId())) {
		landsideCarrierVisit = CarrierVisit.findVesselVisit(tran.getGate().getGateFacility(), "GEN_BARGE");
	  }
	  else if ("RAIL".equals(prean.getGapptGate().getGateId())) {
		landsideCarrierVisit = CarrierVisit.getGenericTrainVisit(ContextHelper.getThreadComplex());
	  }
	}
	return landsideCarrierVisit;
  }

  private Routing buildRoute(IUnitBean inTran, Unit inUnit) {

	Routing route = inUnit.getUnitRouting();
	if (route == null) {
	  route = new Routing();
	}
	route.setRtgPOL(inTran.getTranLoadPoint());
	route.setRtgPOD1(inTran.getTranDischargePoint1());
	route.setRtgPOD2(inTran.getTranDischargePoint2());
	route.setRtgOPT1(inTran.getTranOptionalPod1());

	//CSDV-3177: Verify Gate Appointment has Carrier visit
	if (inTran.getTranCarrierVisit() != null) {
	  route.setRtgDeclaredCv(inTran.getTranCarrierVisit());
	}
	if (inTran.getCarrierVisit() != null && inTran.getCarrierVisit().getCvCvd() != null) {
	  route.setRtgCarrierService(inTran.getCarrierVisit().getCvCvd().getCvdService());
	}

	return route;
  }

  void applyOOGs(TruckTransaction inTran, Unit inUnit, EdiPostingContext inEdiPostingContext){

	if (inEdiPostingContext != null) {
	  if (inTran.getTranOogBack() != null || inTran.getTranOogFront() != null ||
			  inTran.getTranOogLeft() != null || inTran.getTranOogRight() != null ||
			  inTran.getTranOogTop() != null) {

		inEdiPostingContext.updateField(inUnit, InventoryField.UNIT_OOG_BACK_CM, inTran.getTranOogBack());
		inEdiPostingContext.updateField(inUnit, InventoryField.UNIT_OOG_FRONT_CM, inTran.getTranOogFront());
		inEdiPostingContext.updateField(inUnit, InventoryField.UNIT_OOG_LEFT_CM, inTran.getTranOogLeft());
		inEdiPostingContext.updateField(inUnit, InventoryField.UNIT_OOG_RIGHT_CM, inTran.getTranOogRight());
		inEdiPostingContext.updateField(inUnit, InventoryField.UNIT_OOG_TOP_CM, inTran.getTranOogTop());

	  }

	}
	else {

	  inUnit.updateOog(inTran.getTranOogBack(), inTran.getTranOogFront(),inTran.getTranOogLeft(), inTran.getTranOogRight(), inTran.getTranOogTop());
	}

  }

  void applyGrossWeight(TruckTransaction inTran, Unit inUnit, EdiPostingContext inEdiPostingContext){

	Double eqoiGrossWt = inTran.getTranEqoItem().getEqoiGrossWeight();

	if (eqoiGrossWt != null) {
	  if (inEdiPostingContext != null) {
		inEdiPostingContext.updateField(inUnit, InventoryField.UNIT_GOODS_AND_CTR_WT_KG, eqoiGrossWt);
	  }
	  else {
		inUnit.setUnitGoodsAndCtrWtKg(eqoiGrossWt);
	  }
	}

  }

  protected void applyReeferDetails(IUnitBean inTran, Unit inUnit, EdiPostingContext inEdiPostingContext) {

	log("=================  applyReeferDetails Start  ======================");

	//TruckTransaction tran = inDao.getTran();
	if (TransactionTypeUtil.isReceival(inTran.getTranSubType())) {

	  GoodsBase goods = inUnit.ensureGoods();

	  ReeferRqmnts oldReeferRqmnts = goods.getGdsReeferRqmnts();

	  final ReeferRqmnts reeferRqmnts;

	  if (oldReeferRqmnts == null) {

		reeferRqmnts = new ReeferRqmnts();

	  } else {
		reeferRqmnts = oldReeferRqmnts.makeCopy();
	  }

	  reeferRqmnts.setRfreqCO2Pct(inTran.getTranCo2Required());
	  reeferRqmnts.setRfreqO2Pct(inTran.getTranO2Required());
	  reeferRqmnts.setRfreqHumidityPct(inTran.getTranHumidityRequired());

	  log("inTran.getTranTempRequired() "+ inTran.getTranTempRequired());
	  reeferRqmnts.setRfreqTempRequiredC(inTran.getTranTempRequired());

	  //ARGO-2605
	  Commodity cmdy = inTran.getTranCommodity();
	  if (cmdy != null) {
		reeferRqmnts.setRfreqTempLimitMaxC(cmdy.getCmdyTempMax());
		reeferRqmnts.setRfreqTempLimitMinC(cmdy.getCmdyTempMin());
	  } else {
		reeferRqmnts.setRfreqTempLimitMaxC(null);
		reeferRqmnts.setRfreqTempLimitMinC(null);
	  }

	  // 2006-10-17 svaitheas v1.2.E ARGO-4696 ARGO-4699 :Correct update reefer requirements form vent settings to conform to functional
	  // requirements
	  // 2006-10-31 svaitheas v1.2.F ARGO-5480 :Rename To Vent Required
	  reeferRqmnts.setRfreqVentRequired(inTran.getTranVentRequired());
	  reeferRqmnts.setRfreqVentUnit(inTran.getTranVentUnit());

	  //ARGO-3227 setting the unit.requirePower on incase if it is an export,
	  //  reefer and the required temp is set.
	  EquipType eqType = null;

	  if (inUnit.getUnitPrimaryUe() != null && inUnit.getUnitPrimaryUe().getUeEquipment() != null &&
			  inUnit.getUnitPrimaryUe().getUeEquipment().getEqEquipType() != null) {

		eqType = inUnit.getUnitPrimaryUe().getUeEquipment().getEqEquipType();
	  }

	  /* if (eqType != null && eqType.isTemperatureControlled() &&
			  inTran.getTranTempRequired() != null && (inTran.getTranSubType().equals(TranSubTypeEnum.RE) ||
			  inTran.getTranSubType().equals(TranSubTypeEnum.RI))) {
		  inUnit.updateRequiresPower(Boolean.TRUE);
		  inUnit.updateWantPowered(Boolean.TRUE);
	  }*/

	  // 2007.06.21 MEP ARGO-7398 Save a reefer record when setting either temperature or vent
	  log("inTran.getTranTempSetting() "+inTran.getTranTempSetting());
	  if (inTran.getTranTempSetting() != null || inTran.getTranVentSetting() != null) {

		ReeferRecord rfrec = ReeferRecord.createReeferRecord(inUnit);
		if (inTran.getTranTempSetting() != null) {
		  rfrec.updateReturnTmp(inTran.getTranTempSetting());
		}
		if (inTran.getTranVentSetting() != null) {
		  rfrec.setVentSettingAndUnit(inTran.getTranVentSetting(), inTran.getTranVentUnit());
		}
	  }

	  if (inEdiPostingContext != null) {

		inEdiPostingContext.updateField(goods, InventoryField.GDS_REEFER_RQMNTS, reeferRqmnts, true, new ICallBack() {
		  public void execute() {
			goods.setGdsReeferRqmnts(reeferRqmnts);
			inUnit.setUnitRequiresPower(Boolean.valueOf(reeferRqmnts.getRfreqTempRequiredC() != null));
		  }
		});

	  }
	  else {
		goods.setGdsReeferRqmnts(reeferRqmnts);
		inUnit.setUnitRequiresPower(Boolean.valueOf(reeferRqmnts.getRfreqTempRequiredC() != null));
	  }

	}
  }

  private boolean shouldProceed(TransactionAndVisitHolder inWfCtx) {

	boolean  shouldProceed = true;



	List<Serializable> extensionData = inWfCtx.getExtensionData();

	if (extensionData != null && !extensionData.isEmpty() && extensionData.get(0) instanceof String) {
	  String eventId = extensionData.get(0);

	  if (eventId.startsWith("EQ_UPDATE_")) {

		shouldProceed = true;
	  }
	  else {
		shouldProceed = false;
	  }
	}



	return shouldProceed;
  }

  private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
  final String EDI_UID = "-edi-";

  private static MetafieldId EVENT_APPLIED_BY_PROCESS = MetafieldIdFactory.valueOf("evntFlexString01");

  private static HibernateApi _hibernateApi = HibernateApi.getInstance();

  private EqBaseOrderItem _unitCurrentArrivalItem = null;
  private EqBaseOrderItem _unitCurrentDepartureItem = null;

  public static Map<String,String> EQ_PREADVISE_EVENT_TYPES = new HashMap();

  private boolean _shouldProceed = true;

  static {

	EQ_PREADVISE_EVENT_TYPES.put("BOOK", "EQ_PREADVISE_BKG");
	EQ_PREADVISE_EVENT_TYPES.put("ERO", "EQ_PREADVISE_ERO");
	EQ_PREADVISE_EVENT_TYPES.put("RAIL", "EQ_PREADVISE_RO");
  }

  private static String STATUS_UPDATE = "STATUS_UPDATE";

  private static MetafieldId CAN_BE_DELETED_BY_PREAN = UnitField.UNIT_FLEX_STRING02;
  static String YES_VALUE = "YES";

}