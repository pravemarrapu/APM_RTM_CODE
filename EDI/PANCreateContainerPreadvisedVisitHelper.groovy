/*
 * Date 10/09/2014 CSDV-2234 this groovy will be used instead of CreateContainerPreadvisedVisit of which this is a modified copy
 *
 * 04/02/15 - Mantis-4900 (Hacky fix to force creation of a UNIT_REROUTE event to kick off RTO Category calculation on EQ_UPDATE_BKG)
 *
 * Date 24/04/2015 CSDV-2832 comment out preadvising unit to order - it's always done (conditionally) in PANCreateContainerPreadvisedVisit
 */

import java.util.Collection;
import java.util.Iterator;

import com.navis.argo.ContextHelper;
import com.navis.argo.business.api.ServicesManager;
import com.navis.argo.business.atoms.BizRoleEnum;
import com.navis.argo.business.atoms.DataSourceEnum;
import com.navis.argo.business.atoms.DrayStatusEnum;
import com.navis.argo.business.atoms.EdiMessageClassEnum;
import com.navis.argo.business.atoms.EquipClassEnum;
import com.navis.argo.business.atoms.EventEnum;
import com.navis.argo.business.atoms.FreightKindEnum;
import com.navis.argo.business.atoms.LocTypeEnum;
import com.navis.argo.business.atoms.OrderPurposeEnum;
import com.navis.argo.business.atoms.UnitCategoryEnum;
import com.navis.argo.business.model.CarrierVisit;
import com.navis.argo.business.model.Complex;
import com.navis.argo.business.model.EdiPostingContext;
import com.navis.argo.business.model.Facility;
import com.navis.argo.business.model.LocPosition;
import com.navis.argo.business.reference.Accessory;
import com.navis.argo.business.reference.Chassis;
import com.navis.argo.business.reference.Commodity;
import com.navis.argo.business.reference.Container;
import com.navis.argo.business.reference.EquipType;
import com.navis.argo.business.reference.Equipment;
import com.navis.argo.business.reference.Group;
import com.navis.argo.business.reference.OrderPurpose;
import com.navis.argo.business.reference.ScopedBizUnit;
import com.navis.argo.business.reference.SpecialStow;
import com.navis.framework.business.Roastery;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.util.BizViolation;
import com.navis.framework.util.message.MessageLevel;
import com.navis.inventory.business.api.UnitFinder;
import com.navis.inventory.business.api.UnitManager;
import com.navis.inventory.business.atoms.EqUnitRoleEnum;
import com.navis.inventory.business.units.EquipmentState;
import com.navis.inventory.business.units.GoodsBase;
import com.navis.inventory.business.units.ReeferRecord;
import com.navis.inventory.business.units.ReeferRqmnts;
import com.navis.inventory.business.units.Routing;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitEquipment;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.orders.business.eqorders.EquipmentOrder;
import com.navis.road.RoadPropertyKeys;
import com.navis.road.business.adaptor.BaseGateTaskAdaptor;
import com.navis.road.business.adaptor.GateTaskHelper;
import com.navis.road.business.adaptor.IGateTaskUnitAdaptor;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.TranSubTypeEnum;
import com.navis.road.business.util.RoadBizUtil;
import com.navis.road.business.util.TransactionTypeUtil;
import com.navis.road.business.workflow.ITranBundleBean;
import com.navis.road.business.workflow.IUnitBean;
import com.navis.road.business.workflow.TransactionAndVisitHolder;
import com.navis.road.business.adaptor.appointment.IGateAppointmentTask;

/**
 * create the basic unit for container arriving at the gate along with the accomopaning chassis, bundle and possible accessories.
 */
public class PANCreateContainerPreadvisedVisitHelper extends BaseGateTaskAdaptor implements IGateAppointmentTask, IGateTaskUnitAdaptor {


  /**
   * create the basic unit for container arriving at the gate along with the accomopaning chassis, bundle and possible accessories
   *
   * @param inOutDao : TruckVisitTransaction data holder for the biz task.
   *
   */
  @Override
  public void execute(TransactionAndVisitHolder inOutDao) throws BizViolation {
	execute(inOutDao.getTran());

	//2010-01-04 spabbala v2.1.E ARGO-22642 Gate/Appointment Side Changes for NFRM-521 (Associating Agents with Units) (Jan Delivery for Haifa)
	if (inOutDao.getAppt() != null) {
	  executeAppointmentTask(inOutDao);
	}
  }

  //2010-01-04 spabbala v2.1.E ARGO-22642 Gate/Appointment Side Changes for NFRM-521 (Associating Agents with Units) (Jan Delivery for Haifa)

  private void executeAppointmentTask(TransactionAndVisitHolder inOutDao) {
	GateAppointment gappt = inOutDao.getAppt();
	Unit unit = inOutDao.getTran().getTranUnit();
	if (unit != null) {
	  unit.updateAgent1(gappt.getGapptAgent1());
	  unit.updateAgent2(gappt.getGapptAgent2());
	  //Set the unit's reference in the appointment.
	  gappt.setGapptUnit(unit);
	}
  }

  @Override
  public void execute(IUnitBean inTran) throws BizViolation {
	/**
	 * Before continuing, make sure there are no errors exist in the main session
	 */
	if (RoadBizUtil.getMessageCollector().getMessageCount(MessageLevel.SEVERE) > 0) {
	  return;
	}
	EdiPostingContext ediPostingContext = ContextHelper.getThreadEdiPostingContext();
	boolean msgClassAppt = false;

	if ( ediPostingContext != null) {
		EdiMessageClassEnum msgClassEnum =  ediPostingContext.getEdiMessageClass();
		msgClassAppt = EdiMessageClassEnum.APPOINTMENT.equals(msgClassEnum);
	}


	if (_ediUserId.equals(ContextHelper.getThreadUserContext().getUserId()) && msgClassAppt){
	  return;
	}

	LocPosition ufvLastKnownRailPos = null;
	UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);

	//2010-01-06 rsatish v2.1.E ARGO-22642 facility can be null if the caller is EDI.
	Facility facility = ContextHelper.getThreadFacility();
	if (facility == null) {
	  facility = inTran.getGate().getGateFacility();
	}
	final Complex complex = facility.getFcyComplex();
	UnitManager unitMgr = (UnitManager) Roastery.getBean(UnitManager.BEAN_ID);
	boolean isBreakBulkReceive = TranSubTypeEnum.RB.equals(inTran.getTranSubType());

	if (inTran.getTranContainer() == null && !isBreakBulkReceive) {
	  return;
	}

	if (!isBreakBulkReceive) {
	  inTran.getTranContainer().upgradeEqType(inTran.getTranCtrTypeId(), DataSourceEnum.IN_GATE);
	  if (inTran.getTranCtrOperator() != null) {
		EquipmentState.upgradeEqOperator(inTran.getTranContainer(), inTran.getTranCtrOperator(), DataSourceEnum.IN_GATE);
	  }
	}

	Unit unit = inTran.getTranUnit();
	CarrierVisit obCv;
	UnitCategoryEnum categoryEnum = TransactionTypeUtil.getExpectedUnitCategory(inTran.getTranSubType());
	if (TranSubTypeEnum.RE.equals(inTran.getTranSubType()) || isBreakBulkReceive) {
	  obCv = inTran.getTranCarrierVisit();
	} else if (TransactionTypeUtil.isThrough(inTran.getTranSubType())) {
	  // 2008-05-13 jku ARGO-11488 1.5.2 Added support for pre-advising through containers when creating appointments.
	  obCv = CarrierVisit.getGenericTruckVisit(complex);
	} else {
	  obCv = CarrierVisit.getGenericCarrierVisit(complex);
	}

	UnitFacilityVisit ufv;
	FreightKindEnum freightKindEnum;
	 //@todo - explain
	//DataSourceEnum dataSourceEnum = DataSourceEnum.IN_GATE;
	  DataSourceEnum _dataSource = ContextHelper.getThreadDataSource();

	freightKindEnum = inTran.getTranCtrFreightKind();
	// if UI did not provide the Freight Kind, assume full
	if (freightKindEnum == null) {
	  freightKindEnum = FreightKindEnum.FCL;
	}
	if (unit == null) {
	  //2010-01-04 spabbala v2.1.E, v2.0.1 ARGO-22933 CreateContainerPreadvisedVisit biztask for Appointments is throwing system exception
	  // if booking is not selected.
	  /* @todo - revisit
	  if (obCv == null) {
		RoadBizUtil.getMessageCollector().appendMessage(MessageLevel.SEVERE, RoadPropertyKeys.GATE__BKG_REQUIRED, null, new String[]{});
	  }*/

	  // Unit needs to be created
	  if (isBreakBulkReceive) {
		unit = unitMgr.findOrCreateIncomingBreakbulkUnit(complex, inTran.getTranUnitId(), inTran.getTranIbCarrier());
		ufv = unit.adviseToFacility(facility, LocPosition.createLocPosition(inTran.getTranIbCarrier(), null, null), obCv);
		HibernateApi.getInstance().saveOrUpdate(ufv);
	  } else {
		try {
		  Container ctr = inTran.getTranContainer();
		  if (ctr != null) {
			//2010-07-22 spabbala v2.1.W ARGO-27210 Unit is not created in current fcy while Pre-advising via config-preadvise if the
			//same Unit is already preadvised in relay-fcy
			Unit advisedUnit = unitFinder.findAdvisedUnitByLandModes(complex, ctr);
			if (advisedUnit != null) {
			  UnitFacilityVisit advisedUfv = advisedUnit.getUfvForFacilityNewest(facility);
			  //2011-04-12 spabbala v2.2.S ARGO-31299 ConfigGate fails to create new import unit but updates existing storage unit
			  //2012-03-22 Balaji Palani ARGO-36894 Manual Pre-advise Gate configuration is not recorded "UNIT_PREADVISE" event,
			  // if the unit is coming by Train.
			  if (advisedUfv != null && (!LocTypeEnum.TRUCK.equals(advisedUnit.getPreadvCarrierMode()) &&
					  !LocTypeEnum.TRAIN.equals(advisedUnit.getPreadvCarrierMode()))) {
				return;  //exit without creating the pre-advice, since already advised as coming in by a carrier mode other than Truck
			  }
			  if (LocTypeEnum.TRAIN.equals(advisedUnit.getPreadvCarrierMode())) {
				if (advisedUfv != null) {
				  ufvLastKnownRailPos = advisedUfv.getUfvLastKnownPosition();
				}
			  }
			}
		  }
		  //2009-04-24 smani 1.9.E ARGO-16643  to find eqo shipper
		  ScopedBizUnit eqoShipper = null;
		  String shipperId = inTran.getTranShipper();
		  if (shipperId != null) {
			eqoShipper = ScopedBizUnit.resolveScopedBizUnit(shipperId, null, BizRoleEnum.SHIPPER);
		  }
		  //2012-04-26 pmelappalayam ARGO-37972 Including the agent detail to the preadvised unit from the booking.
		  ScopedBizUnit eqoAgent = null;
		  if (inTran.getTranScAgent() != null) {
			eqoAgent = ScopedBizUnit.resolveScopedBizUnit(inTran.getTranScAgent(), null, BizRoleEnum.AGENT);
		  } else if (inTran.getTranEqo() != null) {
			eqoAgent = inTran.getTranEqo().getEqoAgent();
		  }

		  //2009-04-15 spabbala v1.9.D ARGO-17169 Pooling must also work in the new CONFIGURABLE Pre-advise
		  EquipType eqType = inTran.getTranCtrTypeId() != null? EquipType.findOrCreateEquipType(inTran.getTranCtrTypeId()):
			inTran.getTranContainer()!= null?inTran.getTranContainer().getEqEquipType():null;

		  if (inTran.getTranIbCarrier() != null) {
			if (LocTypeEnum.TRAIN.equals(inTran.getTranIbCarrier().getLocType())) {
			  if (ufvLastKnownRailPos == null) {
				if (ctr != null) {
				  Unit activeUnit = unitFinder.findActiveUnitByLandModes(complex, ctr);
				  if (activeUnit != null) {
					UnitFacilityVisit activeUfv = activeUnit.getUfvForFacilityNewest(facility);
					if (activeUfv != null) {
					  ufvLastKnownRailPos = activeUfv.getUfvLastKnownPosition();
					}
				  }
				}
			  }
			}
		  }
		  ufv = unitMgr.findOrCreatePreadvisedUnit(facility, inTran.getTranCtrNbr(),
				  eqType,
				  categoryEnum, freightKindEnum, inTran.getTranLine(), inTran.getTranIbCarrier(), obCv, _dataSource,
				  inTran.getTranNotes(), inTran.getTranDischargePoint1(), inTran.getTranCtrOperator(), eqoShipper,
				  inTran.getTranTruckingCompany(), true, eqoAgent);
		  ufv.updateObCv(obCv);
		  if (ufvLastKnownRailPos != null) {
			ufv.updatePositionForArrivingUnit(ufvLastKnownRailPos);
		  }
		  HibernateApi.getInstance().saveOrUpdate(ufv); // 2006.05.03 isaacson ARGO-3983 ufv needs gkey soon after this
		  unit = ufv.getUfvUnit();
		} catch (BizViolation bizViolation) {
		  RoadBizUtil.appendExceptionChain(bizViolation);
		  return;
		}
		//
		unit.updateLineOperator((inTran.getTranLine() == null) ? inTran.getTranCtrOperator() : inTran.getTranLine());
	  }
	} else {
	  // Unit exists, just make sure that it has the correct UFV
	  //SAB: 2010-11-01 ; ARGO-28948
	  CarrierVisit ibCv = null;
	  if (unit.getUnitDeclaredIbCv() != null && unit.getUnitDeclaredIbCv().getLocType().equals(LocTypeEnum.TRUCK)) {
		ibCv = unit.getUnitDeclaredIbCv();
	  }
	  if (ibCv == null) {
		ibCv = inTran.getTranIbCarrier();
	  }

	  ufv = unit.adviseToFacility(facility, LocPosition.createTruckPosition(ibCv, null, null), obCv);
	}

	if (!inTran.getStage().isAppointmentStage()) {
	  Boolean isDirectedToOb = inTran.getTranUfvIbToObCarrierDirect();
	  //2010-01-25 spabbala v2.1.G ARGO-23322 Add a new attribute to UnitFacilityVisit NFRM-252
	  ufv.updateUfvIsDirectIbToObMove(isDirectedToOb);
	}

	inTran.setTranUnit(unit);

	GateTaskHelper.attachEquipmentToUnit(unit, inTran.getTranCtrAccessory(), EqUnitRoleEnum.ACCESSORY);
	GateTaskHelper.attachEquipmentToUnit(unit, inTran.getTranChsAccessory(), EqUnitRoleEnum.ACCESSORY);

	// 2008-05-13 jku ARGO-11229 1.5.2 Advised Weight entered at the appointment stage does not populate in N4.
	unit.updateGoodsAndCtrWtKgAdvised(inTran.getTranCtrGrossWeight());

	if (inTran.getTranSubType().equals(TranSubTypeEnum.RE) || isBreakBulkReceive) {
	  unit.updateLineOperator((inTran.getTranLine() == null) ? inTran.getTranCtrOperator() : inTran.getTranLine());
	  applyContainerExportGoods(inTran, unit);

	  /*  if (inTran.getTranEqoItem() != null) {
		unit.assignToOrder(inTran.getTranEqoItem(), inTran.getTranContainer());
		//inTran.getTranEqoItem().getEqboiOrder().recordOrderEvent(EventEnum.EQ_RECEIVE_BKG, null, unit.getUnitId());
	  }*/
	  if (!isBreakBulkReceive) {
		redirectReleasesIfReturningExport(inTran.getTranEqo(), unit);
	  }
	} else if (inTran.getTranSubType().equals(TranSubTypeEnum.RM)) {

	  unit.updateCategory(UnitCategoryEnum.STORAGE);
	  unit.updateFreightKind(FreightKindEnum.MTY);

	  //ARGO-10330 2008-03-26 dvenkatesan Setting the Transaction's FreightKind once the unit's Freight kind is updated
	  if (inTran.getTranCtrFreightKind() == null) {
		inTran.setTranCtrFreightKind(unit.getUnitFreightKind());
	  }
	  // 2006-08-24 PAC Argo-4982 save seals for empties (Osprey records seals when receiving an empty)
	  unit.updateSeals(inTran.getTranSealNbr1(), inTran.getTranSealNbr2(), inTran.getTranSealNbr3(), inTran.getTranSealNbr4());
	  // attach to Order if there.
	  /* if (inTran.getTranEqoItem() != null) {
		unit.assignToOrder(inTran.getTranEqoItem(), inTran.getTranContainer());
	  }*/

	  applyRoutingGroup(inTran, unit);
	} else if (TranSubTypeEnum.RI.equals(inTran.getTranSubType())) {
	  //2009-02-16 spabbala v1.8.F, v1.8.E.B ARGO-17084 Configurable pre-advise - can't pre-advise dray-in - facility POD mismatch
	  // Assuming it is Dray-In. It can be Dray-out-and-back
	  applyContainerExportGoods(inTran, unit);
	  if (unit.getUnitDrayStatus() == null) {
		unit.updateDrayStatus(DrayStatusEnum.DRAYIN);
	  }
	}

	// ARGO-6183 2007-04-12 PAC clear OUT AND BACK dray status so container can not go out and come back again and again
	clearReturnDrayStatus(unit);

	updateDrayStatusForItt(inTran, unit);

	if (inTran.getTranCommodity() != null) {
	  unit.updateCommodity(inTran.getTranCommodity());
	}

	  /* CSDV-2234
	  if (inTran.getTranSubType().equals(TranSubTypeEnum.RE) || inTran.getTranSubType().equals(TranSubTypeEnum.RI) || isBreakBulkReceive) {
		applyReeferDetails(inTran, unit);
	  }

	  //2007-11-27 ARGO-4978 1.4.v Siva update Oog details if any of the values are not null.
	  if (inTran.getTranOogBack() != null || inTran.getTranOogFront() != null ||
			  inTran.getTranOogLeft() != null || inTran.getTranOogRight() != null ||
			  inTran.getTranOogTop() != null) {
		unit.updateOog(inTran.getTranOogBack(), inTran.getTranOogFront(),
				inTran.getTranOogLeft(), inTran.getTranOogRight(), inTran.getTranOogTop());
	  }*/

	/**
	 * Apply Container Damagaes
	 */

	if (inTran.getTranCtrDmg() != null && inTran.getTranContainer() != null) {
	  // 2006-09-27 Shan v1.2.D ARGO-5295, moved cloneDamages to roadbizUtil
	  RoadBizUtil.cloneDamages(unit, inTran.getTranContainer(), inTran.getTranCtrDmg());
	}

	/**
	 * Apply Container Notes
	 */
	if (TransactionTypeUtil.isReceival(inTran.getTranSubType()) && inTran.getTranNotes() != null && unit.getUnitRouting() != null) {
	  unit.getUnitRouting().setRtgDescription(inTran.getTranNotes());
	}
	// Apply trucking company
	if (TransactionTypeUtil.isReceival(inTran.getTranSubType()) && inTran.getTranTruckingCompany() != null && unit.getUnitRouting() != null) {
	  unit.getUnitRouting().setRtgTruckingCompany(inTran.getTranTruckingCompany());
	}

	attachBundle(inTran, unit);
	/**
	 * ARGO-3770 cpallapo MAR-25-2006 populate Special Handling Id to the _unit.
	 * Right now the tranShandId is string. It should have a relation to specailstow.
	 * TODO need to define a relation to the special stow.
	 */
	String tranShandId = inTran.getTranShandId();
	if (tranShandId != null) {
	  SpecialStow specialStow = SpecialStow.findSpecialStow(tranShandId);
	  if (specialStow != null) {
		unit.updateSpecialStow(specialStow);
		//tran.setTranShandId(specialStow.getStwId());
	  }
	}

	/**
	 * ARGO-3770 cpallapo MAR-25-2006 populate Special Handling Id to the _unit.
	 * Right now the tranShandId is string. It should have a relation to specailstow.
	 * TODO need to define a relation to the special stow.
	 */
	String tranShandId2 = inTran.getTranShandId2();
	if (tranShandId2 != null) {
	  SpecialStow specialStow = SpecialStow.findSpecialStow(tranShandId2);
	  if (specialStow != null) {
		unit.updateSpecialStow2(specialStow);
		//tran.setTranShandId(specialStow.getStwId());
	  }
	}

	/**
	 * ARGO-3770 cpallapo MAR-25-2006 populate Special Handling Id to the _unit.
	 * Right now the tranShandId is string. It should have a relation to specailstow.
	 * TODO need to define a relation to the special stow.
	 */
	String tranShandId3 = inTran.getTranShandId3();
	if (tranShandId3 != null) {
	  SpecialStow specialStow = SpecialStow.findSpecialStow(tranShandId3);
	  if (specialStow != null) {
		unit.updateSpecialStow3(specialStow);
		//tran.setTranShandId(specialStow.getStwId());
	  }
	}

	ServicesManager srvcMgr = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
	if (!srvcMgr.hasEventTypeBeenRecorded(EventEnum.UNIT_PREADVISE, unit)) {
	  srvcMgr.recordEvent(EventEnum.UNIT_PREADVISE, null, null, null, unit);
	}
  }

  /**
   * Applying Export goods to the Unit.
   *
   * @param inUnit  : Inbound container unit.
   * @param inTran  : Truck transaction
   */
  protected void applyContainerExportGoods(IUnitBean inTran, Unit inUnit) {
	//TruckTransaction tran = inDao.getTran();
	try {
	  inUnit.modifyGoodsDetails(inTran.getTranShipper(), inTran.getTranConsignee(),
			  inTran.getTranOrigin(), inTran.getTranDestination());
	  inUnit.setUnitRouting(buildRoute(inTran, inUnit));
	  inUnit.updateCategory(inTran.getTranUnitCategory());
	  if (inTran.getTranCtrFreightKind() == null) {
		inTran.setTranCtrFreightKind(FreightKindEnum.FCL);
	  }
	  if (!TranSubTypeEnum.RB.equals(inTran.getTranSubType())) {
		inUnit.updateFreightKind(inTran.getTranCtrFreightKind());
	  }
	  inUnit.updateSeals(inTran.getTranSealNbr1(), inTran.getTranSealNbr2(), inTran.getTranSealNbr3(), inTran.getTranSealNbr4());
	  if (inTran.getTranCtrGrossWeight() != null) {
		//2007-01-05 ARGO-5310 eyu v1.2.O Change all WEIGHT fields from Long to Double
		inUnit.updateGoodsAndCtrWtKg(inTran.getTranCtrGrossWeight());
	  }
	  //ARGO-3477 MAR-08-2006 cpallapo making sure to populate the OOG details if any of the values are not null.
	 /* CSDV-2234
		if (inTran.getTranOogBack() != null || inTran.getTranOogFront() != null ||
			  inTran.getTranOogLeft() != null || inTran.getTranOogRight() != null ||
			  inTran.getTranOogTop() != null) {
		inUnit.updateOog(inTran.getTranOogBack(), inTran.getTranOogFront(),
				inTran.getTranOogLeft(), inTran.getTranOogRight(), inTran.getTranOogTop());
	  }*/
	} catch (BizViolation bizViolation) {
	  RoadBizUtil.appendExceptionChain(bizViolation);
	}
  }

  private Routing buildRoute(IUnitBean inTran, Unit inUnit) {
	//Added following condition for ARGO-2185, this will not affect the flow of RejectCarrierVisitNotProvided
	//TruckTransaction tran = inDao.getTran();
	if (inTran.getTranCarrierVisit() == null) {
	  inTran.setTranCarrierVisit(CarrierVisit.getGenericCarrierVisit(ContextHelper.getThreadComplex()));
	}

	// 2006-08-24 PAC ARGO-4979 do not overlay other attributes in existing routing, like GROUP
	Routing route = inUnit.getUnitRouting();
	if (route == null) {
	  route = new Routing();
	}
	route.setRtgPOL(inTran.getTranLoadPoint());
	route.setRtgPOD1(inTran.getTranDischargePoint1());
	route.setRtgPOD2(inTran.getTranDischargePoint2());
	route.setRtgOPT1(inTran.getTranOptionalPod1());
	//For Configurable Preadvise screens the tranCarrierVisit will be set with the outboundCarrierVisit.
	route.setRtgDeclaredCv(inTran.getTranCarrierVisit());

	applyRoutingGroup(inTran, inUnit);
	// 2008-06-11 jku (on behalf of bgopal) ARGO-12182 1.5.6 Train service gkey not being saved in the pre-advised unit.
	if (inTran.getCarrierVisit() != null && inTran.getCarrierVisit().getCvCvd() != null) {
	  route.setRtgCarrierService(inTran.getCarrierVisit().getCvCvd().getCvdService());
	}

	return route;
  }

  protected void applyReeferDetails(IUnitBean inTran, Unit inUnit) {
	//TruckTransaction tran = inDao.getTran();
	if (TransactionTypeUtil.isReceival(inTran.getTranSubType())) {
	  GoodsBase goods = inUnit.ensureGoods();

	  ReeferRqmnts reeferRqmnts = goods.getGdsReeferRqmnts();
	  if (reeferRqmnts == null) {
		reeferRqmnts = new ReeferRqmnts();
		goods.setGdsReeferRqmnts(reeferRqmnts);
	  }
	  reeferRqmnts.setRfreqCO2Pct(inTran.getTranCo2Required());
	  reeferRqmnts.setRfreqO2Pct(inTran.getTranO2Required());
	  reeferRqmnts.setRfreqHumidityPct(inTran.getTranHumidityRequired());
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
	  if (eqType != null && eqType.isTemperatureControlled() &&
			  inTran.getTranTempRequired() != null && (inTran.getTranSubType().equals(TranSubTypeEnum.RE) ||
			  inTran.getTranSubType().equals(TranSubTypeEnum.RI))) {
		inUnit.updateRequiresPower(Boolean.TRUE);
		inUnit.updateWantPowered(Boolean.TRUE);
	  }
	  // 2007.06.21 MEP ARGO-7398 Save a reefer record when setting either temperature or vent
	  if (inTran.getTranTempSetting() != null || inTran.getTranVentSetting() != null) {
		ReeferRecord rfrec = ReeferRecord.createReeferRecord(inUnit);
		if (inTran.getTranTempSetting() != null) {
		  rfrec.updateReturnTmp(inTran.getTranTempSetting());
		}
		if (inTran.getTranVentSetting() != null) {
		  rfrec.setVentSettingAndUnit(inTran.getTranVentSetting(), inTran.getTranVentUnit());
		}
	  }
	}
  }

  private void attachBundle(IUnitBean inTran, Unit inUnit) {
	Collection<ITranBundleBean> bundle = inTran.getTranBundle();
	if (bundle == null) {
	  return;
	}
	Iterator<ITranBundleBean> itr = bundle.iterator();
	if (itr == null) {
	  return;
	}
	while (itr.hasNext()) {
	  ITranBundleBean eq = itr.next();
	  if (eq.getTranbundleNbr() != null) {
		findOrCreateEq(inUnit, eq);
	  }
	}
  }

  private void findOrCreateEq(Unit inUnit, ITranBundleBean inEq) {
	try {
	  Equipment eq = Equipment.findEquipment(inEq.getTranbundleNbr());

	  if (eq == null) {
		if (EquipClassEnum.CHASSIS.equals(inEq.getTranbundleEquipType().getEqtypClass())) {
		  eq = Chassis.createChassis(inEq.getTranbundleNbr(), inEq.getTranbundleEquipType().getEqtypId(), _dataSource);
		} else if (EquipClassEnum.CONTAINER.equals(inEq.getTranbundleEquipType().getEqtypClass())) {
		  eq = Container.findOrCreateContainer(inEq.getTranbundleNbr(),
				  inEq.getTranbundleEquipType().getEqtypId(), _dataSource);
		} else if (EquipClassEnum.ACCESSORY.equals(inEq.getTranbundleEquipType().getEqtypClass())) {
		  eq = Accessory.createAccessory(inEq.getTranbundleNbr(), inEq.getTranbundleEquipType().getEqtypId(), _dataSource);
		}
	  }

	  GateTaskHelper.attachEquipmentToUnit(inUnit, eq, EqUnitRoleEnum.PAYLOAD);
	} catch (BizViolation bizViolation) {
	  RoadBizUtil.appendExceptionChain(bizViolation);
	}
  }

  private void redirectReleasesIfReturningExport(EquipmentOrder inEqo, Unit inUnit) {

	UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
	Unit unit = unitFinder.findNewestPreviousUnit(inUnit.getPrimaryEq(), inUnit);
	// 2006-06-07 Shan v1.1.2.0 , ARGO-000,braces were missing for the if loop, fixed checkstyle issue
	if (unit != null) {
	  if (UnitCategoryEnum.EXPORT.equals(unit.getUnitCategory())) {
		if (DrayStatusEnum.RETURN.equals(unit.getUnitDrayStatus())) {
		  UnitEquipment ue = unit.getUnitPrimaryUe();
		  if (ue.getUeDepartureOrderItem() != null) {
			Long ctrEqoGkey = ue.getUeDepartureOrderItem().getEqboiOrder().getEqboGkey();
			if (inEqo != null && inEqo.getEqboGkey().equals(ctrEqoGkey)) {
			  // Export, Dray Off, Same Booking  - move all flags to the new unit
			  ServicesManager servicesMgr = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
			  servicesMgr.redirectFlags(unit, inUnit, true);
			}
		  }
		}
	  }
	}
  }

  private void applyRoutingGroup(IUnitBean inTruckTransaction, Unit inUnit) {
	String rtgGroupId = inTruckTransaction.getTranImportReleaseNbr();
	if (rtgGroupId != null) {
	  Group rtgGroup = Group.findGroup(rtgGroupId);
	  if (rtgGroup != null) {
		Routing route = inUnit.getUnitRouting();
		if (route == null) {
		  route = new Routing();
		}
		route.setRtgGroup(rtgGroup);
	  }
	}
  }

  protected void clearReturnDrayStatus(Unit inUnit) throws BizViolation {
	if (DrayStatusEnum.RETURN.equals(inUnit.getUnitDrayStatus()) || DrayStatusEnum.OFFSITE.equals(inUnit.getUnitDrayStatus())) {
	  inUnit.updateDrayStatus(null);
	}
  }

  protected void updateDrayStatusForItt(IUnitBean inTran, Unit inUnit) {
	String rtgGroupId = inTran.getTranImportReleaseNbr();
	if (rtgGroupId != null) {
	  Group rtgGroup = Group.findGroup(rtgGroupId);
	  if (rtgGroup != null) {
		OrderPurpose grpDestPurpose = rtgGroup.getGrpPurpose();
		if (grpDestPurpose != null) {
		  if (OrderPurposeEnum.IFT.getId().equals(grpDestPurpose.getOrderpurposeId())) {
			if (inUnit.getUnitCategory().equals(UnitCategoryEnum.IMPORT) && TranSubTypeEnum.RI.equals(inTran.getTranSubType())) {
			  try {
				inUnit.updateDrayStatus(DrayStatusEnum.DRAYIN);
			  } catch (BizViolation bv) {
				RoadBizUtil.appendExceptionChain(bv);
			  }
			}
		  }
		}
	  }
	}
  }

  final String _ediUserId = "-edi-";
  DataSourceEnum _dataSource;
}