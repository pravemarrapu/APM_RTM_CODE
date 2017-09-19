import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.orders.business.eqorders.EquipmentReceiveOrder
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.xmlbeans.XmlObject;
import org.jetbrains.annotations.Nullable;


import com.navis.argo.AppointmentTransactionDocument;
import com.navis.argo.AppointmentTransactionsDocument;
import com.navis.argo.ArgoConfig;
import com.navis.argo.ArgoPropertyKeys;
import com.navis.argo.ArgoRefField;
import com.navis.argo.ContextHelper;
import com.navis.argo.EdiAgent;
import com.navis.argo.EdiCommodity;
import com.navis.argo.EdiDriver;
import com.navis.argo.EdiFacility;
import com.navis.argo.EdiFlexFields;
import com.navis.argo.EdiHazard;
import com.navis.argo.EdiRouting;
import com.navis.argo.EdiTruck;
import com.navis.argo.EdiVesselVisit;
import com.navis.argo.EdiWeight;
import com.navis.argo.business.api.ArgoEdiUtils;
import com.navis.argo.business.api.ArgoUtils;
import com.navis.argo.business.api.EdiPoster;
import com.navis.argo.business.atoms.BizRoleEnum;
import com.navis.argo.business.atoms.DataSourceEnum;
import com.navis.argo.business.atoms.DrayStatusEnum;
import com.navis.argo.business.atoms.EquipClassEnum;
import com.navis.argo.business.atoms.FreightKindEnum;
import com.navis.argo.business.atoms.LocTypeEnum;
import com.navis.argo.business.atoms.UnitCategoryEnum;
import com.navis.argo.business.model.CarrierVisit;
import com.navis.argo.business.model.Complex;
import com.navis.argo.business.model.EdiPostingContext;
import com.navis.argo.business.model.Facility;
import com.navis.argo.business.model.ICallBack;
import com.navis.argo.business.reference.Chassis;
import com.navis.argo.business.reference.Container;
import com.navis.argo.business.reference.EquipType;
import com.navis.argo.business.reference.Equipment;
import com.navis.argo.business.reference.RoutingPoint;
import com.navis.argo.business.reference.ScopedBizUnit;
import com.navis.argo.business.reference.Shipper;
import com.navis.framework.business.Roastery;
import com.navis.framework.business.atoms.LifeCycleStateEnum;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.persistence.HibernatingEntity;
import com.navis.framework.util.BizFailure;
import com.navis.framework.util.BizViolation;
import com.navis.framework.util.BizWarning;
import com.navis.framework.util.LogUtils;
import com.navis.framework.util.ValueObject;
import com.navis.framework.util.unit.UnitUtils;
import com.navis.inventory.InventoryBizMetafield;
import com.navis.inventory.InventoryField;
import com.navis.inventory.InventoryPropertyKeys;
import com.navis.inventory.business.api.UnitField;
import com.navis.inventory.business.api.UnitFinder;
import com.navis.inventory.business.api.UnitManager;
import com.navis.inventory.business.atoms.UfvTransitStateEnum;
import com.navis.inventory.business.imdg.HazardItem;
import com.navis.inventory.business.imdg.Hazards;
import com.navis.inventory.business.imdg.ImdgClass;
import com.navis.inventory.business.units.BaseUnitEdiPosterPea;
import com.navis.inventory.business.units.EqBaseOrder;
import com.navis.inventory.business.units.EquipmentState;
import com.navis.inventory.business.units.GoodsBase;
import com.navis.inventory.business.units.Routing;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitEquipment;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.orders.OrdersField;
import com.navis.orders.OrdersPropertyKeys;
import com.navis.orders.business.eqorders.Booking;
import com.navis.orders.business.eqorders.EquipmentDeliveryOrder;
import com.navis.orders.business.eqorders.EquipmentOrder;
import com.navis.orders.business.eqorders.EquipmentOrderItem;
import com.navis.road.RoadApptsField;
import com.navis.road.RoadApptsPropertyKeys;
import com.navis.road.RoadConfig;
import com.navis.road.RoadPropertyKeys;
import com.navis.road.business.appointment.api.AppointmentFinder;
import com.navis.road.business.appointment.api.IAppointmentManager;
import com.navis.road.business.appointment.model.AppointmentTimeSlot;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.AppointmentStateEnum;
import com.navis.road.business.atoms.TranSubTypeEnum;
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum;
import com.navis.road.business.model.Gate;
import com.navis.road.business.model.TruckDriver;
import com.navis.road.business.model.TruckingCompany;
import com.navis.argo.business.model.LocPosition;
import com.navis.argo.business.model.GeneralReference
import com.navis.external.framework.util.ExtensionUtils
import org.jdom.Element

import java.util.Calendar;
import java.util.Date;

import com.navis.road.business.appointment.api.TimeSlotBean;
import com.navis.road.business.apihandler.AppointmentApiXmlUtil;
import com.navis.road.business.appointment.model.AppointmentQuotaRule;

/**
 * Posts COPINO Appointment
 *
 *
 * Author: Sophia Robertson
 * Date: 03/04/13
 * Called by PANApptPostInterceptor
 *
 * Date 13/02/14 CSDV-1637 modified postDropoffImport: set outbound CV to GEN_TRUCK
 *
 * Date 21/03/14 CSDV-1722 Import Dropoff: prevent unitManager.findOrCreatePreadvisedUnitEdi creating a new advised ufv by
 *                                         passing to it the existing ufv ObCv
 *
 * Date 09/04/14 CSDV-1841 fixed method signatures
 *
 * Date 30/05/14 CSDV-1841 more signatures fixed
 *
 * Date 05/06/14 CSDV-1244 commented out preadvising/reserving unit against an order (it is done in the business flow)

 * Date 25/06/14 CSDV-2152 commented out  appointment.submit in connection with not using Should Run Validation Field
 *
 *  Date 05/07/14 CSDV-2159 When more then one prean  is connected to the same TAR, changing the 1st prean timeslot should change the time slot for all  the other preans.
 *
 *  Date 05/07/14 CSDV-2159 Pass in pre-created app nbr
 *
 *  Date 14/07/14 CSDV-2168 setPostingRuleAttributesToPostingContext for pickup posts
 *
 *  Date 02/09/14 CSDV-2284 set Ib Cv to edi trucking co only for TRUCK Copinos
 *
 *  05/02/15 CSDV-2753 (APMT Mantis-5459) Update unit line operator (per posting rules)
 *
 *  20/03/15 APMT Mantis-5766 and Mantis-5271 temp: bypassing the lock creation until fixed in product
 *
 *  01/04/15 CSDV-2825 for dropoff exports: if a booking is known, get its outbound carrier visit to pass to findOrCreatePreadvisedUnitEdi, so that a an advised unit is not created erroneously
 *  08/04/15 fixed CSDV-2824 as well
 *
 *  04/05/15 CSDV-2832 don't try to prea-advise an export ctr if ctr ID was not provided
 *
 *  30/06/15 CSDV-3042 /Mantis-3700   before and after tolerances are not taken into account when deciding if the time slot is wrong
 *                                    added flush to createNewAndDeleteOldAppointment to prevent "Duplicate appt exists" hard error
 *
 *  05/08/15 //CSDV-3096 - Added Unknown eqiupment type to avoid null pointer exception in postContainerAttributes & postChassisAttributes methods
 *
 *  15/09/15 CSDV-3287 (EHD) : added truckPrean boolean
 *
 *  31-Aug-17 WF#870627 : Removed haz validation since COPINO will never have haz info
 *  18-Sept-17 - Pradeep ARya WF#892222 -  attach ERO to RM on COPINO posting
 */
public class  PANAppointmentPoster extends BaseUnitEdiPosterPea implements EdiPoster {

	public HibernatingEntity postToDomainModel(EdiPostingContext inEdiPostingContext, XmlObject inXmlObject) throws BizViolation {
		ContextHelper.setThreadDataSource(DataSourceEnum.EDI_APPOINTMENT);
		return postAppointment(inEdiPostingContext, inXmlObject);
	}

	/**
	 * Post an Appointment EDI transaction to the domain model.
	 *
	 * @param inEdiPostingContext provides context for this posting of EDI, including settings, facts and a place to store messages
	 * @param inXmlObject         an XmlObject (per apache xml-beans) containing an EDI transaction.
	 * @return HibernatingEntity
	 * @throws com.navis.framework.util.BizViolation
	 *
	 */
	public HibernatingEntity postToDomainModel(EdiPostingContext inEdiPostingContext, XmlObject inXmlObject, Map inParms) throws BizViolation {
		_appNbr  = (Long)inParms.get("APPT_NBR");
		ContextHelper.setThreadDataSource(DataSourceEnum.EDI_APPOINTMENT);
		return postAppointment(inEdiPostingContext, inXmlObject, inParms);
	}

	/**
	 * Post the Appointment EDI transaction to the domain model
	 * <p/>
	 * <p/>
	 *
	 * @param inEdiPostingContext the posting context
	 * @param inXmlObject         an XmlObject (per apache xml-beans) containing an EDI transaction.
	 * @return The top-level Domain entity that was created or modified. This will be Unit here.
	 * @throws com.navis.framework.util.BizViolation
	 *
	 */
	@Nullable
	public GateAppointment postAppointment(EdiPostingContext inEdiPostingContext, XmlObject inXmlObject, Map inParms) throws BizViolation {

		LogUtils.setLogLevel(getClass(), Level.INFO);

		if (inXmlObject == null) {
			throw BizFailure.create(OrdersPropertyKeys.ERRKEY__NULL_XMLBEAN, null);
		}

		if (!AppointmentTransactionsDocument.class.isAssignableFrom(inXmlObject.getClass())) {
			throw BizFailure.create(OrdersPropertyKeys.ERRKEY__TYPE_MISMATCH_XMLBEAN, null, inXmlObject.getClass().getName());
		}

		AppointmentTransactionsDocument appointmentDocument = (AppointmentTransactionsDocument) inXmlObject;
		final AppointmentTransactionsDocument.AppointmentTransactions appointmentTrans = appointmentDocument.getAppointmentTransactions();

		final AppointmentTransactionDocument.AppointmentTransaction[] appointmentTransArray = appointmentTrans.getAppointmentTransactionArray();

		if (appointmentTransArray.length != 1) {
			throw BizFailure.create(OrdersPropertyKeys.ERRKEY__XML_TRANSACTION_DOCUMENT_LENGTH_EXCEED, null,
					String.valueOf(appointmentTransArray.length));
		}
		GateAppointment appointment = postOne(inEdiPostingContext, appointmentTransArray[0], inParms);

		return appointment;
	}

	/**
	 * This performs the actual posting process of the Appointment transaction.
	 *
	 * @param inEdiPostingContext the posting context
	 * @param inEdiAppointment    the appointment XML bean
	 * @return the Unit that was created or updated
	 * @throws com.navis.framework.util.BizViolation
	 *          in case of any errors
	 */
	@Nullable
	public GateAppointment postOne(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, Map inParms)
			throws BizViolation {

		/* FIND OR CREATE APPT */

		//PREAN NOTE: The edi appointment date shouldn't be null
		validateAppointmentDate(inEdiAppointment);

		TruckerFriendlyTranSubTypeEnum apptType = determineAppointmentType(inEdiPostingContext, inEdiAppointment);
		Facility fcy = determineFacility(inEdiPostingContext, inEdiAppointment);
		inEdiPostingContext.throwIfAnyViolations();
		inEdiPostingContext.setTimeZone(fcy.getTimeZone());
		Gate gate = determineGate(inEdiPostingContext, inEdiAppointment, fcy);
		// Don't go any further if we couldn't get any gate
		inEdiPostingContext.throwIfAnyViolations();

		GateAppointment appointment = findOrCreateEqAppointment(inEdiPostingContext, inEdiAppointment, gate, apptType, fcy);
		//HibernateApi.getInstance().flush();

		boolean isUpdateAllowedForUnitInYard = isUpdateAllowedForUnitInYard(inEdiPostingContext);

		// Don't go any further if we couldn't create a basic appointment
		inEdiPostingContext.throwIfAnyViolations();


		/* CREATE OR UPDATE UNIT AND UFV */
		UnitFacilityVisit ufv = null;

		final Routing routing = getRouting(inEdiPostingContext, inEdiAppointment, null);

		CarrierVisit landsideCarrierVisit = (CarrierVisit)inParms.get("LANDSIDE_CARRIER_VISIT_KEY");
		CarrierVisit ibCv = null;

		boolean truckPrean = !(BARGE_GATE.equals(gate.getGateId()) || RAIL_GATE.equals(gate.getGateId()) || RAIL_ITT_GATE.equalsIgnoreCase(gate.getGateId()));

		if (truckPrean) {
			String truckCompId = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyId();
			ibCv = CarrierVisit.findOrCreateTruckVisitForEdi(fcy, truckCompId);
		} else {
			//Prean: barge or train drop-off
			if (landsideCarrierVisit != null ) {

				ibCv = landsideCarrierVisit;
			}
			else {
				ibCv = CarrierVisit.getGenericTruckVisit(fcy.getFcyComplex());
			}

		}
		/* SOPHIA - HERE */
		//ibCv = CarrierVisit.findVesselVisit(Facility.findFacility("FCY111") , "GEN_BARGE");

		EquipmentOrder eqo = inParms.get("EDI_ORDER_KEY");

		CarrierVisit obCv = null;
		if (eqo != null && eqo  instanceof Booking) {
			obCv = eqo.getEqoVesselVisit();
		}

		if (TruckerFriendlyTranSubTypeEnum.PUC.equals(apptType)) {
			postPickupChassis(inEdiPostingContext, inEdiAppointment, appointment, fcy, routing);
		} else if (TruckerFriendlyTranSubTypeEnum.PUE.equals(apptType)) {
			postPickupExport(inEdiPostingContext, inEdiAppointment, appointment, fcy, routing);
		} else if (TruckerFriendlyTranSubTypeEnum.PUI.equals(apptType)) {
			postPickupImport(inEdiPostingContext, inEdiAppointment, appointment, fcy, routing);
		} else if (TruckerFriendlyTranSubTypeEnum.PUM.equals(apptType)) {
			postPickupEmpty(inEdiPostingContext, inEdiAppointment, appointment, fcy, routing);
		} else if (TruckerFriendlyTranSubTypeEnum.DOC.equals(apptType)) {
			postDropoffChassis(inEdiPostingContext, inEdiAppointment, appointment, fcy, routing);
		} else if (TruckerFriendlyTranSubTypeEnum.DOE.equals(apptType)) {
			ufv = postDropoffExport(inEdiPostingContext, inEdiAppointment, appointment, isUpdateAllowedForUnitInYard, fcy, routing, ibCv, obCv);
		} else if (TruckerFriendlyTranSubTypeEnum.DOI.equals(apptType)) {
			ufv = postDropoffImport(inEdiPostingContext, inEdiAppointment, appointment, isUpdateAllowedForUnitInYard, fcy, routing, ibCv);
		} else if (TruckerFriendlyTranSubTypeEnum.DOM.equals(apptType)) {
			ufv = postDropoffEmpty(inEdiPostingContext, inEdiAppointment, appointment, isUpdateAllowedForUnitInYard, fcy, routing, ibCv);
		}

		/* RUN VALIDATION */

		//throw the violations before triggering the biz tasks
		inEdiPostingContext.throwIfAnyViolations();

		// save the appointment to validate the changes by creating non-persitence truckVisit, truckTransaction and possible hazards
		//@todo commented out as a part of Remove Should Run Validation Field  - needs to revisit
		/*if (appointment != null) {
            appointment.setGapptState(AppointmentStateEnum.CREATED);
            Hazards hazards = getHazards(inEdiPostingContext, inEdiAppointment);
            appointment.submit(hazards, GateClientTypeEnum.EDI, null);
            inEdiPostingContext.throwIfAnyViolations();
        }*/

		// 2008-05-14 smahesh v1.5.2 ARGO-9892 Record the posting as Service Events
		if (ufv != null) {
			finalizePosting(inEdiPostingContext, ufv.getUfvUnit());
		}

		//appointment.setFieldValue(RoadApptsField.GAPPT_REFERENCE_NBR,"TransRef1");
		return appointment;
	}

	private void validateAppointment(TruckerFriendlyTranSubTypeEnum inApptType, GateAppointment inGateAppt) throws BizViolation {
		if (isPickUpTransaction(inApptType) && (!(AppointmentStateEnum.CREATED.equals(inGateAppt.getApptState())
				|| AppointmentStateEnum.LATE.equals(inGateAppt.getApptState())))) {
			throw BizViolation.create(RoadApptsPropertyKeys.APPOINTMENT_STATE_INVALID, null, inGateAppt.getGapptNbr(), inGateAppt.getApptState());
		}
	}

	@Nullable
	private Routing getRouting(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
							   Unit inUnit) {
		Routing routing = null;
		EdiRouting ediRouting = inEdiAppointment.getExportRouting();
		if (ediRouting != null) {
			RoutingPoint pol = extractRoutingPoint(inEdiPostingContext, ediRouting.getLoadPort(), UnitField.UNIT_RTG_POL);
			RoutingPoint pod = extractRoutingPoint(inEdiPostingContext, ediRouting.getDischargePort1(), RoadApptsField.GAPPT_POD1);
			RoutingPoint pod2 = extractRoutingPoint(inEdiPostingContext, ediRouting.getDischargePort2(), RoadApptsField.GAPPT_POD2);
			RoutingPoint opt1 = extractRoutingPoint(inEdiPostingContext, ediRouting.getOptionalPort1(), RoadApptsField.GAPPT_POD1_OPTIONAL);
			RoutingPoint opt2 = extractRoutingPoint(inEdiPostingContext, ediRouting.getOptionalPort2(), RoadApptsField.GAPPT_POD2_OPTIONAL);
			RoutingPoint opt3 = extractRoutingPoint(inEdiPostingContext, ediRouting.getOptionalPort3(), UnitField.UNIT_RTG_OPT3);
			RoutingPoint opl = extractRoutingPoint(inEdiPostingContext, ediRouting.getOriginalLoadPort(), UnitField.UNIT_RTG_OPL);
			if (inUnit != null) {
				//always get cloned copy instead of using it directly as posting rule will not applied otherwise.
				routing = inUnit.getSafeRoutingCopy();
			} else {
				routing = Routing.createEmptyRouting();
			}
			setRoutingPOL(routing, pol);
			setRoutingOPL(routing, opl);
			setRoutingPOD1(routing, pod);
			setRoutingPOD2(routing, pod2);
			setRoutingOPT1(routing, opt1);
			setRoutingOPT2(routing, opt2);
			setRoutingOPT3(routing, opt3);
		}
		return routing;
	}

	private boolean isPickUpTransaction(TruckerFriendlyTranSubTypeEnum inApptType) {
		return TruckerFriendlyTranSubTypeEnum.PUC.equals(inApptType) || TruckerFriendlyTranSubTypeEnum.PUE.equals(inApptType) ||
				TruckerFriendlyTranSubTypeEnum.PUI.equals(inApptType) || TruckerFriendlyTranSubTypeEnum.PUM.equals(inApptType);
	}

	private void postFlexFields(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
								UnitFacilityVisit inUfv, GateAppointment inAppointment, Facility inFacility) {
		//2008-03-26 kramu v1.5.M ARGO-10694
		if (inUfv != null) {
			postFlexFields(inEdiPostingContext, inUfv, inEdiAppointment.getEdiFlexFields());
			postFlexFields(inEdiPostingContext, inUfv.getUfvUnit(), inEdiAppointment.getEdiFlexFields());
		} else {
			postFlexFields(inEdiPostingContext, inEdiAppointment.getEdiFlexFields(), inAppointment, inFacility);
		}
		postFlexFieldsForAppointment(inEdiPostingContext, inAppointment, inEdiAppointment.getEdiFlexFields());
	}

	private void postFlexFieldsForAppointment(EdiPostingContext inEdiPostingContext, GateAppointment inAppointment,
											  EdiFlexFields inEdiFlexFields) {

		if (inEdiFlexFields == null) {
			return;
		}

		// Ufv fields
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING01, inEdiFlexFields.getUfvFlexString01());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING02, inEdiFlexFields.getUfvFlexString02());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING03, inEdiFlexFields.getUfvFlexString03());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING04, inEdiFlexFields.getUfvFlexString04());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING05, inEdiFlexFields.getUfvFlexString05());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING06, inEdiFlexFields.getUfvFlexString06());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING07, inEdiFlexFields.getUfvFlexString07());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING08, inEdiFlexFields.getUfvFlexString08());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING09, inEdiFlexFields.getUfvFlexString09());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_STRING10, inEdiFlexFields.getUfvFlexString10());

		Object ufvDate1 = inEdiFlexFields.getUfvFlexDate01();
		if (ufvDate1 instanceof Calendar) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_DATE01,
					ArgoEdiUtils.convertLocalToUtcDate(ufvDate1, inEdiPostingContext.getTimeZone()));
		}
		Object ufvDate2 = inEdiFlexFields.getUfvFlexDate02();
		if (ufvDate2 instanceof Calendar) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UFV_FLEX_DATE02,
					ArgoEdiUtils.convertLocalToUtcDate(ufvDate2, inEdiPostingContext.getTimeZone()));
		}

		// Unit fields
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING01, inEdiFlexFields.getUnitFlexString01());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING02, inEdiFlexFields.getUnitFlexString02());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING03, inEdiFlexFields.getUnitFlexString03());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING04, inEdiFlexFields.getUnitFlexString04());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING05, inEdiFlexFields.getUnitFlexString05());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING06, inEdiFlexFields.getUnitFlexString06());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING07, inEdiFlexFields.getUnitFlexString07());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING08, inEdiFlexFields.getUnitFlexString08());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING09, inEdiFlexFields.getUnitFlexString09());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING10, inEdiFlexFields.getUnitFlexString10());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING11, inEdiFlexFields.getUnitFlexString11());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING12, inEdiFlexFields.getUnitFlexString12());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING13, inEdiFlexFields.getUnitFlexString13());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING14, inEdiFlexFields.getUnitFlexString14());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT_FLEX_STRING15, inEdiFlexFields.getUnitFlexString15());
	}

	private boolean isUpdateAllowedForUnitInYard(EdiPostingContext inEdiPostingContext) {
		String updateUnitInYardConfig = inEdiPostingContext.getConfigValue(ArgoConfig.UPDATE_UNIT_IN_YARD);
		return (updateUnitInYardConfig != null && getBooleanConfigValue(updateUnitInYardConfig));
	}

	private void postPickupChassis(EdiPostingContext inEdiPostingContext,
								   AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
								   GateAppointment inAppointment, Facility inFacility, Routing inRouting) throws BizViolation {
		// Update appointment with transaction specific details
		postBaseAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postRouting(inEdiPostingContext, inAppointment, inRouting);
		postCommodity(inEdiPostingContext, inEdiAppointment, inAppointment);
		postAgents(inEdiPostingContext, inEdiAppointment, inAppointment);
		postChassisAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFreightKind(inEdiPostingContext, inEdiAppointment, inAppointment);
		postOrder(inEdiPostingContext, inEdiAppointment, inAppointment, null);
		postOrderItem(inEdiPostingContext, inAppointment);
		postFlexFields(inEdiPostingContext, inEdiAppointment, null, inAppointment, inFacility);
	}

	private void postPickupExport(EdiPostingContext inEdiPostingContext,
								  AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
								  GateAppointment inAppointment, Facility inFacility, Routing inRouting) throws BizViolation {

		postBaseAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postRouting(inEdiPostingContext, inAppointment, inRouting);
		final EdiVesselVisit outboundVesselVisit = inEdiAppointment.getEdiOutboundVesselVisit();
		CarrierVisit obCv = findVesselVisit(inEdiPostingContext, outboundVesselVisit, inEdiAppointment.getLoadFacility());
		if (obCv != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_VESSEL_VISIT, obCv);
			// 2012-04-12 bbakthavachalam v2.4.G ARGO-34873 Post vessel visit flex fields
			postVesselVisitFlexFields(inEdiPostingContext, obCv, outboundVesselVisit.getVesselVisitFlexFields());
		}
		postCommodity(inEdiPostingContext, inEdiAppointment, inAppointment);
		postContainerAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postChassisAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFreightKind(inEdiPostingContext, inEdiAppointment, inAppointment);
		postAgents(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFlexFields(inEdiPostingContext, inEdiAppointment, null, inAppointment, inFacility);
		postPickUpUnitDetails(inEdiPostingContext, inEdiAppointment, inAppointment, inFacility);
	}

	private void postPickupImport(EdiPostingContext inEdiPostingContext,
								  AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
								  GateAppointment inAppointment, Facility inFacility, Routing inRouting) throws BizViolation {


		postBaseAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		if (inEdiPostingContext != null) {
			inEdiPostingContext.throwIfAnyViolations();
		}

		postRouting(inEdiPostingContext, inAppointment, inRouting);
		final EdiVesselVisit inboundVesselVisit = inEdiAppointment.getEdiInboundVesselVisit();
		CarrierVisit inCv = findVesselVisit(inEdiPostingContext, inboundVesselVisit, inEdiAppointment.getLoadFacility());
		if (inCv != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_VESSEL_VISIT, inCv);
			// 2012-04-12 bbakthavachalam v2.4.G ARGO-34873 Post vessel visit flex fields
			postVesselVisitFlexFields(inEdiPostingContext, inCv, inboundVesselVisit.getVesselVisitFlexFields());
		}
		postCommodity(inEdiPostingContext, inEdiAppointment, inAppointment);
		postContainerAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postChassisAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFreightKind(inEdiPostingContext, inEdiAppointment, inAppointment);
		postAgents(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFlexFields(inEdiPostingContext, inEdiAppointment, null, inAppointment, inFacility);
		postPickUpUnitDetails(inEdiPostingContext, inEdiAppointment, inAppointment, inFacility);
	}

	private void postPickupEmpty(EdiPostingContext inEdiPostingContext,
								 AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
								 GateAppointment inAppointment, Facility inFacility, Routing inRouting) throws BizViolation {
		// Update appointment with transaction specific details
		//2008-03-20 kramu v1.5.L ARGO-10593 container info should be posted, if available
		postBaseAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postRouting(inEdiPostingContext, inAppointment, inRouting);
		postCommodity(inEdiPostingContext, inEdiAppointment, inAppointment);
		postContainerAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postChassisAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFreightKind(inEdiPostingContext, inEdiAppointment, inAppointment);
		//Prean:
		//error out when EDO doesn't exist or it's different than the reserved
		//postOrder(inEdiPostingContext, inEdiAppointment, inAppointment, null);
		//postOrderItem(inEdiPostingContext, inAppointment);
		postAgents(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFlexFields(inEdiPostingContext, inEdiAppointment, null, inAppointment, inFacility);
		postPickUpUnitDetails(inEdiPostingContext, inEdiAppointment, inAppointment, inFacility);
	}

	private void postPickUpUnitDetails(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
									   GateAppointment inAppointment, Facility inFacility)
			throws BizViolation {

		Unit unit = findActiveOrAdvisedUnitForDelivery(inEdiAppointment, inFacility);

		setPostingRuleAttributesToPostingContext(inEdiPostingContext, unit, null);

		if (!inEdiPostingContext.isOkToPostPerGeneralRule(unit)) {
			return;
		}

		if (unit != null) {
			postOog(inEdiPostingContext, unit, inEdiAppointment.getOogDimensions());
			EdiCommodity ediCommodity = inEdiAppointment.getEdiCommodity();
			if (ediCommodity != null) {
				postGoodsBase(inEdiPostingContext, unit, ediCommodity);
				postReeferRequirements(inEdiPostingContext, unit, ediCommodity.getCommodityTemperature(), ediCommodity);
			}
			postHazards(inEdiPostingContext, unit, inEdiAppointment.getEdiHazardArray());

			postAgent1(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent1());
			postAgent2(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent2());
			postCommodityForUnit(inEdiPostingContext, inEdiAppointment, unit);

			UnitFacilityVisit ufv = null;
			if (unit != null) {
				ufv = unit.getUfvForFacilityLiveOnly(inFacility);
				setPostingRuleAttributesToPostingContext(inEdiPostingContext, null, ufv);
			}

			//2008-03-26 kramu v1.5.M ARGO-10694
			if (ufv != null) {
				postFlexFields(inEdiPostingContext, ufv, inEdiAppointment.getEdiFlexFields());
				postFlexFields(inEdiPostingContext, ufv.getUfvUnit(), inEdiAppointment.getEdiFlexFields());
			}

			inEdiPostingContext.updateField(unit, InventoryField.UNIT_SEAL_NBR1, inAppointment.getGapptCtrSealNbr1());
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_SEAL_NBR2, inAppointment.getGapptCtrSealNbr2());
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_SEAL_NBR3, inAppointment.getGapptCtrSealNbr3());
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_SEAL_NBR4, inAppointment.getGapptCtrSealNbr4());
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_GOODS_AND_CTR_WT_KG, inAppointment.getGapptCtrGrossWeight());

			patchHazards(inAppointment, unit);
			//CSDV-1244
			/*if (inAppointment.getGapptOrderItem() != null) {
                if (EquipmentOrderSubTypeEnum.ERO.equals(inAppointment.getGapptOrderItem().getEqboiOrder().getEqboSubType())) {
                    inEdiPostingContext.updateField(unit.getUnitPrimaryUe(), InventoryField.UE_ARRIVAL_ORDER_ITEM,
                            inAppointment.getGapptOrderItem());
                } else {
                    inEdiPostingContext.updateField(unit.getUnitPrimaryUe(), InventoryField.UE_DEPARTURE_ORDER_ITEM,
                            inAppointment.getGapptOrderItem());
                }
            }*/
		}
	}

	@Nullable
	private Unit findActiveOrAdvisedUnitForDelivery(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, Facility inFacility)
			throws BizViolation {
		Container ctr = Container.findContainer(inEdiAppointment.getContainerId());
		Unit unit = null;
		if (ctr != null) {
			UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
			unit = unitFinder.findActiveUnit(inFacility.getFcyComplex(), ctr);
			FreightKindEnum freightKind = FreightKindEnum.getEnum(inEdiAppointment.getFreightKind());

			if (unit == null || !freightKind.equals(unit.getUnitFreightKind())) {
				LocTypeEnum[] locTypes = [LocTypeEnum.TRAIN,LocTypeEnum.VESSEL];
				List unitList = unitFinder.findAdvisedUnits(inFacility.getFcyComplex(), ctr,locTypes);
				for (Unit advisedUnit : unitList) {
					if (freightKind.equals(advisedUnit.getUnitFreightKind())) {
						unit =  advisedUnit;
						break;
					}
				}
			}
		}
		return unit;
	}

	private void postDropoffChassis(EdiPostingContext inEdiPostingContext,
									AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
									GateAppointment inAppointment, Facility inFacility, Routing inRouting) {
		// Update appointment with transaction specific details
		postRouting(inEdiPostingContext, inAppointment, inRouting);
		postCommodity(inEdiPostingContext, inEdiAppointment, inAppointment);
		postChassisAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFreightKind(inEdiPostingContext, inEdiAppointment, inAppointment);
		// Preadvise BARE Chassis
		preadviseBareChassis(inEdiPostingContext, inAppointment);
		postAgents(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFlexFields(inEdiPostingContext, inEdiAppointment, null, inAppointment, inFacility);
	}

	@Nullable
	private UnitFacilityVisit postDropoffExport(EdiPostingContext inEdiPostingContext,
												AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
												GateAppointment inAppointment, boolean inUpdateAllowedForUnitInYard, Facility inFacility,
												Routing inRouting, CarrierVisit inIbCv, CarrierVisit inObCv)
			throws BizViolation {

		UnitFacilityVisit ufv = null;

		if (inUpdateAllowedForUnitInYard) {
			Container ctr = findOrCreateContainer(inEdiAppointment);
			Unit unit = getUnitFinder().findActiveUnit(ContextHelper.getThreadComplex(), ctr);
			if (unit != null) {
				ufv = unit.getUfvForFacilityLiveOnly(inFacility);
			}
			if (ufv != null && UfvTransitStateEnum.S40_YARD.equals(ufv.getUfvTransitState())) {
				postAgent1(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent1());
				postAgent2(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent2());
				postCommodityForUnit(inEdiPostingContext, inEdiAppointment, unit);
				postFlexFields(inEdiPostingContext, inEdiAppointment, ufv, inAppointment, inFacility);
				return ufv;
			}
		}

		if (ufv != null) {
			inEdiPostingContext.setPostingToNewlyCreatedUfv(ufv.isNewlyCreated());
		}

		postCommodity(inEdiPostingContext, inEdiAppointment, inAppointment);

		postRouting(inEdiPostingContext, inAppointment, inRouting);
		//Posts Appt Requested Date/Time, Truck info
		postBaseAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);

		postAgents(inEdiPostingContext, inEdiAppointment, inAppointment);

		//19-Jul-2013 CSDV-1194 use generic outbound visit to avoid Null pointer exception in UnitManager.findOrCreatePreadvisedUnitEdi

		//final EdiVesselVisit outboundVesselVisit = inEdiAppointment.getEdiOutboundVesselVisit();
		//CarrierVisit obCv = findVesselVisit(inEdiPostingContext, outboundVesselVisit, inEdiAppointment.getLoadFacility());

		CarrierVisit obCv = inObCv == null ? CarrierVisit.getGenericVesselVisit(ContextHelper.getThreadComplex()) : inObCv;

		/*
        if (obCv != null) {
          inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_VESSEL_VISIT, obCv);
          // 2012-04-12 bbakthavachalam v2.4.G ARGO-34873 Post vessel visit flex fields
          postVesselVisitFlexFields(inEdiPostingContext, obCv, outboundVesselVisit.getVesselVisitFlexFields());
        }*/

		postChassisAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postContainerAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFreightKind(inEdiPostingContext, inEdiAppointment, inAppointment);

		//postOrder(inEdiPostingContext, inEdiAppointment, inAppointment, obCv);
		//postOrderItem(inEdiPostingContext, inAppointment);

		postGrossWt(inEdiPostingContext, inEdiAppointment, inAppointment);

		postSeals(inEdiPostingContext, inEdiAppointment, inAppointment);

		// Preadvise EXPORT container
		ufv = preadviseContainer(inEdiPostingContext, inEdiAppointment, inAppointment, obCv, UnitCategoryEnum.EXPORT, inIbCv);

		return ufv;
	}

	private UnitFacilityVisit postDropoffImport(EdiPostingContext inEdiPostingContext,
												AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
												GateAppointment inAppointment, boolean inUpdateAllowedForUnitInYard, Facility inFacility,
												Routing inRouting, CarrierVisit inIbCv)
			throws BizViolation {

		UnitFacilityVisit ufv = null;
		Container ctr = findOrCreateContainer(inEdiAppointment);
		Unit unit = getUnitFinder().findActiveUnit(ContextHelper.getThreadComplex(), ctr);

		if (inUpdateAllowedForUnitInYard) {

			if (unit != null) {
				ufv = unit.getUfvForFacilityLiveOnly(inFacility);
			}
			if (ufv != null && UfvTransitStateEnum.S40_YARD.equals(ufv.getUfvTransitState())) {
				postAgent1(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent1());
				postAgent2(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent2());
				postCommodityForUnit(inEdiPostingContext, inEdiAppointment, unit);
				postFlexFields(inEdiPostingContext, inEdiAppointment, ufv, inAppointment, inFacility);
				return ufv;
			}
		}

		if (ufv != null) {
			inEdiPostingContext.setPostingToNewlyCreatedUfv(ufv.isNewlyCreated());
		}

		postCommodity(inEdiPostingContext, inEdiAppointment, inAppointment);
		postRouting(inEdiPostingContext, inAppointment, inRouting);
		postBaseAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postAgents(inEdiPostingContext, inEdiAppointment, inAppointment);

		// Update appointment with transaction specific details
		// 2012-04-12 bbakthavachalam v2.4.G ARGO-34873 Post vessel visit flex fields

		/* CSDV-1637 */
		/*
        final EdiVesselVisit outboundVesselVisit = inEdiAppointment.getEdiOutboundVesselVisit();
        CarrierVisit obCv = findVesselVisit(inEdiPostingContext, outboundVesselVisit, inEdiAppointment.getLoadFacility());
        if (outboundVesselVisit != null) {
          postVesselVisitFlexFields(inEdiPostingContext, obCv, outboundVesselVisit.getVesselVisitFlexFields());
        }*/
		/* CSDV-1722 */
		CarrierVisit obCv;
		if (unit != null) {
			if (DrayStatusEnum.DRAYIN.equals(unit.getUnitDrayStatus())) {

				obCv =  unit.getUnitRouting().getRtgDeclaredCv();
			}
		}
		if (obCv == null) {
			obCv = CarrierVisit.getGenericTruckVisit(ContextHelper.getThreadComplex());
		}

		final EdiVesselVisit inboundVesselVisit = inEdiAppointment.getEdiInboundVesselVisit();
		CarrierVisit inCv = findVesselVisit(inEdiPostingContext, inboundVesselVisit, inEdiAppointment.getLoadFacility());

		if (inCv != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_VESSEL_VISIT, inCv);
			postVesselVisitFlexFields(inEdiPostingContext, inCv, inboundVesselVisit.getVesselVisitFlexFields());
		}

		postChassisAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postContainerAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFreightKind(inEdiPostingContext, inEdiAppointment, inAppointment);
		postOrder(inEdiPostingContext, inEdiAppointment, inAppointment, obCv);
		postOrderItem(inEdiPostingContext, inAppointment);
		postGrossWt(inEdiPostingContext, inEdiAppointment, inAppointment);
		postSeals(inEdiPostingContext, inEdiAppointment, inAppointment);
		// Preadvise container with category to TRANSHIP/IMPORT as given in edi.
		String categoryStr = inEdiAppointment.getCategory();
		UnitCategoryEnum category = UnitCategoryEnum.getEnum(categoryStr);
		ufv = preadviseContainer(inEdiPostingContext, inEdiAppointment, inAppointment, obCv, category, inIbCv);
		//Standard N4 behaviour is to not update ufv Actual Ib Cv, so we do it here
		ufv.updatePositionForArrivingUnit(LocPosition.createLocPosition(inIbCv,null,null));
		ufv.updateActualIbCv(inIbCv);

		unit = ufv.getUfvUnit();

		postAgent1(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent1());
		postAgent2(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent2());
		postCommodityForUnit(inEdiPostingContext, inEdiAppointment, unit);
		postFlexFields(inEdiPostingContext, inEdiAppointment, ufv, inAppointment, inFacility);

		inEdiPostingContext.updateField(unit, InventoryField.UNIT_DRAY_STATUS, DrayStatusEnum.DRAYIN);
		return ufv;
	}

	@Nullable
	private UnitFacilityVisit postDropoffEmpty(EdiPostingContext inEdiPostingContext,
											   AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
											   GateAppointment inAppointment, boolean inUpdateAllowedForUnitInYard, Facility inFacility,
											   Routing inRouting, CarrierVisit inIbCv)
			throws BizViolation {

		UnitFacilityVisit ufv = null;

		if (inUpdateAllowedForUnitInYard) {
			Container ctr = findOrCreateContainer(inEdiAppointment);
			Unit unit = getUnitFinder().findActiveUnit(ContextHelper.getThreadComplex(), ctr);
			if (unit != null) {
				ufv = unit.getUfvForFacilityLiveOnly(inFacility);
			}
			if (ufv != null && UfvTransitStateEnum.S40_YARD.equals(ufv.getUfvTransitState())) {
				postAgent1(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent1());
				postAgent2(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent2());
				postCommodityForUnit(inEdiPostingContext, inEdiAppointment, unit);
				postFlexFields(inEdiPostingContext, inEdiAppointment, ufv, inAppointment, inFacility);
				return ufv;
			}
		}

		if (ufv != null) {
			inEdiPostingContext.setPostingToNewlyCreatedUfv(ufv.isNewlyCreated());
		}

		postCommodity(inEdiPostingContext, inEdiAppointment, inAppointment);
		postRouting(inEdiPostingContext, inAppointment, inRouting);
		postBaseAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postAgents(inEdiPostingContext, inEdiAppointment, inAppointment);

		//WF#892222 -  Post ERO for DOM
		postOrder(inEdiPostingContext, inEdiAppointment, inAppointment, inIbCv);
		postOrderItem(inEdiPostingContext, inAppointment);

		// Update appointment with transaction specific details
		postChassisAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postContainerAttributes(inEdiPostingContext, inEdiAppointment, inAppointment);
		postFreightKind(inEdiPostingContext, inEdiAppointment, inAppointment);
		// Preadvise EMPTY storage container
		ufv = preadviseEmpty(inEdiPostingContext, inEdiAppointment, inAppointment, inIbCv);
		if (ufv != null) {
			postAgent1(inEdiPostingContext, ufv.getUfvUnit(), inEdiAppointment.getContainerAgent1());
			postAgent2(inEdiPostingContext, ufv.getUfvUnit(), inEdiAppointment.getContainerAgent2());
			postCommodityForUnit(inEdiPostingContext, inEdiAppointment, ufv.getUfvUnit());
			postFlexFields(inEdiPostingContext, inEdiAppointment, ufv, inAppointment, inFacility);
		}
		return ufv;
	}

	private Container findOrCreateContainer(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) throws BizViolation {
		Container ctr = null;
		String ctrId = inEdiAppointment.getContainerId();
		String ctrType = inEdiAppointment.getContainerType();
		if (ArgoUtils.isNotEmpty(ctrId) && ArgoUtils.isNotEmpty(ctrType)) {
			ctr = Container.findOrCreateContainer(ctrId, ctrType, DataSourceEnum.EDI_APPOINTMENT);
		}
		return ctr;
	}

	private void preadviseBareChassis(EdiPostingContext inEdiPostingContext, GateAppointment inAppointment) {

		EquipType eqType = inAppointment.getGapptChassisEquipType();
		Facility fcy = inAppointment.getGapptGate().getGateFacility();
		String chsId = inAppointment.getGapptChassisId();
		ScopedBizUnit line = inAppointment.getGapptLineOperator();
		//2008-03-21 kramu v1.5.L ARGO-10610 Empty storage chassis should be preadvised with generic truck visit
		CarrierVisit cv = CarrierVisit.getGenericTruckVisit(fcy.getFcyComplex());

		UnitFacilityVisit ufv;
		try {
			findOrCreateChassis(inEdiPostingContext, inAppointment);
			ufv = _um.findOrCreatePreadvisedUnit(fcy, chsId, eqType, UnitCategoryEnum.STORAGE, FreightKindEnum.MTY,
					line, cv, cv, DataSourceEnum.EDI_APPOINTMENT, null);
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT, ufv.getUfvUnit());
		} catch (BizViolation inBv) {
			inEdiPostingContext.addViolation(inBv);
		}
	}

	@Nullable
	private UnitFacilityVisit preadviseEmpty(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction
			inEdiAppointment, GateAppointment inAppointment, CarrierVisit inIbCv) {

		EquipType eqType = inAppointment.getGapptCtrEquipType();
		Facility fcy = inAppointment.getGapptGate().getGateFacility();
		String ctrId = inAppointment.getGapptCtrId();
		ScopedBizUnit line = inAppointment.getGapptLineOperator();
		//2008-03-21 kramu v1.5.L ARGO-10610 Empty storage containers should be preadvised with generic truck visit
		CarrierVisit cv = CarrierVisit.getGenericTruckVisit(fcy.getFcyComplex());

		UnitFacilityVisit ufv = null;

		CarrierVisit ibCv = inIbCv != null ? inIbCv : cv;
		try {
			if (ctrId != null && StringUtils.isNotEmpty(ctrId.trim())) {
				//check for active container/unit for the same operator if found use the same unit for preannoucment
				Container ctr = findOrCreateContainer(inEdiAppointment);
				Unit unit = getUnitFinder().findActiveUnit(ContextHelper.getThreadComplex(), ctr);
				if(unit && unit.getUnitLineOperator().equals(line)) {
					ufv = unit.getUnitActiveUfv();
					// set the ibCv from the edi
					ufv.setUfvActualIbCv(inIbCv);
				}
				//ibCv should be generic truck visit
				// datasource DataSourceEnum.EDI_APPOINTMENT set to null
				else {

					ufv = _um.findOrCreatePreadvisedUnitEdi(fcy, ctrId, eqType, null, UnitCategoryEnum.STORAGE, line, ibCv, cv,
							DataSourceEnum.EDI_APPOINTMENT, null, EquipClassEnum.CONTAINER);
				}
				// 2010-11-24 kjeyapandian ARGO-29277 Empty Storage appointment posting creates Full Active Pre-advise
				inEdiPostingContext.setPostingToNewlyCreatedUfv(ufv.isNewlyCreated());
				if(!unit)
					unit = ufv.getUfvUnit();
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT, unit);
				inEdiPostingContext.updateField(unit, InventoryField.UNIT_CATEGORY, UnitCategoryEnum.STORAGE);
				inEdiPostingContext.updateField(unit, InventoryField.UNIT_FREIGHT_KIND, FreightKindEnum.MTY);
			}
		} catch (BizViolation inBv) {
			inEdiPostingContext.addViolation(inBv);
		}
		return ufv;
	}

	@Nullable
	private UnitFacilityVisit preadviseContainer(EdiPostingContext inEdiPostingContext,
												 AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
												 GateAppointment inAppointment,
												 CarrierVisit inObCv, UnitCategoryEnum inCategory, CarrierVisit inIbCv) throws BizViolation {

		EquipType eqType = inAppointment.getGapptCtrEquipType();
		Facility fcy = inAppointment.getGapptGate().getGateFacility();
		String ctrId = inAppointment.getGapptCtrId();
		//CSDV-2832
		if (ctrId == null || StringUtils.isEmpty(ctrId.trim())) {
			return;
		}

		FreightKindEnum freightKind = inAppointment.getGapptFreightKind();
		ScopedBizUnit line = inAppointment.getGapptLineOperator();
		CarrierVisit ibCv = inIbCv;

		//Prean:
		/*if (inEdiAppointment.getEdiOutboundVesselVisit() == null) {
          throw BizViolation.create(InventoryPropertyKeys.OUTBOUND_VESSEL_VISIT_IS_REQUIRED, null);
        }*/

		UnitFacilityVisit ufv = null;
		try {
			Container ctr = findOrCreateContainer(inEdiAppointment);
			ufv = _um.findOrCreatePreadvisedUnitEdi(fcy, ctrId, eqType, ctr, inCategory,
					line, ibCv, inObCv, DataSourceEnum.EDI_APPOINTMENT, null, EquipClassEnum.CONTAINER);
			inEdiPostingContext.setPostingToNewlyCreatedUfv(ufv.isNewlyCreated());
			Unit unit = ufv.getUfvUnit();
			UnitEquipment ue = unit.getUnitPrimaryUe();
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_UNIT, unit);
			if (!inEdiPostingContext.isOkToPostPerGeneralRule(unit)) {
				return ufv;
			}
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_FREIGHT_KIND, freightKind);
			//2010-12-24 spabbala v2.2.J, v2.1.3 ARGO-29694 Appointment EDI for category EXPRT creates appointment in data model with catefory STRGE
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_CATEGORY, inCategory);
			postOog(inEdiPostingContext, unit, inEdiAppointment.getOogDimensions());

			Routing unitRouting = getRouting(inEdiPostingContext, inEdiAppointment, unit);
			// since we have a defined outbound vessel visit we can set the same to declared ob visit as well
			postPreadvisedRouting(inEdiPostingContext, ufv, inEdiAppointment.getExportRouting(), inObCv, inObCv, inCategory, unitRouting);

			EdiCommodity ediCommodity = inEdiAppointment.getEdiCommodity();
			if (ediCommodity != null) {
				postGoodsBase(inEdiPostingContext, unit, ediCommodity);
				postReeferRequirements(inEdiPostingContext, unit, ediCommodity.getCommodityTemperature(), ediCommodity);
			}

			// Before posting hazards from appointment, validate EDI Hazards against exiting Unit Hazards
			// WF#870627 commented out, COPINO will never have the haz info so the validation is not required
			// rejectHazardNotAsAdvised(inEdiPostingContext, inEdiAppointment.getEdiHazardArray(), unit, inAppointment);

			postHazards(inEdiPostingContext, unit, inEdiAppointment.getEdiHazardArray());

			postAgent1(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent1());
			postAgent2(inEdiPostingContext, unit, inEdiAppointment.getContainerAgent2());
			postCommodityForUnit(inEdiPostingContext, inEdiAppointment, unit);
			postFlexFields(inEdiPostingContext, inEdiAppointment, ufv, inAppointment, fcy);

			inEdiPostingContext.updateField(unit, InventoryField.UNIT_SEAL_NBR1, inAppointment.getGapptCtrSealNbr1());
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_SEAL_NBR2, inAppointment.getGapptCtrSealNbr2());
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_SEAL_NBR3, inAppointment.getGapptCtrSealNbr3());
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_SEAL_NBR4, inAppointment.getGapptCtrSealNbr4());
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_GOODS_AND_CTR_WT_KG, inAppointment.getGapptCtrGrossWeight());
			//CSDV-1244
			/* if (inAppointment.getGapptOrderItem() != null) {
                if (EquipmentOrderSubTypeEnum.ERO.equals(inAppointment.getGapptOrderItem().getEqboiOrder().getEqboSubType())) {
                    inEdiPostingContext.updateField(ue, InventoryField.UE_ARRIVAL_ORDER_ITEM, inAppointment.getGapptOrderItem());
                } else {
                    inEdiPostingContext.updateField(ue, InventoryField.UE_DEPARTURE_ORDER_ITEM, inAppointment.getGapptOrderItem());
                }
            }*/
			patchHazards(inAppointment, unit);
			//Mantis-5459
			inEdiPostingContext.updateField(unit, InventoryField.UNIT_LINE_OPERATOR, line);

		} catch (BizViolation inBv) {
			inEdiPostingContext.addViolation(inBv);
		}
		return ufv;
	}

	/**
	 * Posts the routing information from the EdiRouting element when pre-advising a container.
	 *
	 * @param inEdiPostingContext posting context
	 * @param inUfv               UnitFacilityVisit being updated
	 * @param inEdiRouting        Routing points for unit
	 * @param inInboundCv
	 * @param inOutboundCv
	 */
	protected void postPreadvisedRouting(final EdiPostingContext inEdiPostingContext,
										 final UnitFacilityVisit inUfv, EdiRouting inEdiRouting, CarrierVisit inInboundCv,
										 final CarrierVisit inOutboundCv, final UnitCategoryEnum inCategory, final Routing inRouting) {
		if (inEdiRouting == null) {
			return;
		}

		if (inInboundCv != null) {
			inRouting.setRtgDeclaredCv(inInboundCv);
		}

		final Unit unit = inUfv.getUfvUnit();

/*
		The code below is an example of a complex use of EdiPostingContext.  We want to update the routing field of the
		Unit, but also update the category and intended outbound carrier in parallel.  That is, if the rules for posting
		the routing validate to APPLY, we also want to apply and correctly record the category and obcv.
*/
		inEdiPostingContext.updateField(unit, InventoryField.UNIT_ROUTING, inRouting, true, new ICallBack() {
			public void execute() {
				// Directly set the routing: the rules have been evaluated and the changes recorded
				unit.setUnitRouting(inRouting);

				// Set the category WITHOUT evaluating rules but WITH recording of field changes
				inEdiPostingContext.updateField(unit, InventoryField.UNIT_CATEGORY, inCategory, false, null);

				// Set the intended OB carrier as done above for category, but use our business method to do the actual update
				inEdiPostingContext.updateField(inUfv, InventoryField.UFV_INTENDED_OB_CV, inOutboundCv, false, new ICallBack() {
					public void execute() {
						inUfv.updateObCv(inOutboundCv);
					}
				});
			}
		});
	}

	private void postCommodityForUnit(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction
			inEdiAppointment, Unit inUnit) {
		EdiCommodity ediCommodity = inEdiAppointment.getEdiCommodity();
		if (ediCommodity != null) {
			postGoodsBaseForUnit(inEdiPostingContext, inUnit, ediCommodity);
		}
	}

	private void postGoodsBaseForUnit(EdiPostingContext inEdiPostingContext, Unit inUnit, EdiCommodity inEdiCommodity) {
		if (inEdiCommodity == null) {
			return;
		}
		GoodsBase goods = inUnit.ensureGoods();
		String consignee = inEdiCommodity.getConsignee();
		String shipper = inEdiCommodity.getShipper();
		inEdiPostingContext.updateField(goods, InventoryBizMetafield.GDS_CONSIGNEE_AS_STRING, consignee);
		inEdiPostingContext.updateField(goods, InventoryBizMetafield.GDS_SHIPPER_AS_STRING, shipper);
	}

	/**
	 * Compare hazards in Appointment EDI with UNIT (which may or may not be preadvised).  This code is here and not in the gate workflow because
	 * during workflow execution we cannot distinquish between preadvised hazards and the EDI hazards since we are also preadvising containers right
	 * inside the appointment poster
	 *
	 * @param inEdiContext
	 * @param inEdiHazardArray
	 * @param inUnit
	 */
	private void rejectHazardNotAsAdvised(EdiPostingContext inEdiContext, EdiHazard[] inEdiHazardArray, Unit inUnit, GateAppointment inAppointment) {

		if (ArgoConfig.HAZARD_MATCH.isOn(ContextHelper.getThreadUserContext())) {
			Hazards advisedHazards = inUnit.getUnitGoods().getGdsHazards();
			if (inEdiHazardArray != null && inEdiHazardArray.length > 0) {
				// EDI is hazardous
				if (advisedHazards == null) {
					// Reject, appointment is hazardous and container is not advised as hazardous
					inEdiContext.addViolation(RoadPropertyKeys.GATE__APPT_WITH_HAZARDS_NOT_ADVISED, inUnit.getUnitId());
				} else {
					// Verify each hazard in the EDI is also advised
					for (int i = 0; i < inEdiHazardArray.length; i++) {

						String ediImdgClassString = inEdiHazardArray[i].getImdgClass();
						if (StringUtils.isNotEmpty(ediImdgClassString)) {
							ImdgClass ediImdgClass = ImdgClass.getEnum(ediImdgClassString);
							if (advisedHazards.findHazardItem(ediImdgClass) == null) {
								// Reject, appointment hazard not amongst advised hazards
								inEdiContext.addViolation(RoadPropertyKeys.GATE__APPT_WITH_AN_UNADVISED_HAZARD, inUnit.getUnitId(), ediImdgClass);
							}
						}
					}
				}
			} else if (advisedHazards != null) {
				// Reject, appointment is non-hazardous but container is advised as hazardous
				inEdiContext.addViolation(RoadPropertyKeys.GATE__APPT_WITHOUT_HAZARDS_ADVISED_AS_HAZARDOUS, inUnit.getUnitId());
			}
		}
	}

	/**
	 * EDI may just have a hazard class, try and patch up the UN Number from the related booking data
	 *
	 * @param inAppointment
	 * @param inUnit
	 */
	private void patchHazards(GateAppointment inAppointment, Unit inUnit) {
		Hazards apptHazards = inUnit.getUnitGoods().getGdsHazards();
		if (apptHazards != null && inAppointment.getGapptOrderItem() != null) {
			// Appointment has hazards and a Booking
			Iterator it = apptHazards.getHazardItemsIterator();
			while (it.hasNext()) {
				HazardItem apptItem = (HazardItem) it.next();
				ImdgClass apptClass = apptItem.getHzrdiImdgClass();

				if (StringUtils.isEmpty(apptItem.getHzrdiUNnum()) && apptClass != null) {
					// Appointment Hazard is missing UN Number
					EqBaseOrder baseOrder = inAppointment.getGapptOrderItem().getEqboiOrder();
					Booking booking = (Booking) EquipmentOrder.resolveEqoFromEqbo(baseOrder);
					Hazards bkgHazards = booking.getEqoHazards();
					if (bkgHazards != null) {
						// Booking also has hazards, find Booking hazard item with the same class
						Iterator bkgIt = bkgHazards.getHazardItemsIterator();
						while (bkgIt.hasNext()) {
							HazardItem bkgItem = (HazardItem) bkgIt.next();
							if (apptClass.equals(bkgItem.getHzrdiImdgClass())) {
								// Fix appointment hazard UN Number
								apptItem.updateHzrdiUNnum(bkgItem.getHzrdiUNnum());
								break;
							}
						}
					}
				}
			}
		}
	}

	@Nullable
	private Container findOrCreateContainer(EdiPostingContext inEdiPostingContext, GateAppointment inAppointment) throws BizViolation {
		// Find or create the Container
		Container container = null;
		String ctrId = inAppointment.getGapptCtrId();
		EquipType ctrType = inAppointment.getGapptCtrEquipType();
		String ctrTypeId = null;
		if (ctrType != null) {
			ctrTypeId = ctrType.getEqtypId();
		}
		ScopedBizUnit line = inAppointment.getGapptLineOperator();

		if (ctrId != null && StringUtils.isEmpty(ctrId.trim())) {
			throw BizViolation.createFieldViolation(ArgoPropertyKeys.CONTAINER_ID_REQUIRED, null, null);
		}

		try {
			container = Container.findOrCreateContainer(ctrId, ctrTypeId, DataSourceEnum.EDI_APPOINTMENT);
			if (line != null) {
				EquipmentState.upgradeEqOperator(container, line, DataSourceEnum.EDI_APPOINTMENT);
			}
		} catch (BizViolation bv) {
			inEdiPostingContext.addViolation(bv);
		}
		return container;
	}

	@Nullable
	private Chassis findOrCreateChassis(EdiPostingContext inEdiPostingContext, GateAppointment inAppointment) {
		// Find or create the Chassis
		Chassis chassis = null;
		String chsId = inAppointment.getGapptChassisId();
		EquipType chsType = inAppointment.getGapptChassisEquipType();
		String chsTypeId = null;
		if (chsType != null) {
			chsTypeId = chsType.getEqtypId();
		}
		ScopedBizUnit line = inAppointment.getGapptLineOperator();
		try {
			chassis = Chassis.findOrCreateChassis(chsId, chsTypeId, DataSourceEnum.EDI_APPOINTMENT);
			if (line != null) {
				EquipmentState.upgradeEqOperator(chassis, line, DataSourceEnum.EDI_APPOINTMENT);
			}
		} catch (BizViolation bv) {
			inEdiPostingContext.addViolation(bv);
		}
		return chassis;
	}

	/**
	 * Method to find the vessel visit from the appointment transaction document
	 */
	@Nullable
	protected CarrierVisit findVesselVisit(EdiPostingContext inEdiPostingContext, EdiVesselVisit inVesselVisit, EdiFacility inEdiFacility) {

		if (inVesselVisit == null) {
			return null;
		}

		RoutingPoint pol = extractRoutingPoint(inEdiPostingContext, inVesselVisit.getLoadPort(), OrdersField.EQO_POL);

		CarrierVisit cv = null;
		try {
			cv = getArgoEdiFacade().findVesselVisit(inEdiPostingContext, inEdiFacility, inVesselVisit, null, pol, false);
		} catch (BizViolation bv) {
			inEdiPostingContext.addViolation(bv);
		}
		return cv;
	}

	@Nullable
	private GateAppointment createNewAppointment(EdiPostingContext inEdiPostingContext,
												 AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
												 Gate inGate, TruckerFriendlyTranSubTypeEnum inTranSubType) {
		AppointmentTimeSlot slot;
		Boolean exactTimeSlot = false;
		if(inEdiAppointment.getAppointmentTime() != null){
			exactTimeSlot = true;
		}
		Date apptDate = mergeDateTime(inEdiPostingContext,inEdiAppointment);
		IAppointmentManager appMgr = (IAppointmentManager) Roastery.getBean(IAppointmentManager.BEAN_ID);
		try {
			//slot = appMgr.getAppointmentTimeSlot(inGate.getGateGkey(), inTranSubType.getInternalTranSubTypeEnum(), apptDate, null, exactTimeSlot);
			//APMT Mantis-5766 and Mantis-5271 temp: bypassing the lock creation until fixed in product
			slot = getAppointmentTimeSlot(inGate.getGateGkey(), inTranSubType.getInternalTranSubTypeEnum(), apptDate, null, exactTimeSlot,null);
		} catch (BizViolation bv) {
			inEdiPostingContext.addViolation(bv);
			return null;
		}
		// 2008-02-26 jkathir v1.5.H ARGO-10170 Gate Appointment validation rules are called when we create an appointment.
		// Need to create appointment, related entities initally and finally to triggger the validation rules.
		// Added a new method and constructor which accepts appointment state as parameter to avoid the gate appointment validation rules initially.
		return GateAppointment.createGateAppointment(_appNbr, inGate, apptDate, slot, inTranSubType);
	}
	//temp workaround
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

	private static IAppointmentManager _appMgr = (IAppointmentManager) Roastery.getBean(IAppointmentManager.BEAN_ID);
	String SLOT_START = "slot-start";
	String SLOT_END = "slot-end";
	String AVAILABLE_SLOTS = "available-slots";

	//

	Long _appNbr = null;

	private void postBaseAttributes(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
									GateAppointment inAppointment) {

		// Set appointment time
		Date dateTime = mergeDateTime(inEdiPostingContext, inEdiAppointment);
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_REQUESTED_DATE, dateTime);

		// 2008-02-23 jkathir v1.5.H ARGO-10128 Added a new method resolveScopedBizUnit() - to resolve the scoped biz unit
		// using Id (or) Name (or) Code & Agency
		if (inEdiAppointment.getEdiTruckingCompany() != null) {
			String trkcoId = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyId();
			String trkcoName = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyName();
			String trkcoCode = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyCode();
			String trckCodeAgency = inEdiAppointment.getEdiTruckingCompany().getTruckingCompanyCodeAgency();
			ScopedBizUnit bzu = resolveScopedBizUnit(trkcoId, trkcoName, trkcoCode, trckCodeAgency, BizRoleEnum.HAULIER, null);
			//2008-04-02 kramu v1.5.N ARGO-10840 RejectTrcukingCompanyUnknown biz task will take care if its null, if configured.
			TruckingCompany truckingCompany;
			if (bzu != null) {
				//2011-10-28, EPonnuru ARGO-32461 Unclear error message if an appointment posts for 'obsolete' trucking company
				if (LifeCycleStateEnum.OBSOLETE.equals(bzu.getLifeCycleState())) {
					BizViolation bv = BizViolation.create(RoadApptsPropertyKeys.DELETED_TRUCKING_COMPANY_CAN_NOT_HAVE_APPOINTMENT, null, trkcoId);
					inEdiPostingContext.addViolation(bv);
				}
				truckingCompany = (TruckingCompany) HibernateApi.getInstance().downcast(bzu, TruckingCompany.class);
			} else {
				//todo make this configurable
				truckingCompany = TruckingCompany.createTruckingCompany(trkcoId);
				truckingCompany.setFieldValue(ArgoRefField.BZU_NAME, trkcoName);
			}
			inAppointment.setGapptTruckingCompany(truckingCompany);
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_TRUCKING_COMPANY, truckingCompany);
			if (inEdiAppointment.getEdiTruck() != null) {
				String truckId = inEdiAppointment.getEdiTruck().getTruckId();
				String licenseNbr = inEdiAppointment.getEdiTruck().getLicenseNbr();
				inAppointment.setGapptTruckId(truckId);
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_TRUCK_ID, truckId);
				inAppointment.setGapptTruckLicenseNbr(licenseNbr);
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_TRUCK_LICENSE_NBR, licenseNbr);
			}
		}

		// Set line operator
		if (inEdiAppointment.getEdiShippingLine() != null) {
			String lineCode = inEdiAppointment.getEdiShippingLine().getShippingLineCode();
			String lineCodeAgency = inEdiAppointment.getEdiShippingLine().getShippingLineCodeAgency();
			if (lineCode != null) {
				ScopedBizUnit line = ScopedBizUnit.resolveScopedBizUnit(lineCode, lineCodeAgency, BizRoleEnum.LINEOP);
				if (line == null) {
					BizViolation bv = BizViolation.create(ArgoPropertyKeys.INVALID_OPERATOR_ID, null, lineCode);
					inEdiPostingContext.addViolation(bv);
				} else {
					inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_LINE_OPERATOR, line);
				}
			}
		}

		EdiTruck ediTruck = inEdiAppointment.getEdiTruck();
		if (ediTruck != null) {
			//todo add external truck id to appointment entity (or use flex fields)
			//String truckLicenseNbr = ediTruck.getLicenseNbr();
			//String truckId = ediTruck.getTruckId();
			//String electronicId = ediTruck.getElectronicId();
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_TRUCK_LICENSE_NBR, ediTruck.getLicenseNbr());
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_TRUCK_ID, ediTruck.getTruckId());
		}

		EdiDriver ediDriver = inEdiAppointment.getEdiDriver();
		if (ediDriver != null) {
			TruckDriver driver = TruckDriver.findDriverByCardIdOrLicenseNbr(ediDriver.getCardId(), ediDriver.getLicenseNbr());
			if (driver != null) {
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_TRUCK_DRIVER, driver);
			} else {
				BizViolation bv = BizWarning.create(RoadApptsPropertyKeys.DRIVER_ID_DOES_NOT_EXIST, null, ediDriver.getCardId());
				inEdiPostingContext.addWarning(bv);
			}
		}

		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_IMPORT_RELEASE_NBR, inEdiAppointment.getReleaseNbr());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_IS_XRAY_REQUIRED, inEdiAppointment.getIsXrayInspectionRequired());
	}

	private void postOrder(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
						   GateAppointment inAppointment, CarrierVisit inObCv) {

		String eqoNbr = inEdiAppointment.getOrderNbr();
		ScopedBizUnit line = inAppointment.getGapptLineOperator();

		LOGGER.info("postOrder eqoNbr:" + eqoNbr);
		if (eqoNbr != null && eqoNbr.length() > 0 && line != null) {
			TranSubTypeEnum tranType = inAppointment.getGapptTranType().getInternalTranSubTypeEnum();
			EquipmentOrder eqo = null;
			try {
				if (TranSubTypeEnum.RE.equals(tranType) || TranSubTypeEnum.RI.equals(tranType)) {
					//2008-03-19 kramu v1.5.L ARGO-10561 the API in Booking.java expects the vessel visit to find the unique booking
					eqo = Booking.findBookingByUniquenessCriteria(eqoNbr, line, inObCv);
				} else if (TranSubTypeEnum.DM.equals(tranType)) {
					//2008-03-20 PAC v1.5.3

					eqo = EquipmentDeliveryOrder.findEquipmentDeliveryOrder(eqoNbr, line);
					if (eqo == null) {
						eqo = Booking.findBookingByUniquenessCriteria(eqoNbr, line, null);
					}
				} else if (TranSubTypeEnum.DC.equals(tranType)) {
					eqo = EquipmentDeliveryOrder.findEquipmentDeliveryOrder(eqoNbr, line);
				}
				//WF#892222 -  attach ERO to RM on COPINO posting
				else if(TranSubTypeEnum.RM.equals(tranType)) {
					eqo = EquipmentReceiveOrder.findEroByUniquenessCriteria(eqoNbr, line);
				}
			} catch (BizViolation bv) {
				inEdiPostingContext.addViolation(bv);
			}

			if (eqo == null) {
				// 2008-02-21 jkathir v1.5.G ARGO-10076 used line operator id instead of gkey in error message
				//   BizViolation bv = BizViolation.create(RoadPropertyKeys.GATE__ORDER_NOT_FOUND_FOR_LINE, null, eqoNbr, line.getBzuId());
				//  inEdiPostingContext.addViolation(bv);
			} else {
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_ORDER, eqo);
			}
		}
	}

	private void postOrderItem(EdiPostingContext inEdiPostingContext, GateAppointment inAppointment) {

		EqBaseOrder baseEqo = inAppointment.getGapptOrder();

		if (baseEqo != null) {
			EquipmentOrder eqo = EquipmentOrder.resolveEqoFromEqbo(baseEqo);
			EquipmentOrderItem item;
			TranSubTypeEnum tranType = inAppointment.getGapptTranType().getInternalTranSubTypeEnum();

			EquipType eqType = inAppointment.getGapptCtrEquipType();
			if (TranSubTypeEnum.DC.equals(tranType)) {
				eqType = inAppointment.getGapptChassisEquipType();
			}

			if (eqType != null) {
				try {
					if (TranSubTypeEnum.RE.equals(tranType)) {
						item = eqo.findMatchingItemRcv(eqType, false, false);
					} else {
						item = eqo.findMatchingItemDsp(eqType, false, false);
					}
					inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_ORDER_ITEM, item);
				} catch (BizViolation bv) {
					inEdiPostingContext.addViolation(bv);
				}
			}
		}
	}

	private void postChassisAttributes(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
									   GateAppointment inAppointment) {

		// Post Chassis ID
		String chsId = inEdiAppointment.getChassisId();
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CHASSIS_ID, chsId);

		// Post Chassis Accessory ID
		String chsAccId = inEdiAppointment.getChassisAccessoryId();
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CHASSIS_ACCESSORY_ID, chsAccId);

		// Post Chassis Is Owners field
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CHS_IS_OWNERS, inEdiAppointment.getChassisIsOwners());

		// Post chassis type
		String chsTypeId = inEdiAppointment.getChassisType();
		if (chsTypeId != null) {
			EquipType chsType = EquipType.findEquipType(chsTypeId);
			if (chsType == null) {
				BizViolation bv = BizViolation.create(ArgoPropertyKeys.EQUIPMENT_NOT_FOUND, null, chsTypeId);
				inEdiPostingContext.addViolation(bv);
				//CSDV-3096 - Added Unknown eqiupment type to avoid null pointer exception
				inAppointment.setGapptChassisEquipType(EquipType.getUnknownEquipType());
				//CSDV-3096
			}
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CHASSIS_EQUIP_TYPE, chsType);
		}
	}

	private void postContainerAttributes(EdiPostingContext inEdiPostingContext,
										 AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, GateAppointment inAppointment) {

		// Post container ID
		String ctrId = inEdiAppointment.getContainerId();
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CTR_ID, ctrId);

		// Post container Accessory ID
		String ctrAccId = inEdiAppointment.getContainerAccessoryId();
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CTR_ACCESSORY_ID, ctrAccId);

		// Post container equipment type
		String ctrTypeId = inEdiAppointment.getContainerType();
		if (ctrTypeId != null) {
			EquipType ctrType = EquipType.findEquipType(ctrTypeId);
			if (ctrType == null) {
				BizViolation bv = BizViolation.create(ArgoPropertyKeys.EQUIPMENT_NOT_FOUND, null, ctrTypeId);
				inEdiPostingContext.addViolation(bv);
				//CSDV-3096 - Added Unknown eqiupment type to avoid null pointer exception
				inAppointment.setGapptCtrEquipType(EquipType.getUnknownEquipType());
				//CSDV-3096
			}
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CTR_EQUIP_TYPE, ctrType);
		}
	}

	private void postGrossWt(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
							 GateAppointment inAppointment) {
		EdiWeight ediWeight = inEdiAppointment.getGrossWeight();
		if (ediWeight != null) {
			String grossWtStr = ediWeight.getWtValue();
			String grossWtUnit = ediWeight.getWtUnit();
			if (grossWtStr != null) {
				Double grossWt = null;
				try {
					grossWt = convertWtToKg(grossWtStr, grossWtUnit, InventoryField.UNIT_GOODS_AND_CTR_WT_KG);
				} catch (UnitUtils.UnknownUnitException uue) {
					BizViolation violation = BizViolation.create(InventoryPropertyKeys.STOWPLAN_UNKNOWN_MEASUREMENT_UNIT, null, grossWtUnit,
							InventoryField.UNIT_GOODS_AND_CTR_WT_KG, uue);
					inEdiPostingContext.addViolation(violation);
				}
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CTR_GROSS_WEIGHT, grossWt);
			}
		}
	}

	private void postSeals(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
						   GateAppointment inAppointment) {

		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CTR_SEAL_NBR1, inEdiAppointment.getSealNbr1());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CTR_SEAL_NBR2, inEdiAppointment.getSealNbr2());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CTR_SEAL_NBR3, inEdiAppointment.getSealNbr3());
		inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CTR_SEAL_NBR4, inEdiAppointment.getSealNbr4());
	}

	private void postAgents(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
							GateAppointment inAppointment) {
		ScopedBizUnit agent1 = getAgent(inEdiAppointment, inEdiAppointment.getContainerAgent1());
		if (agent1 != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_AGENT1, agent1);
		}
		ScopedBizUnit agent2 = getAgent(inEdiAppointment, inEdiAppointment.getContainerAgent2());
		if (agent2 != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_AGENT2, agent2);
		}
	}

	private void postRouting(EdiPostingContext inEdiPostingContext, GateAppointment inAppointment, Routing inRouting) {

		if (inRouting == null) {
			return;
		}

		RoutingPoint pol = inRouting.getRtgPOL();
		if (pol != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_POL, pol);
		}
		RoutingPoint pod1 = inRouting.getRtgPOD1();
		if (pod1 != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_POD1, pod1);
		}
		RoutingPoint pod2 = inRouting.getRtgPOD2();
		if (pod2 != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_POD2, pod2);
		}
		RoutingPoint opt1 = inRouting.getRtgOPT1();
		if (opt1 != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_POD1_OPTIONAL, opt1);
		}
		RoutingPoint opt2 = inRouting.getRtgOPT2();
		if (opt2 != null) {
			inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_POD2_OPTIONAL, opt2);
		}
	}

	private void postCommodity(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
							   GateAppointment inAppointment) {
		EdiCommodity ediCommodity = inEdiAppointment.getEdiCommodity();
		if (ediCommodity != null) {
			String consigneeId = ediCommodity.getConsignee();
			if (isNotEmpty(consigneeId)) {
				ScopedBizUnit consignee = Shipper.findOrCreateShipperByIdOrName(consigneeId);
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_CONSIGNEE, consignee);
			}
			String shipperId = ediCommodity.getShipper();
			if (isNotEmpty(shipperId)) {
				ScopedBizUnit shipper = Shipper.findOrCreateShipperByIdOrName(shipperId);
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_SHIPPER, shipper);
			}
			String origin = ediCommodity.getOrigin();
			if (isNotEmpty(origin)) {
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_ORIGIN, origin);
			}
			String destination = ediCommodity.getDestination();
			if (isNotEmpty(destination)) {
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_DESTINATION, destination);
			}
		}
	}

	private ScopedBizUnit getAgent(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
								   EdiAgent inAgent) {
		ScopedBizUnit agent = null;
		if (inAgent != null) {
			String agentId = inAgent.getAgentId();
			String agentName = inAgent.getAgentName();
			String agentCode = inAgent.getAgentCode();
			String agentCodeAgency = inAgent.getAgentCodeAgency();
			agent = resolveScopedBizUnit(agentId, agentName, agentCode, agentCodeAgency, BizRoleEnum.AGENT, LifeCycleStateEnum.ACTIVE);
		}
		return agent;
	}

	private void postFreightKind(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
								 GateAppointment inAppointment) {

		String freightKindStr = inEdiAppointment.getFreightKind();
		if (freightKindStr != null) {
			FreightKindEnum freightKind = FreightKindEnum.getEnum(freightKindStr);
			if (freightKind == null) {
				BizViolation bv = BizViolation.create(InventoryPropertyKeys.STOWPLAN_UNKNOWN_MEASUREMENT_UNIT, null, freightKindStr);
				inEdiPostingContext.addViolation(bv);
			} else {
				inEdiPostingContext.updateField(inAppointment, RoadApptsField.GAPPT_FREIGHT_KIND, freightKind);
			}
		}
	}

	/**
	 * Validates the appointment date whether is null of past date.
	 *
	 * @param inEdiAppointment
	 * @throws BizViolation
	 */
	private void validateAppointmentDate(AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) throws BizViolation {
		if (inEdiAppointment.getAppointmentDate() == null) {
			throw BizViolation.create(RoadApptsPropertyKeys.APPOINTMENT_DATE_NULL, null);
		}
	}

	@Nullable
	private GateAppointment findOrCreateEqAppointment(EdiPostingContext inEdiPostingContext,
													  AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, Gate inGate,
													  TruckerFriendlyTranSubTypeEnum inTranType, Facility inFacility) throws BizViolation {

		GateAppointment newAppointment = null;
		GateAppointment oldAppointment = findAppointmentById(inEdiPostingContext, inEdiAppointment.getAppointmentId(), inFacility);

		if (oldAppointment == null) {
			String eqId = inEdiAppointment.getContainerId();
			if (TruckerFriendlyTranSubTypeEnum.DOC.equals(inTranType)) {
				eqId = inEdiAppointment.getChassisId();
			}
			if (inGate != null) {
				// Can't identify an existing appointment without EQ ID, since pickup appointments may not have an EQ ID
				if (isNotEmpty(eqId)) {
					// At PNC several trucks can make an appointment to do the same move.
					EdiTruck ediTruck = inEdiAppointment.getEdiTruck();
					String truckLicNbr = null;
					if (ediTruck != null) {
						truckLicNbr = ediTruck.getLicenseNbr();
					}
					AppointmentFinder af = (AppointmentFinder) Roastery.getBean(AppointmentFinder.BEAN_ID);
					if (RoadConfig.ALLOW_MULTIPLE_TRUCKS.isOn(ContextHelper.getThreadUserContext()) && truckLicNbr != null) {
						oldAppointment = af.findAppointmentByEquipmentIdAndTruckLic(
								inGate, eqId, inTranType, truckLicNbr, AppointmentStateEnum.CREATED);
					} else {
						oldAppointment = af.findAppointmentByEquipmentId(inGate, eqId, inTranType, AppointmentStateEnum.CREATED);
					}
				}
				if (oldAppointment == null) {
					newAppointment = createNewAppointment(inEdiPostingContext, inEdiAppointment, inGate, inTranType);
				} else {
					Calendar appointmentTime = inEdiAppointment.getAppointmentTime();
					Date requestedDate = oldAppointment.getGapptRequestedDate();
					Date appointmentDate = ArgoEdiUtils.convertLocalToUtcDate(inEdiAppointment.getAppointmentDate(),
							inEdiPostingContext.getTimeZone());
					if (appointmentTime != null || requestedDate.getTime() != appointmentDate.getTime()) {
						if (wrongTimeSlot(inEdiPostingContext, oldAppointment, inEdiAppointment)) {
							// Existing appointment is for a different time slot, make a new appointment
							newAppointment = createNewAndDeleteOldAppointment(inEdiPostingContext, inEdiAppointment, inGate, inTranType,
									oldAppointment);
						}
					}
					if (newAppointment == null) {
						newAppointment = oldAppointment;
					}
				}
			}
		} else {
			validateAppointment(inTranType, oldAppointment);
			Calendar appointmentTime = inEdiAppointment.getAppointmentTime();
			Date requestedDate = oldAppointment.getGapptRequestedDate();
			Date appointmentDate = ArgoEdiUtils.convertLocalToUtcDate(inEdiAppointment.getAppointmentDate(), inEdiPostingContext.getTimeZone());
			if (appointmentTime != null || requestedDate.getTime() != appointmentDate.getTime()) {
				if (wrongTimeSlot(inEdiPostingContext, oldAppointment, inEdiAppointment)) {
					// Existing appointment is for a different time slot, make a new appointment
					newAppointment = createNewAndDeleteOldAppointment(inEdiPostingContext, inEdiAppointment, inGate, inTranType, oldAppointment);
				}
			}
			if (newAppointment == null) {
				newAppointment = oldAppointment;
			}
		}
		return newAppointment;
	}

	@Nullable
	private GateAppointment createNewAndDeleteOldAppointment(EdiPostingContext inEdiPostingContext,
															 AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment,
															 Gate inGate, TruckerFriendlyTranSubTypeEnum inTranType,
															 GateAppointment inOldAppointment) {
		GateAppointment newAppointment = createNewAppointment(inEdiPostingContext, inEdiAppointment, inGate, inTranType);
		if (newAppointment != null) {
			// Delete old appointment if we create a new appointment, otherwise leave the old appointment
			String eqId = inOldAppointment.getGapptCtrId();
			if (TruckerFriendlyTranSubTypeEnum.DOC.equals(inTranType)) {
				eqId = inOldAppointment.getGapptChassisId();
			}
			HibernateApi.getInstance().delete(inOldAppointment);
			//30-Jun-15
			HibernateApi.getInstance().flush();
			//inEdiPostingContext.addWarning(BizWarning.create(RoadApptsPropertyKeys.DUPLICATE_APPT_DELETED, null, inTranType, eqId));
		}
		return newAppointment;
	}

	@Nullable
	private GateAppointment findAppointmentById(EdiPostingContext inEdiPostingContext, String inAppId, Facility inFacility) {
		GateAppointment appointment = null;
		if (inAppId != null) {
			if (StringUtils.isNumeric(inAppId)) {
				appointment = GateAppointment.findGateAppointment(new Long(inAppId), null, inFacility.getPrimaryKey());
			} else {
				BizViolation bv = BizViolation.create(RoadPropertyKeys.APPOINTMENT_ID_IS_INVALID, null, inAppId);
				inEdiPostingContext.addViolation(bv);
			}
		}
		return appointment;
	}

	private boolean wrongTimeSlot(EdiPostingContext inEdiPostingContext, GateAppointment inOldAppointment,
								  AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {

		AppointmentTimeSlot oldSlot = inOldAppointment.getGapptTimeSlot();
		Calendar mergedDateAndTime;
		Calendar date = inEdiAppointment.getAppointmentDate();
		Calendar time = inEdiAppointment.getAppointmentTime();
		if (date != null && time != null) {
			mergedDateAndTime = ArgoEdiUtils.mergeDateAndTime(inEdiAppointment.getAppointmentDate().getTime(), inEdiAppointment.getAppointmentTime().getTime());
		} else {
			mergedDateAndTime = date;
		}
		mergedDateAndTime.setTimeZone(inEdiPostingContext.getTimeZone());
		Date convertedApptDt = mergedDateAndTime.getTime();

		//return oldSlot.checkTimeInSlot(convertedApptDt) != 0;

		//Sophia Robertson 30/06/15  before and after tolerances are not taken into account when deciding if the time slot is wrong
		boolean wrongTimeSlot = true;
		if (oldSlot.isTimeOk(convertedApptDt) && !oldSlot.isEarly(convertedApptDt) && !oldSlot.isLate(convertedApptDt)){
			wrongTimeSlot = false;
		}
		return wrongTimeSlot;
		//return !oldSlot.isTimeOk(convertedApptDt);

	}



	@Nullable
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
			BizViolation bv = BizViolation.create(RoadApptsPropertyKeys.ERROR_CANNOT_DETERMINE_TYPE, (Throwable) null, null, null,parms);
			inEdiPostingContext.addViolation(bv);
		}
		return apptType;
	}

	@Nullable
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

	@Nullable
	private Gate determineGate(EdiPostingContext inEdiPostingContext,
							   AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment, Facility inFcy) {
		String gateId = inEdiAppointment.getAppointmentGateId();
		Gate gate = null;
		if (gateId == null) {
			List gates = Gate.findAllGatesForFacility(inFcy);
			if (gates.size() == 0) {
				BizViolation bv = BizViolation.create(RoadPropertyKeys.GATE__FACILITY_HAS_NO_GATE, null);
				inEdiPostingContext.addViolation(bv);
			} else if (gates.size() > 1) {
				Iterator it = gates.iterator();
				List gateIdList = new ArrayList();
				Gate tempGate = null;
				while (it.hasNext()) {
					gate = (Gate) it.next();
					if (gate.getGateApptRuleSet() != null) {
						gateIdList.add(gate.getGateId());
						tempGate = gate;
					}
				}
				// 2008-07-29 skumaravel v1.7.B ARGO-12485 handled multiple gates
				if (gateIdList.size() == 0) {
					BizViolation bv = BizViolation.create(RoadPropertyKeys.GATE__NO_APPOINTMENT_RULESET_CONFIGURED, null, inFcy);
					inEdiPostingContext.addViolation(bv);
				} else if (gateIdList.size() == 1) {
					return tempGate;
				} else {
					BizViolation bv = BizViolation.create(RoadPropertyKeys.GATE__MULTIPLE_GATE_WITH_APPOINTMENT_RULESET, null,
							inFcy, gateIdList.toString());
					inEdiPostingContext.addViolation(bv);
				}
			} else {
				gate = (Gate) gates.get(0);
			}
		} else {
			gate = Gate.findGateById(gateId);
			if (gate == null) {
				BizViolation bv = BizViolation.create(RoadPropertyKeys.GATE__INVALID_GATE_ID, null, gateId);
				inEdiPostingContext.addViolation(bv);
			}
		}
		return gate;
	}

	/*2008-04-03 kramu v1.5.N ARGO-10866 New method added which takes the equipment id and updates the respective Unit & UnitFacilityVisit
  with the flex fields from the Appointment xml. */

	private void postFlexFields(EdiPostingContext inEdiPostingContext, EdiFlexFields inEdiFlexFields,
								GateAppointment inAppointment, Facility inFacility) {
		if (inFacility == null) {
			return;
		}
		String eqId = inAppointment.getGapptCtrId();
		if (eqId == null) {
			eqId = inAppointment.getGapptChassisId();
		}
		Equipment eq = Equipment.findEquipment(eqId);
		if (eq != null) {
			Unit unit = getUnitFinder().findActiveUnit(inFacility.getFcyComplex(), eq);
			if (unit != null) {
				postFlexFields(inEdiPostingContext, unit, inEdiFlexFields);
				UnitFacilityVisit ufv = unit.getUfvForFacilityLiveOnly(inFacility);
				if (ufv != null) {
					postFlexFields(inEdiPostingContext, ufv, inEdiFlexFields);
				}
			}
		}
	}

	private Hazards getHazards(EdiPostingContext inEdiPostingContext, AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment) {
		EdiHazard[] ediHazardArray = inEdiAppointment.getEdiHazardArray();
		if (ediHazardArray == null || ediHazardArray.length == 0) {
			return null;
		}

		ValueObject[] hazardItemsVaoArray = new ValueObject[ediHazardArray.length];
		for (int haziCount = 0; haziCount < ediHazardArray.length; haziCount++) {
			hazardItemsVaoArray[haziCount] = ediHazard2ValueObject(inEdiPostingContext, ediHazardArray[haziCount]);
		}

		Hazards hazards = Hazards.createHazardsEntity();
		hazards.setHazardsHazItemsVao(hazardItemsVaoArray);
		return hazards;
	}

	private Date mergeDateTime(EdiPostingContext inEdiPostingContext,
							   AppointmentTransactionDocument.AppointmentTransaction inEdiAppointment){
		Date apptDate = ArgoEdiUtils.convertLocalToUtcDate(inEdiAppointment.getAppointmentDate(), inEdiPostingContext.getTimeZone());
		Date apptTime = ArgoEdiUtils.convertLocalToUtcDate(inEdiAppointment.getAppointmentTime(), inEdiPostingContext.getTimeZone());
		Calendar convertedApptDt = ArgoEdiUtils.mergeDateAndTime(apptDate, apptTime);
		return convertedApptDt.getTime();
	}

	private static String AGENCY_BIC = "BIC";
	private static String AGENCY_SCAC = "SCAC";
	private static String PICKUP = "PICKUP";
	private static String DROPOFF = "DROPOFF";
	private UnitManager _um = (UnitManager) Roastery.getBean(UnitManager.BEAN_ID);
	private static Logger LOGGER = Logger.getLogger(PANAppointmentPoster.class);

	private static String BARGE_GATE = "BARGE";
	private static String RAIL_GATE = "RAIL";
	//16-May-2017 : Create the Prean for the RAIL ITT gate.
	private static String RAIL_ITT_GATE = "RAIL_ITT"
}
