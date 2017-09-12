package com.weserve.APM.EDI

import com.navis.argo.business.reference.RoutingPoint

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import com.navis.argo.ArgoEntity;
import com.navis.argo.ArgoField;
import com.navis.argo.ContextHelper;
import com.navis.argo.business.api.Serviceable;
import com.navis.argo.business.atoms.DataSourceEnum;
import com.navis.argo.business.atoms.EquipmentOrderSubTypeEnum;
import com.navis.argo.business.atoms.PropertyGroupEnum;
import com.navis.argo.business.model.CarrierVisit;
import com.navis.argo.business.model.Facility;
import com.navis.argo.business.model.PropertySource;
import com.navis.external.framework.AbstractExtensionCallback;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.Ordering;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.inventory.business.api.UnitField;
import com.navis.inventory.business.atoms.UfvTransitStateEnum;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitEquipment;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.orders.business.eqorders.EquipmentOrder;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.services.ServicesEntity;
import com.navis.services.ServicesField;
import com.navis.services.business.rules.EventType;
import com.navis.argo.business.model.GeneralReference
import com.navis.inventory.business.api.UnitManager
import com.navis.framework.business.Roastery
import com.navis.framework.business.atoms.YesOrNoAtom
import com.navis.argo.business.api.IEventType
import com.navis.argo.business.api.ServicesManager;


/*
Date 05/03/2015 CSDV-2733 Moved unit cleanup functionality from  PANApptPostInterceptor into new code extension library  PANPreanUtils (used in both edi and manual prean processing)
						  Modified unit cleanup functionality to
								prevent unit deletion when unit transaction state is INBOUND and unit arrival position was last set by rail consists.
								retain unit Declared/Actual inbound visit and position (this also insures that an advised unit is created by N4 api findOrCreatePreadvisedUnitEdi  when applicable)
								set unit outbound actual visit to GEN_CARRIER
								cancel unit preadvise against  an order (if Prean order and unit order match)
								set unit Prean RCV status to null to insure  PREAN_RECEIVE_PRM being required

Date 04-05-2015 CSDV-2832 moved getCancelNotes to PreanUtils
						  prevent a unit deletion if the unit was created by the Copino 13 prean, but was updated by another process (coparn, loadlist, ETT, manual update, etc)

Date 23/06/2015 CSDV-2832 added cancelPrean which cleans up the unit, gets the cancel note and sets prean send msg to YES if applicable

Date 08/10/2015 CSDV-3188 Retire the unit if applicable

Date 21/10/2015 CSDV-3287 When setting prean unit to null, also set prean ctr ID to null (for EHD case when ctr ID is passed in te orginal prean, but not in the update)

Date 25/11/2015 CSDV-3188 unitFlexString02 is used instead of unitFlexString01 as Can be deleted by prean flag
*/

public class PANPreanUtils extends AbstractExtensionCallback {

	void cleanupPreanUnit(GateAppointment inPrean){

		Unit unit = inPrean.getGapptUnit();
		UnitFacilityVisit ufv = null;
		CarrierVisit landsideCarrierVisit =  (CarrierVisit)inPrean.getField(PREAN_LANDSIDE_CARRIER_VISIT);

		if (unit != null)  {

			cancelUnitToOrderPreadvice(inPrean.isReceival(), EquipmentOrder.resolveEqoFromEqbo(inPrean.getGapptOrder()), unit, inPrean.getGapptGate().getGateFacility());
			clearUnitITTFields(inPrean);

			CarrierVisit genericVisit = CarrierVisit.getGenericCarrierVisit(ContextHelper.getThreadComplex());

			ufv = unit.getUfvForFacilityLiveOnly(inPrean.getGapptGate().getGateFacility());

			if (ufv != null) {
				//do not retire or delete
				if (inPrean.isReceival()){

					if (UfvTransitStateEnum.S20_INBOUND.equals(ufv.getUfvTransitState()) && DataSourceEnum.EDI_CNST.equals(PropertySource.findDataSourceForProperty(ufv, PropertyGroupEnum.POSITION))) {

						inPrean.setGapptUnit(null);
						unit.getUnitRouting().setRtgDeclaredCv(genericVisit);
						HibernateApi.getInstance().flush();
					}

					else {
						//COPINO 13 Dropoff  - @todo: will CSDV-3188 functionalty take care of this?
						if (!ufv.isTransitStateBeyond(UfvTransitStateEnum.S20_INBOUND) && inPrean.wasUnitCreatedInAppointment() && STATUS_UPDATE.equals(inPrean.getFieldString(_panFields.RESPONSE_MSG_TYPE))
								&&  (updatedByOtherProcess(unit.getEntityName(), unit.getPrimaryKey()) || updatedByOtherProcess(ufv.getEntityName(),ufv.getPrimaryKey()))) {

							inPrean.setGapptUnit(null);
							HibernateApi.getInstance().flush();
						}

						if (landsideCarrierVisit != null && landsideCarrierVisit.equals(ufv.getUfvActualIbCv())){
							ufv.updateActualIbCv(genericVisit);
						}

					}

					ufv.setFieldValue(UFV_PREAN_RECEIVAL_STATUS, null);

					//CSDV-3188
					if (inPrean.getGapptUnit() != null) {

						if (YES.equals(unit.getFieldString(CAN_BE_DELETED_BY_PREAN))) {
							String unitId = unit.getUnitId();
							UnitManager um = (UnitManager) Roastery.getBean(UnitManager.BEAN_ID);
							//um.purgeUnit(unit);
							unit.makeRetired();

							/*  IEventType customEvnt = _srvcMgr.getEventType("PREAN_DELETED_UNIT");
							  _srvcMgr.recordEvent(customEvnt, "Deleted unit "+unitId, null, null, inPrean);
							*/
						}

						inPrean.setGapptUnit(null);
						// inPrean.setGapptCtrId(null);
						HibernateApi.getInstance().flush();

					}
				}
				// Pickups (Deliveries)

				else {
					if (landsideCarrierVisit != null && landsideCarrierVisit.equals(ufv.getUfvActualIbCv())){
						ufv.updateObCv(genericVisit);
					}

					if(landsideCarrierVisit == null && inPrean.getGapptGate() != null && inPrean.getGapptGate().getGateId().equals(RAIL_ITT_GATE)){
						ufv.updateObCv(genericVisit);
					}
					ufv.setFieldValue(UFV_PREAN_DELIVERY_STATUS, null);
				}

			}
		}
	}

	/**
	 * Clear the ITT fields for the unit when the appointment is cancelled that is created via the RAIL_ITT gate
	 * @param inGateAppointment
     */
	private void clearUnitITTFields(GateAppointment inGateAppointment) {
		Unit apptUnit = inGateAppointment.getGapptUnit();
		if(apptUnit != null && inGateAppointment.getGapptGate() != null && inGateAppointment.getGapptGate().getGateId().equals(RAIL_ITT_GATE)){
			apptUnit.setUnitFlexString04(null);
			apptUnit.setUnitFlexString06(null);
			apptUnit.setUnitFlexString07(null);
			apptUnit.setUnitFlexString15(null);
			RoutingPoint currentFacility = RoutingPoint.findRoutingPoint("NLRTM");
			if(inGateAppointment.isReceival()){
				apptUnit.getUnitRouting().setRtgPOL(currentFacility)
			}else{
				apptUnit.getUnitRouting().setRtgPOD1(currentFacility)
			}

			HibernateApi.getInstance().save(apptUnit);
			HibernateApi.getInstance().flush();
		}

	}

	private void cancelUnitToOrderPreadvice(boolean inIsReceival, EquipmentOrder inEqo, Unit inUnit, Facility inPreanFcy) {

		UnitFacilityVisit ufv = inUnit.getUfvForFacilityLiveOnly(inPreanFcy);

		if (inEqo != null) {
			UnitEquipment ue = inUnit.getUnitPrimaryUe();

			if (inIsReceival){

				if (EquipmentOrderSubTypeEnum.BOOK.equals(inEqo.getEqboSubType()) || EquipmentOrderSubTypeEnum.RAIL.equals(inEqo.getEqboSubType())) {

					if (ue.getUeDepartureOrderItem() != null) {

						EquipmentOrder preadviseEqo = EquipmentOrder.resolveEqoFromEqbo(ue.getUeDepartureOrderItem().getEqboiOrder());

						if (inEqo.equals(preadviseEqo)) {
							String evntTypeID = EQ_PREADVISE_EVENT_TYPES.get(preadviseEqo.getEqboSubType().getKey());

							if (isOrderSetByPrean(EventType.findEventType(evntTypeID),preadviseEqo, inUnit.getUnitId())) {
								ue.setUeDepartureOrderItem(null);
								ufv.updateObCv(CarrierVisit.getGenericCarrierVisit(ContextHelper.getThreadComplex()));

							}

						}
					}

				} else if (EquipmentOrderSubTypeEnum.ERO.equals(inEqo.getEqboSubType())) {

					if (ue.getUeArrivalOrderItem() != null) {
						EquipmentOrder preadviseEqo = EquipmentOrder.resolveEqoFromEqbo(ue.getUeArrivalOrderItem().getEqboiOrder());

						if (inEqo.equals(preadviseEqo)) {
							String evntTypeID = EQ_PREADVISE_EVENT_TYPES.get(preadviseEqo.getEqboSubType().getKey());

							if (isOrderSetByPrean(EventType.findEventType(evntTypeID),preadviseEqo, inUnit.getUnitId())) {
								ue.setUeArrivalOrderItem(null);
								ufv.updateObCv(CarrierVisit.getGenericCarrierVisit(ContextHelper.getThreadComplex()));
							}


						}
					}
				}

			} else {

				if (ue.getUeDepartureOrderItem() != null) {

					EquipmentOrder reserveForEqo = EquipmentOrder.resolveEqoFromEqbo(ue.getUeDepartureOrderItem().getEqboiOrder());

					if (inEqo.equals(reserveForEqo)) {
						String evntTypeID = "EQ_RESERVE_BKG";

						if (isOrderSetByPrean(EventType.findEventType(evntTypeID),reserveForEqo, inUnit.getUnitId())) {
							ue.setUeDepartureOrderItem(null);
						}


					}
				}


			}


		}

	}

	public boolean isOrderSetByPrean(EventType inEventType, Serviceable inTargetServiceable, String inUnitId) {

		boolean result = true;

		DomainQuery dq = QueryUtils.createDomainQuery(ServicesEntity.EVENT)
				.addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_APPLIED_TO_PRIMARY_KEY, inTargetServiceable.getPrimaryKey()))
				.addDqOrdering(Ordering.desc(ServicesField.EVNT_GKEY))
				.addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_EVENT_TYPE, inEventType.getEvnttypeGkey()))
				.addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_NOTE, inUnitId))
				.addDqPredicate(PredicateFactory.eq(EVENT_APPLIED_BY_PROCESS, "PREAN"));

		dq.setDqMaxResults(1);

		dq.setRequireTotalCount(false);


		Serializable[] eventGkeys = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(dq);
		if (eventGkeys == null || eventGkeys.length == 0) {
			result = false;
		}

		//Event event = (Event) _hibernateApi.load(Event.class, eventGkeys[0]);
		return result;

	}


	public String getCancelNote(String inMsgType, String inCancelNoteType) {

		String cancelNote = CANCELLED_BY_SENDER.equals(inCancelNoteType)  ? "Cancelled by sender" : "Cancelled by terminal";

		GeneralReference gf = GeneralReference.findUniqueEntryById(inMsgType, "CANCEL_NOTE", inCancelNoteType);

		if (gf != null && gf.getRefValue1() != null) {

			cancelNote = gf.getRefValue1();
		}
		else {
			log("General Reference "+ inMsgType+"/CANCEL_NOTE/" + inCancelNoteType + " is not found or value1 is not set");
		}

		return cancelNote;
	}

	public boolean updatedByOtherProcess(String inEntityName, Serializable inPrimaryKey) {

		DomainQuery dq = QueryUtils.createDomainQuery(ArgoEntity.PROPERTY_SOURCE)
				.addDqPredicate(PredicateFactory.eq(ArgoField.PRPSRC_APPLIED_TO_ENTITY, inEntityName))
				.addDqPredicate(PredicateFactory.eq(ArgoField.PRPSRC_APPLIED_TO_PRIMARY_KEY, inPrimaryKey))
				.addDqPredicate(PredicateFactory.ne(ArgoField.PRPSRC_DATA_SOURCE, DataSourceEnum.EDI_APPOINTMENT));

		return HibernateApi.getInstance().existsByDomainQuery(dq);
	}

	public void cancelPrean(GateAppointment inPrean, String inMsgType, String inCancelNoteType, boolean inCleanupUnit){

		if (inCleanupUnit) {
			cleanupPreanUnit(inPrean);
		}

		String cancelNote = getCancelNote(inMsgType,inCancelNoteType);
		inPrean.cancelAppointment(cancelNote);

		setPreanSendMsg(inPrean);
	}

	public void setPreanSendMsg(GateAppointment inPrean) {

		String preanType = inPrean.getFieldString(_panFields.RESPONSE_MSG_TYPE);

		if  (!STATUS_UPDATE.equals(preanType) || (STATUS_UPDATE.equals(preanType) && multipleAperaksSentForStatusUpdateRequest())) {

			inPrean.setFieldValue(_panFields.SEND_MSG, "YES");
		}
	}

	private boolean multipleAperaksSentForStatusUpdateRequest() {

		boolean multipleAperaksSent = false;

		GeneralReference genRef = GeneralReference.findUniqueEntryById("NON_STD_APERAK_OUT","COPINO_13","MULTIPLE_APERAKS");
		multipleAperaksSent = genRef != null && "YES".equals(genRef.getRefValue1());
		if (!multipleAperaksSent) {
			log("GeneralReference record NON_STD_APERAK_OUT/COPINO_13/MULTIPLE_APERAKS does not exist");
		}
		return multipleAperaksSent;
	}


	private static MetafieldId EVENT_APPLIED_BY_PROCESS = MetafieldIdFactory.valueOf("evntFlexString01");

	public static Map<String,String> EQ_PREADVISE_EVENT_TYPES = new HashMap();

	static {

		EQ_PREADVISE_EVENT_TYPES.put("BOOK", "EQ_PREADVISE_BKG");
		EQ_PREADVISE_EVENT_TYPES.put("ERO", "EQ_PREADVISE_ERO");
		EQ_PREADVISE_EVENT_TYPES.put("RAIL", "EQ_PREADVISE_RO");
	}
	private static MetafieldId UFV_PREAN_RECEIVAL_STATUS = UnitField.UFV_FLEX_STRING01;
	public static MetafieldId UFV_PREAN_DELIVERY_STATUS = UnitField.UFV_FLEX_STRING02;
	public static MetafieldId PREAN_LANDSIDE_CARRIER_VISIT =  MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFlandsideCV");

	private static String CANCELLED_BY_SENDER = "CANCELLED_BY_SENDER";

	private static String STATUS_UPDATE = "STATUS_UPDATE";
	private def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");

	private static String YES = "YES";
	private static MetafieldId CAN_BE_DELETED_BY_PREAN = UnitField.UNIT_FLEX_STRING02;
	private static String RAIL_ITT_GATE = "RAIL_ITT";

	private static ServicesManager _srvcMgr = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
}