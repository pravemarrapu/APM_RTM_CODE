import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.xmlbeans.XmlObject;

import com.navis.argo.ActivityTransactionsDocument;
import com.navis.argo.ArgoPropertyKeys;
import com.navis.argo.ContextHelper;
import com.navis.argo.ActivityTransactionDocument.ActivityTransaction;
import com.navis.argo.ActivityTransactionsDocument.ActivityTransactions;
import com.navis.argo.business.model.Facility;
import com.navis.argo.business.reference.Equipment;
import com.navis.external.edi.entity.AbstractEdiPostInterceptor;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.util.BizFailure;
import com.navis.framework.util.internationalization.PropertyKey;
import com.navis.inventory.business.api.UnitFinder;
import com.navis.inventory.business.atoms.UfvTransitStateEnum;
import com.navis.inventory.business.atoms.UnitVisitStateEnum;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.road.RoadPropertyKeys;
import com.navis.road.business.appointment.api.AppointmentFinder;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.AppointmentStateEnum;
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum;
import com.navis.road.business.model.Gate;
import com.navis.road.business.util.TransactionTypeUtil;

/**
 * This groovy is used to update the flex string of the unit when the TRACOM message is received with the sequence number of the unit from ECT.
 * @author Praveen Babu M
 * Applies at: Post code extension interceptor of the EDI session that is used to recieve the TRACOM message.
 */

class RTMTracomPostInterceptor extends AbstractEdiPostInterceptor {

	private Logger LOGGER = Logger.getLogger(this.class)

	@Override
	public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
		LOGGER.setLevel(Level.DEBUG);

		LOGGER.debug("Inside the Before EDI post method for TRACOM :: ");
		ActivityTransactionsDocument activityTransactionsDocument = (ActivityTransactionsDocument) inXmlTransactionDocument;
		ActivityTransactions activityTransactions = activityTransactionsDocument.getActivityTransactions();

		ActivityTransaction[] activityTransactionArray = activityTransactions.getActivityTransactionArray();



		if (activityTransactionArray.length != 1) {
			throw BizFailure.create("expected exactly one ActivityTransactionDocument, but inXmlObject contained " + activityTransactionArray.length);
		}
		ActivityTransaction activityTransaction = activityTransactionArray[0];
		Facility currentFacility = Facility.findFacility(activityTransaction.getFacilityId(), ContextHelper.getThreadComplex());
		if (activityTransaction.getEdiFlexFields() != null) {
			String sequenceNbr = activityTransaction.getEdiFlexFields().getUnitFlexString06();
			if (sequenceNbr != null && !sequenceNbr.isEmpty()) {
				if (activityTransaction.getEdiContainer() != null) {
					String containerNbr = activityTransaction.getEdiContainer().getContainerNbr();
					if (containerNbr != null) {
						LOGGER.debug("Inside the Before EDI post method for TRACOM :: Current Container Number :: " + containerNbr + "Current Sequence that is mapped :: " + sequenceNbr);
						Equipment equipment = Equipment.findEquipment(containerNbr);
						if (equipment != null) {
							UnitFinder unitFinder = (UnitFinder) Roastery.getBean("unitFinder");
							Unit unit = unitFinder.findAttachedUnit(ContextHelper.getThreadComplex(), equipment);
							if (unit != null) {
								UnitFacilityVisit ufv = unit.getUfvForFacilityNewest(currentFacility);
								if(ufv != null){
									LOGGER.debug("UFV :: "+ufv+"Current properties of unit :: "+ufv.getUfvVisitState() + " :: Transit State :: "+ ufv.getUfvTransitState() + " :: Is beyond :: "+ ufv.isTransitStateAtMost(UfvTransitStateEnum.S40_YARD))
								}
								if (ufv != null && UnitVisitStateEnum.ACTIVE.equals(ufv.getUfvVisitState()) && ufv.isTransitStateAtMost(UfvTransitStateEnum.S40_YARD)) {
									if(validateAppointmentForUnit(ufv)){
										StringBuilder currentUnitFlexString
										
										if(unit.getUnitFlexString06() != null && !unit.getUnitFlexString06().isEmpty()){
											currentUnitFlexString = new StringBuilder(unit.getUnitFlexString06());
											if(currentUnitFlexString.contains("-")){
												currentUnitFlexString.replace(currentUnitFlexString.indexOf("-")+1, currentUnitFlexString.length(), sequenceNbr);
											}else{
												currentUnitFlexString.append("-").append(sequenceNbr).toString();
											}
										}else {
											currentUnitFlexString = new StringBuilder();
											currentUnitFlexString.append("-").append(sequenceNbr);
										}
										unit.setUnitFlexString06(currentUnitFlexString.toString());
										
										LOGGER.debug("Inside the Before EDI post method for TRACOM :: Updated value of the FLEX String 06 :: " + unit.getUnitFlexString06());
										HibernateApi.getInstance().saveOrUpdate(unit);
										HibernateApi.getInstance().flush();
									}else{
										createBizFaiureMessage(RoadPropertyKeys.GATE__NO_APPOINTMENT_FOR_EQ, inParams, containerNbr);
									}
								} else {
									createBizFaiureMessage(null, inParams, "Unit $containerNbr Transit state is beyond Yard");
								}
							} else {
								createBizFaiureMessage(RoadPropertyKeys.GATE__UNIT_NOT_FOUND, inParams, containerNbr);
							}
						} else {
							createBizFaiureMessage(ArgoPropertyKeys.EQID_UNKNOWN, inParams, containerNbr);
						}
					}
				}
			}
		}

		inParams.put("SKIP_POSTER", Boolean.TRUE);
	}

	private boolean validateAppointmentForUnit(UnitFacilityVisit inUfv) {
		Boolean canUpdate = Boolean.FALSE;
		
		try {
			AppointmentFinder apptFinder =  (AppointmentFinder)Roastery.getBean("appointmentFinder");
			TruckerFriendlyTranSubTypeEnum[] tranSubTypes = new TruckerFriendlyTranSubTypeEnum[10]
			List<TruckerFriendlyTranSubTypeEnum> truckList = (List<TruckerFriendlyTranSubTypeEnum>) TruckerFriendlyTranSubTypeEnum.getList();
			tranSubTypes = truckList.toArray(tranSubTypes)
			GateAppointment gateAppointment = apptFinder.findAppointmentByContainerId(inUfv.getUfvUnit().getUnitId(), tranSubTypes, AppointmentStateEnum.CREATED)
			if (gateAppointment != null) {
				Gate currentPreanGate = gateAppointment.getGapptGate();
				LOGGER.debug("Current Appointment that is fetched :: "+gateAppointment+" Gate :: "+currentPreanGate + " :: Appointment state :: "+gateAppointment.getGapptState()+" isPickUpTXn :: "+isPickUpTransaction(gateAppointment))
				if ( AppointmentStateEnum.CREATED.equals(gateAppointment.getGapptState()) && isPickUpTransaction(gateAppointment)
				&& currentPreanGate != null && currentPreanGate.getGateId().equalsIgnoreCase(RAIL_ITT_GATE)) {
					canUpdate = Boolean.TRUE;
				}
			}
		} catch (Exception inException) {
			log("Exception while retrieving appointment " + inException);
		}
		return canUpdate;
	}

	private boolean isPickUpTransaction(GateAppointment inGateAppointment){
		TruckerFriendlyTranSubTypeEnum currentTranType = inGateAppointment.getGapptTranType();
		LOGGER.debug("cURRENT TRUCK TRAN type :: "+ currentTranType)
		if(currentTranType!= null){
			LOGGER.debug("cURRENT TRAN sub type :: "+ TransactionTypeUtil.getTranSubTypeEnum(currentTranType))
			LOGGER.debug("cURRENT TRAN sub type IS delivery :: "+ TransactionTypeUtil.isDelivery(TransactionTypeUtil.getTranSubTypeEnum(currentTranType)))
			return TransactionTypeUtil.isDelivery(TransactionTypeUtil.getTranSubTypeEnum(currentTranType))
		}
		return false
	}


	private createBizFaiureMessage(PropertyKey inPropertyKey, Map inParams, String errorMessage) {
		BizFailure bv;
		if (inPropertyKey != null) {
			bv = BizFailure.create(inPropertyKey, null);
		} else if (errorMessage != null) {
			bv = BizFailure.createProgrammingFailure(errorMessage);
		}
		if (bv != null) {
			ContextHelper.getThreadMessageCollector().appendMessage(bv);
		}
		inParams.put("SKIP_POSTER", Boolean.TRUE);
	}

	private static final String RAIL_ITT_GATE = "RAIL_ITT"
}
