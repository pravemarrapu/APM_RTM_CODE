/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 * ==================   FOR DELIVERIES ONLY  =====================
 *
 * 27-Nov-2013 CSDV-1528 Fixed: When a COPINO-13 is received for outbound mode RAIL and the unit is known for outbound visit GEN_BARGE, the mode wasn't updated.
 *
 * Date: 26/02/2014 CSDV-1658 Set unit POD for RAIL and TRUCK (in addition to BARGE)
 *
 * Date: 11/07/2014  CSDV-2168 Check EDI Posting Rules for POD and OB Carrier
 *                             run only if invoked by COPINO posting or  UNIT_ACTIVATE event (when a new unit is created)
 *
 * Date: 08/08/2014  CSDV-2220 made PANApplyPreanFieldsToUnit a code extension;
 *                             use it by customizing an unused task that preceeds RejectUnitServiceRules (tran types: DI, DM, DE)
 *
 * Date 12-08-2014 CSDV-2230 Barge/Rail receivals: don't update Arrival Position if it's already been populated by CONSIST/STOWPLAN
 *
 * Date 18-09-2014 CSDV-2230 Merge back the lost change applied by Danny in CSDV-2005
 *
 * Date 14-11-2014 CSDV-2439 COPINO 13 sets unit pod if applicable; Important! All relevant ports must be in GEN_BARGE rotation(EDI will fail otherwise).
 *
 * Date 01-04-2015 CSDV-2825 (AMPT Mantis-4673) modified setIntendedObCv : if unit DRAY status is OFFSITE, do not update intneded OB, so that gate out finctionality copies ot to the new advised unit
 *
 * Date 28-04-2015 CSDV-2832  A concomitant clean-up : De Facto this task is only used for Deliveries; The equivalent functionality in Receivals is implemented in PANCreateContainerPeradvisedVisit,
 *                                                     so removed all the Receival related code to avoid confusion
 */


import java.io.Serializable;
import java.util.List;

import com.navis.argo.ContextHelper;
import com.navis.argo.business.atoms.DrayStatusEnum;
import com.navis.argo.business.model.CarrierVisit;
import com.navis.argo.business.model.EdiPostingContext;
import com.navis.argo.business.model.ICallBack;
import com.navis.argo.business.reference.RoutingPoint;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.external.road.AbstractGateTaskInterceptor;
import com.navis.external.road.EGateTaskInterceptor;
import com.navis.inventory.InventoryField;
import com.navis.inventory.business.units.Routing;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.model.TruckTransaction;
import com.navis.road.business.workflow.TransactionAndVisitHolder;

public class PANApplyPreanFieldsToUnit extends AbstractGateTaskInterceptor implements EGateTaskInterceptor {

	public void execute(TransactionAndVisitHolder inWfCtx) {

		//Used for Deliveries only

		if (inWfCtx.getTran().isReceival()) {
			return;
		}

		if (!shouldProceed(inWfCtx)) {
			return;
		}

		GateAppointment prean = inWfCtx.getAppt();
		TruckTransaction tran = inWfCtx.getTran();

		if (prean != null &&  tran != null) {

			Unit unit = tran.getTranUnit();
			UnitFacilityVisit ufv = tran.getTranUfv();

			String gateId = tran.getGate().getGateId();

			if (BARGE.equals(gateId) || (RAIL.equals(gateId))) {

				CarrierVisit landsideCarrierVisit =  (CarrierVisit)prean.getField(_panFields.PREAN_LANDSIDE_CARRIER_VISIT);

				// Set unit Intended and Declared O/B Carrier and pod if applicable

				if (ufv != null){
					if (STATUS_UPDATE.equals(prean.getFieldValue(_panFields.RESPONSE_MSG_TYPE))) {
						CarrierVisit ufvCurrentActualObCv = ufv.getUfvActualObCv();
						if (ufvCurrentActualObCv != null && (ufvCurrentActualObCv.isGenericCv() || GEN_BARGE.equals(ufvCurrentActualObCv.getCvId()))) {

							if (BARGE.equals(gateId)) {
								if (!GEN_BARGE.equals(ufvCurrentActualObCv.getCvId())) {
									landsideCarrierVisit = CarrierVisit.findVesselVisit(tran.getGate().getGateFacility(), GEN_BARGE);
									ufv.updateObCv(landsideCarrierVisit);
									setUnitPod(unit, prean.getGapptPod1());
								}
							}
							else {
								if (!TRAIN.equals(ufvCurrentActualObCv.getCvCarrierMode().getKey())) {
									landsideCarrierVisit = CarrierVisit.getGenericTrainVisit(ContextHelper.getThreadComplex());
									ufv.updateObCv(landsideCarrierVisit);
									setUnitPod(unit, prean.getGapptPod1());
								}
							}

						}

					}
					else {
						//ufv.updateObCv(landsideCarrierVisit);
						setIntendedObCv(ufv,landsideCarrierVisit,unit);
						setUnitPod(unit, prean.getGapptPod1());

					}

				}


			}
			//TRUCK
			else {

				if (ufv != null){

					/*ufv.updateObCv(CarrierVisit.getGenericTruckVisit(ContextHelper.getThreadComplex()));
					Routing unitRtg = unit.ensureUnitRouting();
					unitRtg.setRtgTruckingCompany(prean.getGapptTruckingCompany());
					*/
					setIntendedObCv(ufv,CarrierVisit.getGenericTruckVisit(ContextHelper.getThreadComplex()),unit);
					RoutingPoint landsidePod = prean.getGapptPod1();
					setUnitPod(unit, landsidePod);
				}

			}

		}
	}
	// Should proceed if called by EDI or UNIT_ACTIVATE general notice
	private boolean shouldProceed(TransactionAndVisitHolder inWfCtx) {

		boolean  shouldProceed = ContextHelper.getThreadEdiPostingContext() != null;

		if (!shouldProceed){

			List<Serializable> extensionData = inWfCtx.getExtensionData();

			if (extensionData != null && !extensionData.isEmpty() && extensionData.get(0) instanceof String) {
				shouldProceed = "UNIT_ACTIVATE".equals(extensionData.get(0));
			}

		}

		return shouldProceed;
	}

	private void setUnitPod(Unit inUnit, RoutingPoint inLandsidePod) {

		if (inLandsidePod != null && inUnit != null) {

			//Routing unitRtg = inUnit.ensureUnitRouting();
			Routing routing = null;

			routing = inUnit.getSafeRoutingCopy();

			routing.setRtgPOD1(inLandsidePod);

			EdiPostingContext ediContext = ContextHelper.getThreadEdiPostingContext();

			if (ediContext != null) {
				ediContext.updateField(inUnit, InventoryField.UNIT_ROUTING, routing);
			}
			else {
				inUnit.setUnitRouting(routing);
			}

		}
	}

	private void setIntendedObCv(final UnitFacilityVisit inUfv, final CarrierVisit inObCv, Unit inUnit) {

		EdiPostingContext ediContext = ContextHelper.getThreadEdiPostingContext();
		CarrierVisit currIntendedObCv = inUfv.getUfvIntendedObCv();

		if (ediContext != null) {
			ediContext.updateField(inUfv, InventoryField.UFV_INTENDED_OB_CV, inObCv, true, new ICallBack() {
				public void execute() {
					inUfv.updateObCv(inObCv);
				}
			});
		}else {
			inUfv.updateObCv(inObCv);
		}

		if  (DrayStatusEnum.OFFSITE.equals(inUnit.getUnitDrayStatus())) {
			inUfv.setUfvIntendedObCv(currIntendedObCv);
		}
	}


	private static String GEN_BARGE = "GEN_BARGE";
	private static String TRAIN = "TRAIN";
	private static String STATUS_UPDATE = "STATUS_UPDATE";
	private static String BARGE = "BARGE";
	private static String RAIL = "RAIL";
	private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
}