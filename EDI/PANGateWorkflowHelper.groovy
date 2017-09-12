/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *

 * $Id: PANGateWorkflowHelper.groovy 227995 2015-09-07 09:43:25Z kumarsu3 $

 * Modifier: K.Sundar 23-07-2-20015 CSDV-3084 Method:setTranEqoIf Appointment EQO Number is null then Gate-Transaction
 * EQO Number will be considered

 * 20-05-2014 CSDV-1914 update unit custom field prean processed time with the current time
 *
 * 25/06/14 CSDV-2152 Removed checking SHOULD_VALIDATION_RUN field value
 *
 * Date 05/07/14 CSDV-2159 when setting SEND_MSG, only check for different errors if the prean status is NOK
 *
 * Date 10/09/2014 CSDV-2234 set Datasource to EDI_APPOINTMENT and replace ReadOrderItem with custom PANReadOrderItem
 *
 * 05/11/14   CSDV-1703 - copy the order nbr entered by the user in the ui to the Prean Uknown Eqo Nbr value
 *
 * 05/11/14 CSDV-2415  pre-set tran Eqo nbr for DM transactions, so ReadOrder doesn't try to populate tran eqo with a booking
 *
 * 05/02/15 CSDV-2753 (APMT Mantis-5459)  customize ReadOrder
 *
 * 11/Feb/2015 CSDV-2731 added setNokCancelDelay functionality
 * Modified By: Praveen Babu
 Date 11/09/2017 APMT #25 - Update the right validation run id to Prean.
 Modified By - Pradeep Arya 08-Sept-17 WF#892222 commented out code throwing null pointer exception
 */

import org.apache.commons.lang.StringUtils

import javax.naming.Context;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.navis.argo.ContextHelper;
import com.navis.argo.business.api.ArgoUtils;
import com.navis.argo.business.atoms.DataSourceEnum;
import com.navis.argo.business.model.GeneralReference;
import com.navis.external.framework.AbstractExtensionCallback;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.orders.business.eqorders.EquipmentDeliveryOrder;
import com.navis.road.business.appointment.model.GateAppointment;
import com.navis.road.business.atoms.GateClientTypeEnum;
import com.navis.road.business.atoms.TranSubTypeEnum;
import com.navis.road.business.model.TruckTransaction;
import com.navis.road.business.workflow.TransactionAndVisitHolder;
import com.navis.road.portal.configuration.CachedGateTask;

public class PANGateWorkflowHelper extends AbstractExtensionCallback {

	private void preWorkflow(TransactionAndVisitHolder inWfCtx, List<CachedGateTask> inTasks) {

		TruckTransaction tran = inWfCtx.getTran();

		if (tran != null) {

			ContextHelper.setThreadDataSource(DataSourceEnum.EDI_APPOINTMENT);

			GateAppointment prean = inWfCtx.getAppt();

			customizeReadUnitActive(inTasks);

			customizeReadOrder(inTasks);

			customizeReadOrderItem(inTasks);


			if (isWorkflowExecutedOnDispatch(inTasks)) {
				log("isWorkflowExecutedOnDispatch:true");
				inWfCtx.put(WORKFLOW_EXECUTED_ON_DISPATCH_KEY, "YES");
			}
			else {
				log("isWorkflowExecutedOnDispatch:false");
				setTranFields(tran, prean);

				Date validationDate = new Date();

				prean.setFieldValue(_panFields.PREAN_LAST_VALIDATION_DATE, validationDate);

				prean.setFieldValue(_panFields.PREAN_VALIDATION_RUN_ID, validationDate.getTime());

				inWfCtx.put(VALIDATION_RUN_ID_KEY, validationDate.getTime());

				inWfCtx.put(NEW_PREAN_STATUS_KEY, "OK");

				//Pradeep Arya WF#892222 - commented out the code since it's throwing null pointer exception
				//Praveen Babu WF#892222 - the below setting to the thread EDI execution is not required, hence, removed it.
			}



		}

	}

	private void customizeReadUnitActive(List<CachedGateTask> inTasks) {

		for (CachedGateTask task : inTasks) {

			if ("ReadUnitActive".equals(task.getId())) {
				if (task.getExtension() == null) {
					task.setCustomCode("PANReadUnitActive");
					task._extensionId = "PANReadUnitActive";
				}
				break;
			}
		}

	}

	private void customizeReadOrderItem(List<CachedGateTask> inTasks) {

		for (CachedGateTask task : inTasks) {

			if ("ReadOrderItem".equals(task.getId())) {
				if (task.getExtension() == null) {
					task.setCustomCode("PANReadOrderItem");
					task._extensionId = "PANReadOrderItem";
				}
				break;
			}
		}

	}

	private void customizeReadOrder(List<CachedGateTask> inTasks) {

		for (CachedGateTask task : inTasks) {

			if ("ReadOrder".equals(task.getId())) {
				if (task.getExtension() == null) {
					task.setCustomCode("PANReadOrder");
					task._extensionId = "PANReadOrder";
				}
				break;
			}
		}

	}

	private void unCustomizeReadOrder(List<CachedGateTask> inTasks) {

		for (CachedGateTask task : inTasks) {

			if ("ReadOrder".equals(task.getId())) {
				//if (task.getExtension() == null) {
				task.setCustomCode(null);
				task._extensionId = null;
				//}
				break;
			}
		}

	}

	private boolean isWorkflowExecutedOnDispatch(List<CachedGateTask> inTasks) {

		boolean isWorkflowExecutedOnDispatch = true;

		for (CachedGateTask task : inTasks) {

			if (!task.isDefault() && !task.isIncludedDispatch()) {
				isWorkflowExecutedOnDispatch = false;
				break;
			}
		}
		return isWorkflowExecutedOnDispatch;
	}

	private void setTranFields(TruckTransaction inTran, GateAppointment inPrean) {

		setTranEqo(inPrean, inTran);

		if (TranSubTypeEnum.RE.equals(inTran.getTranSubType())
				&& inTran.getTranCarrierVisit() != null && inTran.getTranCarrierVisit().isGenericCv()) {
			//&& inTran.getTranCarrierVisit() != null && DUMMY_OUTBOUND_VESSEL_VISIT.equals(inTran.getTranCarrierVisit().getCvId())) {
			inTran.setTranCarrierVisit(null);
		}

		inTran.setTranFacility(inPrean.getGate().getGateFacility());

	}

	//pre-set Eqo nbr for DM transactions only, so ReadOrder doesn't populate tran eqo with a booking

	private void setTranEqo(GateAppointment inPrean, TruckTransaction inTran) {

		if (TranSubTypeEnum.DM.equals(inTran.getTranSubType())){

			String eqoNbr = inPrean.getFieldString(_panFields.PREAN_EQO_NBR);

			//CSDV-3084 - Start
			if (eqoNbr == null) {
				eqoNbr = inTran.getTranEqoNbr()
			}
			//CSDV-3084 - End

			if (eqoNbr == null) {
				registerError("Eqo Nbr is null");
				return;
			}
			if (inTran.getTranLine() == null) {
				registerError("Cannot search for EDO w/o a Line Operator provided");
				return;
			}
			EquipmentDeliveryOrder eqo = EquipmentDeliveryOrder.findEquipmentDeliveryOrder(eqoNbr, inTran.getTranLine());
			if (eqo != null) {
				inTran.setTranEqo(eqo);
			}
			else {
				inTran.setTranEqoNbr(null);
			}
		}
	}


	private void postWorkflow(TransactionAndVisitHolder inWfCtx) {
		log("WORKFLOW_EXECUTED_ON_DISPATCH_KEY:" + inWfCtx.get(WORKFLOW_EXECUTED_ON_DISPATCH_KEY));

		if (!inWfCtx.isAborted() && !"YES".equals(inWfCtx.get(WORKFLOW_EXECUTED_ON_DISPATCH_KEY))) {

			TruckTransaction tran = inWfCtx.getTran();


			if (tran != null) {

				GateAppointment prean = inWfCtx.getAppt();
				String oldPreanStatus = (String) prean.getFieldValue(_panFields.PREAN_STATUS);
				String newPreanStatus = (String) inWfCtx.get(NEW_PREAN_STATUS_KEY);
				log("oldPreanStatus:$oldPreanStatus");
				log("newPreanStatus:$newPreanStatus");

				String currErrorAperakCodes = _preanErrorUtil.getErrRefAperakCodesByPreanAndValidationRunId(prean.getGapptGkey(),inWfCtx.get(VALIDATION_RUN_ID_KEY));
				/*boolean differentErrorsRecorded;
                if(prean.getGapptGate() != null && RAIL_ITT_GATE.equalsIgnoreCase(prean.getGapptGate().getGateId())){
                    differentErrorsRecorded =  false;
                }else{
                    differentErrorsRecorded =  _preanErrorUtil.diffErrorsRecorded(prean.getGapptGkey(), inWfCtx.get(VALIDATION_RUN_ID_KEY),currErrorAperakCodes);
                }*/
				log("Post work flow :: Gate information :: "+prean.getGapptGate())
				boolean differentErrorsRecorded =  false;

				//send a message if validation was initiated by a COPINO process, or status has changed or different errors have been recorded
				if (GateClientTypeEnum.EDI.equals(inWfCtx.getGateClientType()) || !newPreanStatus.equals(oldPreanStatus) || (newPreanStatus.equals("NOK") && differentErrorsRecorded)) {
					prean.setFieldValue(_panFields.SEND_MSG, "YES");

				}

				setNokCancelDelay(inWfCtx, oldPreanStatus, newPreanStatus, currErrorAperakCodes, differentErrorsRecorded);

				setPreanEqoNbr(tran, prean);

				//set Prean Routing
				if (tran.getTranCarrierVisit() != null && !tran.getTranCarrierVisit().equals(prean.getGapptVesselVisit())) {
					prean.setGapptVesselVisit(tran.getTranCarrierVisit());
				}

				Unit unit = tran.getTranUnit();
				UnitFacilityVisit ufv = null;

				if (unit != null) {
					if (prean.getGapptUnit() == null || !prean.getGapptUnit().getUnitGkey().equals(unit.getUnitGkey())) {
						prean.setGapptUnit(unit);
					};
				}


				if (!newPreanStatus.equals(oldPreanStatus)) {
					prean.setFieldValue(_panFields.PREAN_STATUS, newPreanStatus);
				}

				if (STATUS_UPDATE.equals(prean.getFieldString(_panFields.RESPONSE_MSG_TYPE))) {
					processStatusUpdatePrean(prean);
				}
				else {
					if (unit != null) {

						ufv = unit.getUfvForFacilityLiveOnly(prean.getGapptGate().getGateFacility());
						if (ufv != null) {
							propagatePreanStatusToUfv(ufv, newPreanStatus, tran.isReceival(), prean);
						}
					}
				}

				if (unit != null) {
					unit.setFieldValue(PREAN_PROCESSED_TIME, ArgoUtils.timeNow());
				}
			}

		}

	}

	private void setNokCancelDelay(TransactionAndVisitHolder inWfCtx, String inOldPreanStatus, String inNewPreanStatus, String inCurrErrorAperakCodes,
								   boolean inDifferentErrorsRecorded) {


		if ("TRUCK".equals(inWfCtx.getAppt().getGapptGate().getGateId()) || "RAIL_ITT".equals(inWfCtx.getAppt().getGapptGate().getGateId())) {

			List<GeneralReference> references = (List<GeneralReference>)GeneralReference.findAllEntriesById("PREANNOUNCEMENT","NOK_CANCL_DELAY_COND","ERR_CODES_COMBO");

			if (references != null && !references.isEmpty()) {

				boolean nokCancelDelay = false;

				if (inNewPreanStatus.equals("NOK"))  {

					//if (!inNewPreanStatus.equals(inOldPreanStatus) || inDifferentErrorsRecorded) {

					nokCancelDelay = calculateNokCancelDelay(inCurrErrorAperakCodes, references);

					//}

				}
				inWfCtx.getAppt().setFieldValue(_panFields.NOK_CANCEL_DELAY, nokCancelDelay);

			}

		}
	}


	private boolean calculateNokCancelDelay(String inCurrErrorAperakCodes, List<GeneralReference> inReferences) {

		boolean nokCancelDelay = false;



		String primaryErrCode = "";

		for(GeneralReference reference : inReferences) {

			primaryErrCode = reference.getRefValue1() == null ?  "" : reference.getRefValue1();

			String errList = inCurrErrorAperakCodes;

			if (errList.contains(primaryErrCode)) {

				errList = StringUtils.remove(errList,primaryErrCode);

				String theRestOfStringPattern = reference.getRefValue2() == null ?  "" : reference.getRefValue2();
				theRestOfStringPattern = theRestOfStringPattern.replace("*",".*").replace("d","\\d").replace(")",")+");
				theRestOfStringPattern = StringUtils.remove(theRestOfStringPattern,",");

				Pattern p = Pattern.compile(theRestOfStringPattern);

				Matcher m = p.matcher(errList);

				if (m.matches()) {
					nokCancelDelay = true;
					break;
				}

			}

		}

		return nokCancelDelay;
	}

	void processStatusUpdatePrean(GateAppointment inPrean) {
		//wipe out order and order item to prevent the wrong tally counter
		log("inside processStatusUpdatePrean");
		inPrean.setGapptOrder(null);
		inPrean.setGapptOrderItem(null);

	}

	void propagatePreanStatusToUfv(UnitFacilityVisit inUfv, String inNewPreanStatus, boolean isReceival, GateAppointment InPrean) {
		if (isReceival) {
			inUfv.setFieldValue(_panFields.UFV_PREAN_RECEIVAL_STATUS, inNewPreanStatus);
		}
		else {
			inUfv.setFieldValue(_panFields.UFV_PREAN_DELIVERY_STATUS, inNewPreanStatus);
		}

	}


	//copy the order nbr entered by the user in the ui to the Prean Uknown Eqo Nbr value

	private void setPreanEqoNbr(TruckTransaction inTran, GateAppointment inPrean) {

		String tmpPreanEqoNbr = inPrean.getFieldString(_panFields.PREAN_EQO_NBR);
		String tranEqoNbr = inTran.getTranEqoNbr();


		if (tranEqoNbr != null && !tranEqoNbr.equals(tmpPreanEqoNbr)) {
			inPrean.setFieldValue(_panFields.PREAN_EQO_NBR, tranEqoNbr);
		}

	}


	private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
	private static def _preanErrorUtil = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANPreanErrorUtil");


	public static String NEW_PREAN_STATUS_KEY = "newPreanStatus";
	public static String VALIDATION_RUN_ID_KEY = "validationRunId";
	public static String WORKFLOW_EXECUTED_ON_DISPATCH_KEY = "workflowExecutedOnDispatch";

	private static String STATUS_UPDATE = "STATUS_UPDATE";
	private static String RAIL_ITT_GATE = "RAIL_ITT";

	private static MetafieldId PREAN_PROCESSED_TIME = MetafieldIdFactory.valueOf("customFlexFields.unitCustomDFFPreanProcessedTime");

}