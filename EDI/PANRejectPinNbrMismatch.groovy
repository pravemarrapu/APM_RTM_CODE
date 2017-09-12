/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 */


import com.navis.argo.ContextHelper
import com.navis.argo.business.reference.LineOperator
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.external.road.EGateTaskInterceptor
import com.navis.framework.util.DateUtil
import com.navis.framework.util.TransactionParms
import com.navis.framework.util.internationalization.PropertyKey
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollector
import com.navis.framework.util.message.MessageCollectorFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.units.Unit
import com.navis.road.RoadPropertyKeys
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.util.RoadBizUtil
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.external.framework.util.ExtensionUtils

public class PANRejectPinNbrMismatch extends AbstractGateTaskInterceptor implements EGateTaskInterceptor {

  public void execute(TransactionAndVisitHolder inWfCtx) {

	MessageCollector origMsgCollector = RoadBizUtil.getMessageCollector();
	MessageCollector thisTaskMsgCollector =  MessageCollectorFactory.createMessageCollector();
	TransactionParms.getBoundParms().setMessageCollector(thisTaskMsgCollector);

	executeCustom(inWfCtx);

	setGapptFlexFieldPinNbr(inWfCtx.getTran(),inWfCtx.getAppt());

	if (thisTaskMsgCollector.hasError()){
	  _panGateTaskHelper.recordPreanError(inWfCtx, getClass().getSimpleName(), RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector));
	}

	TransactionParms.getBoundParms().setMessageCollector(origMsgCollector);
  }

  public void executeCustom(TransactionAndVisitHolder inDao) {
	TruckTransaction tran = inDao.getTran();

	if (tran != null) {

	  String unitPinNbr = null;
	  String gapptPinNbr = tran.getTranPinNbr();
	  
	  GateAppointment gappt = inDao.getAppt();

	  Unit unit = tran.getTranUnit();

	  if (unit != null)  {
		unitPinNbr = unit.getUnitRouting() != null ?  unit.getUnitRouting().getRtgPinNbr(): null;
	  }
	  log("gapptPinNbr:" + gapptPinNbr);
	  log("unitPinNbr:" + unitPinNbr);

	  if (unitPinNbr != gapptPinNbr) {

		RoadBizUtil.getMessageCollector().appendMessage(
				MessageLevel.SEVERE,
				RoadPropertyKeys.GATE__PIN_NBR_MISMATCH,
				null,
				null
		);
	  }
	  else if (unitPinNbr != null && pinExpired(gappt, unit)) {
		getMessageCollector().appendMessage(MessageLevel.SEVERE,
				CUSTOM_PIN_NBR_EXPIRED,
				"PIN Nbr has expired",
				null);

	  }


	}

  }

  private boolean checkLineOperatorForPinBasedDelivery(LineOperator inLineOperator) {
	if (inLineOperator != null && inLineOperator.getLineopUsePinNbr() != null) {
	  return inLineOperator.getLineopUsePinNbr();
	}
	return false;
  }
  private boolean pinExpired(GateAppointment inGappt, Unit inUnit) {

	boolean pinExpired = false;

	Date requestedDate = inGappt.getGapptRequestedDate();
	Date unitPinExpiryDate = inUnit.getUnitImportDeliveryOrderExpiryDate();

	if (unitPinExpiryDate != null) {
	  pinExpired = requestedDate.after(unitPinExpiryDate) && !DateUtil.isSameDay(requestedDate, unitPinExpiryDate, ContextHelper.getThreadUserTimezone());

	}

	return  pinExpired;

  }

  void setGapptFlexFieldPinNbr(TruckTransaction inTran, GateAppointment inGappt){
	if (inTran != null && inGappt != null) {
	  inGappt.setFieldValue(_panFields.PREAN_PIN, inTran.getTranPinNbr());

	}
  }

  private static PropertyKey CUSTOM_PIN_NBR_EXPIRED = PropertyKeyFactory.valueOf("CUSTOM_PIN_NBR_EXPIRED");
  private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
  private static def _panGateTaskHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateTaskHelper");

}