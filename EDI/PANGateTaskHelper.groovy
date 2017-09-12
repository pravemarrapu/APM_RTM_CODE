
/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 * 10-Oct-2013 CSDV-1410 Get APERAK error text from Gen Refs
 *
 * Date: CSDV-1402 overloaded recordPreanError - added Additional Criteria parameter (to pass as ID-3 to General Reference finder)
 *
 * 19/02/2015 CSDV-2558 error desc: concatenate gen ref value2 and value 3
 */


import com.navis.argo.ContextHelper
import com.navis.argo.business.model.GeneralReference
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.framework.portal.context.PortalApplicationContext
import com.navis.framework.presentation.internationalization.IMessageTranslatorProvider
import com.navis.framework.presentation.internationalization.MessageTranslator
import com.navis.framework.util.internationalization.UserMessage
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.external.framework.util.ExtensionUtils

public class PANGateTaskHelper extends AbstractExtensionCallback {


  public void recordPreanError(TransactionAndVisitHolder inWfCtx, String inTaskName, UserMessage inErrMsg) {


	String tranType =  inWfCtx.getTran().isReceival() ? "RECEIVAL" : "DELIVERY";

	String taskName =  inTaskName.substring(TASK_PREFIX.length());
	if (taskName.length() > 20){
	  taskName = taskName.substring(0,20);
	}

	//String msg = _messageTranslator.getMessage(inErrMsg.getMessageKey(),inErrMsg.getParms());
	recordPreanError(inWfCtx,tranType,taskName, null);

	inWfCtx.put(NEW_PREAN_STATUS_KEY,"NOK");

  }

  public void recordPreanError(TransactionAndVisitHolder inWfCtx, String inTranType, String inValidationType, String inMsg, String inAdditionalCriteria) {
	log("PANGateTaskHelper inWfCtx:${inWfCtx.getAppt().getGapptGkey()} + tranType:$inTranType + taskName:$inValidationType + VALIDATION_RUN_ID_KEY:${inWfCtx.get(VALIDATION_RUN_ID_KEY)}");

	GeneralReference errRef = GeneralReference.findUniqueEntryById(PREAN_CONDITION_REF_TYPE, inTranType, inValidationType, inAdditionalCriteria);

	_preanErrorUtil.recordError(inWfCtx.getAppt(),
			errRef.getRefValue1(),
			errRef.getRefValue2() + (errRef.getRefValue3() == null ? "" : errRef.getRefValue3()),
			inWfCtx.get(VALIDATION_RUN_ID_KEY));

  }


  public void recordPreanError(TransactionAndVisitHolder inWfCtx, String inTranType, String inValidationType, String inMsg) {

	recordPreanError(inWfCtx,inTranType,inValidationType,inMsg, null);

  }

  public MessageTranslator getMessageTranslator() {
	if (_messageTranslator == null) {
	  IMessageTranslatorProvider translatorProvider =
		(IMessageTranslatorProvider) PortalApplicationContext.getBean(IMessageTranslatorProvider.BEAN_ID);
	  _messageTranslator = translatorProvider.createMessageTranslator(Locale.getDefault(), ContextHelper.getThreadUserContext());
	}
	return _messageTranslator;
  }


  public static String NEW_PREAN_STATUS_KEY = "newPreanStatus";
  public static String VALIDATION_RUN_ID_KEY = "validationRunId";

  public String PREAN_CONDITION_REF_TYPE = "PREAN_ERR_CONDITION";
  public String TASK_PREFIX = "PANReject";

  MessageTranslator _messageTranslator = getMessageTranslator();

  private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");
  private static def _preanErrorUtil = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANPreanErrorUtil");
}