import java.util.List;

import com.navis.apex.business.model.GroovyInjectionBase;
import com.navis.argo.ContextHelper;
import com.navis.argo.business.atoms.EdiMessageClassEnum;
import com.navis.argo.business.model.GeneralReference;
import com.navis.argo.business.portal.EdiExtractDao;
import com.navis.edi.EdiEntity;
import com.navis.edi.EdiField;
import com.navis.edi.EdiPropertyKeys;
import com.navis.edi.business.api.EdiExtractManager;
import com.navis.edi.business.atoms.EdiMessageDirectionEnum;
import com.navis.edi.business.entity.EdiSession;
import com.navis.edi.business.entity.EdiTransaction;
import com.navis.external.framework.util.ExtensionUtils;
import com.navis.framework.SimpleSavedQueryEntity;
import com.navis.framework.SimpleSavedQueryField;
import com.navis.framework.business.Roastery;
import com.navis.framework.business.atoms.LifeCycleStateEnum;
import com.navis.framework.business.atoms.PredicateParmEnum;
import com.navis.framework.business.atoms.PredicateVerbEnum;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.query.business.SavedPredicate;
import com.navis.framework.util.ValueHolder;
import com.navis.framework.util.ValueObject;
import com.navis.framework.util.internationalization.PropertyKeyFactory;
import com.navis.framework.util.message.MessageCollector;
import com.navis.framework.util.message.MessageCollectorFactory;
import com.navis.framework.util.message.MessageCollectorUtils;
import com.navis.framework.util.message.MessageLevel;
import com.navis.road.RoadApptsEntity;
import com.navis.road.RoadApptsField;
import com.navis.road.RoadField;
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.edi.business.api.EdiCompoundField
import com.navis.argo.business.api.ArgoUtils
import com.navis.framework.util.BizViolation;

/*
 *  04-11-2013 CSDV-1503 Made the logic to get a session ID generic
 *  04-02-2014 CSDV-1630 validate the existence of a COPINO transaction for the prean.EDI_TRAN_GKEY (technically is not related to the 3.0.1 upgrade)
 *  31-03-2014 CSDV-1753 fixed message collector instantiation, added meaningful logging
 *                       IMPORTANT! ARGOEDI009 setting for all non standard APERAK edi sessions needs to be set to 0
 *  20-05-2014 CSDV-1411 Fix: do not delete COPINO 13 prean if it is for an unknown unit
 *  25/06/14 CSDV-2152 EDI_TRAN_GKEY is not a Long, so no need for parsing
 *  11/07/14 CSDV-2169  modified cleanupProcessedPrean: if system is configured to send mutiple APERAKs for COPINO 13, do not delete the COPINO 13 preans

 */
public class PANSendNonStdAperakHelper extends GroovyInjectionBase {


	public void execute(String inTransportMode) {

		log("PANSendNonStdAperakHelper start "+ArgoUtils.timeNow() + "transport mode: "+inTransportMode);

		List<GateAppointment> preans = getPreansToSendMsgsFor(inTransportMode);

		if (preans.isEmpty()) {

			getMessageCollector().appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("NO_PREANS_TO_SEND_APERAKS"),
					"No preannouncements to send Aperaks for", null);

		}

		else {

			EdiSession aperakSession = null;

			for(GateAppointment prean : preans) {

				Long ediTranGkey = prean.getField(_panFields.EDI_TRAN_GKEY); //Long.parseLong(prean.getFieldString(_panFields.EDI_TRAN_GKEY));

				if (!doesEdiTranExist(ediTranGkey)) {

					logWarn("COPINO tran for the prean "+ prean.getApptNbr() +  "is not found");

				}
				else {

					aperakSession = getEdiSession(prean,inTransportMode);

					if (aperakSession != null) {

					  MessageCollector mc = extractAperak(aperakSession, ediTranGkey);


					  if (!mc.hasError() || (mc.containsMessage(EdiPropertyKeys.UNABLE_TO_SEND_EXTRACTED_FILE))) {

						cleanupProcessedPrean(prean);

					  }
					  else {
						MessageCollectorUtils.appendMessages(getMessageCollector(), mc);
					  }

					}

				}


			}

		}
	  log("PANSendNonStdAperakHelper end "+ArgoUtils.timeNow() + "transport mode: "+inTransportMode);
	}

	private EdiSession getEdiSession(GateAppointment prean, String inTransportMode) {

		EdiSession ediSession = null;
		String ediPartnerName =  prean.getFieldString(_panFields.EDI_PARTNER_NAME);

		if (ediPartnerName != null) {
			String ediSessionName = getEdiSessionName(ediPartnerName, inTransportMode);
			if (ediSessionName != null) {
				ediSession = findEdiSession(ediSessionName);
				if (ediSession == null) {
					registerError("No Session found for edi session name"+ediSessionName);

				}
			}

		}
		else {
			log("EDI partner name in prean "+prean.getApptNbr()+"is not set");
		}

		return ediSession;
	}



	private List<GateAppointment> getPreansToSendMsgsFor(String inTransportMode) {

		List<GateAppointment> preans = new ArrayList<>();

		DomainQuery dq = QueryUtils.createDomainQuery(RoadApptsEntity.GATE_APPOINTMENT)
						  .addDqPredicate(PredicateFactory.eq(_panFields.SEND_MSG, "YES"))
						  .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.getCompoundMetafieldId(RoadApptsField.GAPPT_GATE, RoadField.GATE_ID),
								  inTransportMode))
						  .addDqPredicate(PredicateFactory.isNotNull(_panFields.EDI_TRAN_GKEY));

		preans.addAll(HibernateApi.getInstance().findEntitiesByDomainQuery(dq));

		return preans;
	}

	public MessageCollector  extractAperak(EdiSession inAperakSession, Long inEdiTranGkey) {

		EdiSession aperakSess = inAperakSession;

		//Set the last run date back NN days, so it would pick all the applicable transactions created
		//within last NN days
		//- 200 for now, to use in my test system

		Calendar cal = Calendar.getInstance();
		cal.add( Calendar.DAY_OF_YEAR, LAST_RUN_TIME_OFFSET);
		Date newDate =  cal.getTime();

		aperakSess.setFieldValue(EdiField.EDISESS_LAST_RUN_TIMESTAMP, newDate);

		setFilter(inEdiTranGkey, aperakSess);

		// Extract

		final EdiExtractDao ediExtractDao = new EdiExtractDao();
		ediExtractDao.setSessionGkey(aperakSess.getEdisessGkey());

		return _extractMgr.extractEdiSession(ediExtractDao);

	}

  private void setFilter(Long inEdiTranGkey, EdiSession inAperakSess) {

	ValueObject vaoAll = createVaoEntry(0, null, PredicateVerbEnum.AND, null);
	ValueHolder[] childPredicates = new ValueHolder[1];
	//childPredicates[0] = createVaoEntry(0, EdiCompoundField.EDITRAN_BATCH_MSGCLASS, PredicateVerbEnum.EQ, EdiMessageClassEnum.APPOINTMENT);
	childPredicates[0] = createVaoEntry(0, EdiField.EDITRAN_GKEY, PredicateVerbEnum.EQ, inEdiTranGkey);

	vaoAll.setFieldValue(SimpleSavedQueryField.PRDCT_CHILD_PRDCT_LIST, childPredicates);

	SavedPredicate sp = new SavedPredicate(vaoAll);
	inAperakSess.setFieldValue(EdiField.EDISESS_PREDICATE, sp);


	HibernateApi.getInstance().update(inAperakSess);
	HibernateApi.getInstance().flush();
  }

  private ValueObject createVaoEntry(long inOrder, MetafieldId inMetafieldId, PredicateVerbEnum inVerbEnum, Object inValue) {
		ValueObject vao = new ValueObject(SimpleSavedQueryEntity.SAVED_PREDICATE);
		vao.setFieldValue(SimpleSavedQueryField.PRDCT_METAFIELD, inMetafieldId);
		vao.setFieldValue(SimpleSavedQueryField.PRDCT_VERB, inVerbEnum);
		vao.setFieldValue(SimpleSavedQueryField.PRDCT_VALUE, inValue);
		vao.setFieldValue(SimpleSavedQueryField.PRDCT_UI_VALUE, null);
		vao.setFieldValue(SimpleSavedQueryField.PRDCT_ORDER, new Long(inOrder));
		vao.setFieldValue(SimpleSavedQueryField.PRDCT_NEGATED, Boolean.FALSE);
		vao.setFieldValue(SimpleSavedQueryField.PRDCT_PARAMETER_TYPE, PredicateParmEnum.NO_PARM);
		return vao;
	}


	 private void cleanupProcessedPrean(GateAppointment inPrean) {

		 String rspMsgType = (String)inPrean.getFieldValue(_panFields.RESPONSE_MSG_TYPE);

		 if(STATUS_UPDATE.equals(rspMsgType) && inPrean.getGapptUnit() != null && !multipleAperaksSent()) {
		   HibernateApi.getInstance().delete(inPrean);
		 }

		 else {
			   inPrean.setFieldValue(_panFields.SEND_MSG, null);
			   HibernateApi.getInstance().update(inPrean);
			   log("PANSendNonStdAperakHelper.cleanupProcessedPrean: Set SEND_MSG to NULL");
		 }
	}

	private EdiSession findEdiSession(String inEdiSessionName) {

		DomainQuery dq = QueryUtils.createDomainQuery(EdiEntity.EDI_SESSION)
				.addDqPredicate(PredicateFactory.eq(EdiField.EDISESS_MESSAGE_CLASS, EdiMessageClassEnum.ACKNOWLEDGEMENT))
				.addDqPredicate(PredicateFactory.eq(EdiField.EDISESS_DIRECTION, EdiMessageDirectionEnum.S))
				.addDqPredicate(PredicateFactory.eq(EdiField.EDISESS_LIFE_CYCLE_STATE, LifeCycleStateEnum.ACTIVE))
				.addDqPredicate(PredicateFactory.eq(EdiField.EDISESS_NAME, inEdiSessionName));


		List<EdiSession> sessionList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);

		if (sessionList == null || sessionList.isEmpty()) {
			return null;
		}
		if (sessionList.size() == 1) {
			return sessionList.get(0);
		}

		return null;
	}

	private String getEdiSessionName(String inEdiPartnerName, String inGateId) {

		String ediSessionName = null;

		GeneralReference ediSessionGf = GeneralReference.findUniqueEntryById("NON_STD_APERAK_OUT","SESSION_ID",inEdiPartnerName, inGateId);

		if (ediSessionGf == null) {
			registerError("General Reference NON_STD_APERAK_OUT/SESSION_ID/"+inEdiPartnerName+"/"+inGateId+" is not found");
		}
		else {
			ediSessionName = ediSessionGf.getRefValue1();

			if (ediSessionName == null) {
				registerError("General Reference NON_STD_APERAK_OUT/SESSION_ID/"+inEdiPartnerName+"/"+inGateId+" does not contain a value");
			}

		}

		return ediSessionName;
	}

	private boolean doesEdiTranExist(Long inEdiTranGkey) {

		DomainQuery dq = QueryUtils.createDomainQuery(EdiEntity.EDI_TRANSACTION)
						 .addDqPredicate(PredicateFactory.eq(EdiField.EDITRAN_GKEY, inEdiTranGkey));


		return HibernateApi.getInstance().existsByDomainQuery(dq);
	}

	 private boolean multipleAperaksSent() {

	   boolean multipleAperaksSent = false;

	   GeneralReference genRef = GeneralReference.findUniqueEntryById("NON_STD_APERAK_OUT","COPINO_13","MULTIPLE_APERAKS");
	   multipleAperaksSent = genRef != null && "YES".equals(genRef.getRefValue1());
	   if (!multipleAperaksSent) {
		   log("GeneralReference record NON_STD_APERAK_OUT/COPINO_13/MULTIPLE_APERAKS does not exist")
	   }
	   return multipleAperaksSent;
	 }

	static EdiExtractManager _extractMgr = (EdiExtractManager) Roastery.getBean(EdiExtractManager.BEAN_ID);

	static int LAST_RUN_TIME_OFFSET = -200;

	private static def _panFields = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANFields");

	static String STATUS_UPDATE = "STATUS_UPDATE";

}
