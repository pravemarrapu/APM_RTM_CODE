package com.weserve.APM.EDI

import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.xmlbeans.XmlObject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.navis.argo.ActivityTransactionDocument;
import com.navis.argo.ActivityTransactionDocument.ActivityTransaction;
import com.navis.argo.ActivityTransactionsDocument;
import com.navis.argo.ActivityTransactionsDocument.ActivityTransactions;
import com.navis.argo.ArgoPropertyKeys;
import com.navis.argo.ContextHelper;
import com.navis.argo.EdiContainer;
import com.navis.argo.business.api.ArgoEdiUtils;
import com.navis.argo.business.model.Complex;
import com.navis.argo.business.model.Facility;
import com.navis.argo.business.reference.Equipment;
import com.navis.external.edi.entity.AbstractEdiPostInterceptor;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.Ordering;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.util.BizFailure;
import com.navis.framework.util.internationalization.PropertyKey;
import com.navis.inventory.business.api.UnitField;
import com.navis.inventory.business.api.UnitFinder;
import com.navis.inventory.business.atoms.UfvTransitStateEnum;
import com.navis.inventory.business.atoms.UnitVisitStateEnum;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitFacilityVisit;
import com.navis.road.RoadPropertyKeys;

/**
 * This groovy is used for updating the flex string on the unit when the CODECO60 inbound message is received from APMT
 * terminal providing the received time stamp of the unit
 *  at ECT.
 * @author Praveen Babu M
 *	Applies at: EDI CODECO 60 Session as EDI post interceptor.
 */
class RTMCodeco60PostInterceptor extends AbstractEdiPostInterceptor{
	
	private Logger LOGGER = Logger.getLogger(this.class);

	@Override
	public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
		LOGGER.setLevel(Level.DEBUG);
		
		LOGGER.debug("Inside RTMCodeco60PostInterceptor :: start");
		ActivityTransactionsDocument activityTransactionsDocument = (ActivityTransactionsDocument)inXmlTransactionDocument;
		ActivityTransactions activityTransactions = activityTransactionsDocument.getActivityTransactions();
		ActivityTransaction[] activityTransactionArray = activityTransactions.getActivityTransactionArray();
		if(activityTransactionArray.length !=1 ){
			throw BizFailure.create("expected exactly one ActivityTransactionDocument, but inXmlObject contained " + activityTransactionArray.length);
		}
		
		ActivityTransaction activityTransaction = activityTransactionArray[0];
		Facility currentFacility = Facility.findFacility(activityTransaction.getFacilityId(), ContextHelper.getThreadComplex());
		
		if(activityTransaction.getEdiContainer()!= null){
			Date activityDate = getActivityDate(activityTransaction, inParams);
			
			EdiContainer ediContainer = activityTransaction.getEdiContainer();
			LOGGER.debug("Inside RTMCodeco60PostInterceptor :: Activity Date :: "+activityDate+" Current Container :: "+ediContainer.getContainerNbr());
			
			Equipment equipment = Equipment.findEquipment(ediContainer.getContainerNbr());
			if(equipment != null){
				UnitFinder unitFinder = (UnitFinder)Roastery.getBean("unitFinder");
				
				Unit unit = findLatestDepartedUnit(ContextHelper.getThreadComplex(), equipment);
				if(unit !=null){
					UnitFacilityVisit ufv = unit.getUfvForFacilityNewest(currentFacility);
					LOGGER.debug("Current UFV :: "+ufv+" for facility :: "+currentFacility);
					if(ufv != null){
						ufv.setUfvFlexDate02(activityDate);
						HibernateApi.getInstance().saveOrUpdate(ufv);
						HibernateApi.getInstance().flush();
					}else{
						createBizFaiureMessage(null, inParams, "Container :: "+ediContainer.getContainerNbr() + " is not Departed yet");
					}
				}else{
					createBizFaiureMessage(RoadPropertyKeys.GATE__UNIT_NOT_FOUND, inParams, ediContainer.getContainerNbr());
				}
			}else{
				createBizFaiureMessage(ArgoPropertyKeys.EQID_UNKNOWN, inParams, ediContainer.getContainerNbr());
			}
		}
		inParams.put("SKIP_POSTER", Boolean.TRUE);
		LOGGER.debug("Inside RTMCodeco60PostInterceptor :: END");
	}
	
	private Unit findLatestDepartedUnit(Complex inComplex, Equipment inPrimaryEq){
		DomainQuery dq = QueryUtils.createDomainQuery("Unit").addDqPredicate(PredicateFactory.eq(UnitField.UNIT_COMPLEX, inComplex.getCpxGkey()))
			.addDqPredicate(PredicateFactory.eq(UnitField.UNIT_VISIT_STATE, UnitVisitStateEnum.DEPARTED))
			.addDqPredicate(PredicateFactory.eq(UnitField.UNIT_PRIMARY_EQ, inPrimaryEq.getEqGkey()))
			.addDqPredicate(PredicateFactory.isNotNull(UnitField.UNIT_EQUIPMENT))
			.addDqPredicate(PredicateFactory.isNotNull(MetafieldIdFactory.valueOf("customFlexFields.unitCustomDFFITTMTS")))
			.addDqOrdering(Ordering.desc(UnitField.UNIT_CREATE_TIME));
		List<Unit> currentDepartedList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
		if(currentDepartedList != null && currentDepartedList.size() > 0){
			return currentDepartedList.get(0);
		}
		return null
	}
	
	private createBizFaiureMessage(PropertyKey inPropertyKey, Map inParams, String errorMessage){
		BizFailure bv;
		if(inPropertyKey){
			bv = BizFailure.create(inPropertyKey, null);
		}else if(errorMessage){
			bv = BizFailure.createProgrammingFailure(errorMessage);
		}
		ContextHelper.getThreadMessageCollector().appendMessage(bv);
		inParams.put("SKIP_POSTER", Boolean.TRUE);
	}
	
	private Date getActivityDate(ActivityTransaction inEdiActivity, Map inParams){
		Date activityDate = null;
		if (inEdiActivity.getActivityDate() != null) {
		  Date workDate = ArgoEdiUtils.convertLocalToUtcDate(inEdiActivity.getActivityDate(), ContextHelper.getThreadUserTimezone());
		  Date workTime = ArgoEdiUtils.convertLocalToUtcDate(inEdiActivity.getActivityTime(), ContextHelper.getThreadUserTimezone());
		  if (workDate != null) {
			activityDate = ArgoEdiUtils.mergeDateAndTime(workDate, workTime).getTime();
		  }
		} else {
			BizFailure bv = BizFailure.create(ArgoPropertyKeys.ACTIVITY_DATE_REQUIRED, null);
			ContextHelper.getThreadMessageCollector().appendMessage(bv);
			inParams.put("SKIP_POSTER", Boolean.TRUE);
		}
		return activityDate;
	}
}
