package com.weserve.APM.EDI

import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.road.RoadApptsField;
import com.navis.road.business.appointment.model.GateAppointment;

class UpdateAppointmentStatus {
	
	public String execute(){
		DomainQuery dq = QueryUtils.createDomainQuery("GateAppointment")
		.addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_NBR, "300003711"));
		//.addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_CTR_ID, "PRAD7100042"));
		
		List<GateAppointment> gateAppointment = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
		for(GateAppointment gAppt : gateAppointment){
			/*if(gAppt.getApptGkey() == 1713){
				HibernateApi.getInstance().delete(gAppt)
				HibernateApi.getInstance().flush()
			}*/
			//gAppt.setFieldValue(MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFsendMsg"), "YES");
			gAppt.setFieldValue(MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFpreanEqoNbr"), "010103");
			gAppt.setGapptOrderNbr(null);
			gAppt.setGapptOrder(null);
			HibernateApi.getInstance().saveOrUpdate(gAppt)
			HibernateApi.getInstance().flush()
		}
	}
}
