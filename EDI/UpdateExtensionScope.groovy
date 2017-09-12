package com.weserve.APM.EDI

import com.navis.argo.business.atoms.ExamEnum;
import com.navis.argo.business.atoms.InbondEnum;
import com.navis.extension.business.Extension;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.inventory.InvField;
import com.navis.inventory.business.units.Unit;
class UpdateExtensionScope {
	
	
	public String execute(){
		DomainQuery dq = QueryUtils.createDomainQuery("Extension")
		.addDqPredicate(PredicateFactory.in(MetafieldIdFactory.valueOf("extName"), "RTMITTUtils"))
		List<Extension> currentUnitList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
		for(Extension ext : currentUnitList){
			ext.setExtScopeLevel(2)
			ext.setExtScopeGkey("1");
			HibernateApi.getInstance().saveOrUpdate(ext)
		}
		HibernateApi.getInstance().flush()
	}

}
