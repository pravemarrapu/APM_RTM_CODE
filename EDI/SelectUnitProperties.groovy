package com.weserve.APM.EDI

import com.navis.argo.business.atoms.FreightKindEnum;
import com.navis.argo.business.atoms.UnitCategoryEnum;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.inventory.business.api.UnitField;
import com.navis.inventory.business.atoms.UfvTransitStateEnum;
import com.navis.inventory.business.atoms.UnitVisitStateEnum;
import com.navis.inventory.business.units.Unit;

class SelectUnitProperties {
	
	public String execute(){
		StringBuilder result = new StringBuilder();
		DomainQuery dq = QueryUtils.createDomainQuery("Unit").addDqPredicate(PredicateFactory.isNotNull(UnitField.UNIT_DEPARTURE_ORDER))
		.addDqPredicate(PredicateFactory.eq(UnitField.UNIT_VISIT_STATE, UnitVisitStateEnum.ACTIVE))
		.addDqPredicate(PredicateFactory.eq(UnitField.UNIT_EQTYPE_ID, "4510"))
		.addDqPredicate(PredicateFactory.eq(UnitField.UNIT_CATEGORY, UnitCategoryEnum.EXPORT))
		.addDqPredicate(PredicateFactory.eq(UnitField.UNIT_FREIGHT_KIND, FreightKindEnum.FCL))
		.addDqPredicate(PredicateFactory.eq(UnitField.UNIT_CURRENT_UFV_TRANSIT_STATE, UfvTransitStateEnum.S20_INBOUND));
		
		List<Unit>unitList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
		for(Unit unit : unitList){
			result.append(unit.getUnitId()).append(unit.getField(UnitField.UNIT_DEPARTURE_ORDER)).append(unit.getUnitLineOperator());
			result.append("\n")
		}
		
		return result.toString();
		
		
		
	}

}
