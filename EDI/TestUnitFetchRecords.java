package com.weserve.APM.EDI;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.navis.argo.ArgoRefField;
import com.navis.argo.business.reference.Equipment;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.inventory.business.api.UnitField;
import com.navis.inventory.business.units.EqBaseOrder;
import com.navis.inventory.business.units.EqBaseOrderItem;
import com.navis.inventory.business.units.Unit;
import com.navis.inventory.business.units.UnitEquipment;
import com.navis.orders.business.eqorders.EquipmentOrder;
import com.navis.orders.business.eqorders.EquipmentOrderItem;

public class TestUnitFetchRecords {
	
	public String execute(){
		
		DomainQuery dq = QueryUtils.createDomainQuery("Unit").addDqPredicate(PredicateFactory.isNotNull(UnitField.UNIT_DEPARTURE_ORDER))
				.addDqPredicate(PredicateFactory.eq(UnitField.UNIT_EQTYPE_ID, "4510"));
		EquipmentOrder eqOrder = null;
		EquipmentOrderItem eqOrderItem = null;
		List<Unit> unitList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
		StringBuilder unitListString = new StringBuilder();
		for (Unit unit : unitList) {
			unitListString.append(unit.getUnitId()).append(unit.getUeEquipment().getEqEquipType().getEqtypId()).append("-");
			if(unit.getUnitDepartureOrderItem() != null){
				EqBaseOrder eqBaseOrder = unit.getUnitDepartureOrderItem().getEqboiOrder();
				eqOrder = (EquipmentOrder) HibernateApi.getInstance().downcast(eqBaseOrder, EquipmentOrder.class);
				Set<EqBaseOrderItem> equipmentOrderItem = eqOrder.getEqboOrderItems();
				for(EqBaseOrderItem eqBaseOrderItem : equipmentOrderItem){
					eqOrderItem = (EquipmentOrderItem) HibernateApi.getInstance().downcast(eqBaseOrderItem, EquipmentOrderItem.class);
					if(eqOrderItem != null){
						return String.valueOf(eqOrderItem.getEqoiGrossWeight());
					}
				}
				break;
			}
		}
		
		//List<Equipment> unitList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
		
		
		return "Success"+eqOrder;
	}

}
