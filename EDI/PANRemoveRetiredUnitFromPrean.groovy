/*
 * Copyright (c) 2017 Navis LLC. All Rights Reserved.
 *
 * $Id: PANRemoveRetiredUnitFromPrean.groovy 267230 2017-02-02 09:30:41Z neefpa $
 */



package com.navis.apex.groovy.preannouncements

import com.navis.apex.business.model.GroovyInjectionBase
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.inventory.business.units.Unit
import com.navis.road.RoadApptsEntity
import com.navis.road.RoadApptsField
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.atoms.AppointmentStateEnum
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum
import com.navis.services.business.event.Event
import com.navis.services.business.event.GroovyEvent

/* Author: Sophia Robertson
 * CSDV-4326 (CRQ-1196)
* Creation Date: 27/01/2017
*/

public class PANRemoveRetiredUnitFromPrean extends GroovyInjectionBase {

    public void execute(GroovyEvent inGroovyEvent) {
        Event evnt = inGroovyEvent.getEvent();
        Serializable unitPk = evnt.getEventAppliedToGkey();
        Unit unit = (Unit)HibernateApi.getInstance().load(Unit.class, unitPk);
        List<GateAppointment> unitApptList = findOpenGateAppointmentsByUnit(unit);

        for(GateAppointment appt : unitApptList){

            appt.setGapptUnit(null);

        }

    }

    public List findOpenGateAppointmentsByUnit(Unit inUnit) {

        DomainQuery dq = QueryUtils.createDomainQuery(RoadApptsEntity.GATE_APPOINTMENT)
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_UNIT, inUnit.getUnitGkey()))
                .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_STATE, AppointmentStateEnum.CREATED))
                .addDqPredicate(PredicateFactory.in(RoadApptsField.GAPPT_TRAN_TYPE, [TruckerFriendlyTranSubTypeEnum.PUI,TruckerFriendlyTranSubTypeEnum.PUM]));


        List<GateAppointment> gapptList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
        return gapptList;
    }

}
