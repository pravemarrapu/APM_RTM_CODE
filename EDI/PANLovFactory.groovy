package com.navis.apex.groovy.mv2_rwg.preannouncements.ui;


import java.io.Serializable;

import com.navis.argo.ArgoField;
import com.navis.argo.ContextHelper;
import com.navis.argo.business.atoms.CarrierVisitPhaseEnum;


import com.navis.external.framework.ui.lov.AbstractExtensionLovFactory;
import com.navis.external.framework.ui.lov.ELovKey;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.portal.Ordering;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.presentation.lovs.Lov;
import com.navis.framework.presentation.lovs.Style;
import com.navis.framework.presentation.lovs.list.DomainQueryLov;
import com.navis.vessel.VesselEntity;
import com.navis.vessel.VesselField;
import com.navis.vessel.business.atoms.VesselClassificationEnum
import com.navis.vessel.api.VesselVisitField
import com.navis.framework.metafields.MetafieldId
import com.navis.rail.RailEntity
import com.navis.rail.business.api.RailCompoundField
import com.navis.rail.business.atoms.TrainDirectionEnum
import com.navis.rail.RailField
import com.navis.argo.business.model.Facility
import com.navis.framework.ulc.server.extension.ULCHelp
import com.navis.framework.presentation.context.PresentationContextUtils
import com.navis.road.presentation.preadvise.TruckVisitAppointmentLovKey
import com.navis.road.business.atoms.AppointmentStateEnum
import com.navis.road.RoadApptsEntity
import com.navis.road.RoadApptsField
import com.navis.framework.presentation.lovs.list.ObsoleteFilteredDomainQueryLov
import com.navis.road.business.model.Gate;
/*
* Author: Sophia Robertson
* Date: 03/06/13
*
* Date 21/03/14 CSDV-1710 added INBOUND to the list of allowed phases in outbound train visit lov
* Date 27/06/14 CSDV-2156 added tvaLOV twhich is based on Tva Ref Nbr
*/

public class PANLovFactory extends AbstractExtensionLovFactory {

    public Lov getLov(ELovKey inKey) {
        if (inKey.represents("bargeLov")) {
            CarrierVisitPhaseEnum[] disallowedPhases = [CarrierVisitPhaseEnum.CANCELED,
                                                        CarrierVisitPhaseEnum.CLOSED,
                                                        CarrierVisitPhaseEnum.ARCHIVED];

            final MetafieldId cvGkey = MetafieldIdFactory.getCompoundMetafieldId(ArgoField.CVD_CV, ArgoField.CV_GKEY);
            final MetafieldId visitId = MetafieldIdFactory.getCompoundMetafieldId(ArgoField.CVD_CV, ArgoField.CV_ID);
            final MetafieldId vslName = MetafieldIdFactory.getCompoundMetafieldId(VesselField.VVD_VESSEL, VesselField.VES_NAME);

            DomainQuery dq = QueryUtils.createDomainQuery(VesselEntity.VESSEL_VISIT_DETAILS)
                    .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("vvdClassification"), VesselClassificationEnum.BARGE))
                    .addDqPredicate(PredicateFactory.not(PredicateFactory.in(VesselVisitField.VVD_VISIT_PHASE, disallowedPhases)))
                    .addDqPredicate(PredicateFactory.ne(visitId, "GEN_BARGE"))
                    .addDqField(visitId)
                    .addDqField(vslName)
                    .addDqField(VesselVisitField.VVD_IB_VYG_NBR)
                    .addDqField(cvGkey)
                    .addDqOrdering(Ordering.asc(VesselVisitField.VVD_ID));

            DomainQueryLov dqLov = new DomainQueryLov(dq, Style.LABEL1_PAREN_LABEL2);
            dqLov.setKeyFieldId(cvGkey);
            return dqLov;
        }

        else if (inKey.represents("inboundTrainVisitLov")) {

            CarrierVisitPhaseEnum[] allowedPhases = [CarrierVisitPhaseEnum.CREATED, CarrierVisitPhaseEnum.INBOUND,CarrierVisitPhaseEnum.WORKING, CarrierVisitPhaseEnum.ARRIVED];
            TrainDirectionEnum[]  allowedDirections = [TrainDirectionEnum.INBOUND, TrainDirectionEnum.THROUGH];

            MetafieldId fcy = MetafieldIdFactory.getCompoundMetafieldId(ArgoField.CVD_CV, ArgoField.CV_FACILITY);
            MetafieldId visitId = MetafieldIdFactory.getCompoundMetafieldId(ArgoField.CVD_CV, ArgoField.CV_ID);
            MetafieldId cvGkey = MetafieldIdFactory.getCompoundMetafieldId(ArgoField.CVD_CV, ArgoField.CV_GKEY);

            DomainQuery dq = QueryUtils.createDomainQuery(RailEntity.TRAIN_VISIT_DETAILS)
                    .addDqPredicate(PredicateFactory.eq(fcy, ContextHelper.getFacilityKey(PresentationContextUtils.getRequestContext())))
                    .addDqPredicate(PredicateFactory.in(RailField.RVDTLSD_DIRECTION, allowedDirections))
                    .addDqPredicate(PredicateFactory.in(RailCompoundField.CVD_CV_PHASE, allowedPhases))
                    .addDqField(visitId)
                    .addDqField(cvGkey)
                    .addDqOrdering(Ordering.asc(visitId));

            DomainQueryLov dqLov = new DomainQueryLov(dq, Style.LABEL1_PAREN_LABEL2);
            dqLov.setKeyFieldId(cvGkey);
            return dqLov;
        }
        else if (inKey.represents("outboundTrainVisitLov")) {

            CarrierVisitPhaseEnum[] allowedPhases = [CarrierVisitPhaseEnum.CREATED,CarrierVisitPhaseEnum.INBOUND, CarrierVisitPhaseEnum.WORKING, CarrierVisitPhaseEnum.ARRIVED];
            TrainDirectionEnum[]  allowedDirections = [TrainDirectionEnum.OUTBOUND, TrainDirectionEnum.THROUGH];

            MetafieldId fcy = MetafieldIdFactory.getCompoundMetafieldId(ArgoField.CVD_CV, ArgoField.CV_FACILITY);
            MetafieldId visitId = MetafieldIdFactory.getCompoundMetafieldId(ArgoField.CVD_CV, ArgoField.CV_ID);
            MetafieldId cvGkey = MetafieldIdFactory.getCompoundMetafieldId(ArgoField.CVD_CV, ArgoField.CV_GKEY);

            DomainQuery dq = QueryUtils.createDomainQuery(RailEntity.TRAIN_VISIT_DETAILS)
                    .addDqPredicate(PredicateFactory.eq(fcy, ContextHelper.getFacilityKey(PresentationContextUtils.getRequestContext())))
                    .addDqPredicate(PredicateFactory.in(RailField.RVDTLSD_DIRECTION, allowedDirections))
                    .addDqPredicate(PredicateFactory.in(RailCompoundField.CVD_CV_PHASE, allowedPhases))
                    .addDqField(visitId)
                    .addDqField(cvGkey)
                    .addDqOrdering(Ordering.asc(visitId));

            DomainQueryLov dqLov = new DomainQueryLov(dq, Style.LABEL1_PAREN_LABEL2);
            dqLov.setKeyFieldId(cvGkey);
            return dqLov;
        }
        else if (inKey.represents("tvaLov")) {

            //Long gateGkey = new Long(38);

            Long gateGkey =   (Long)PresentationContextUtils.getRequestContext().getAttribute("CUSTOM_GATE_GKEY");
            AppointmentStateEnum[] includeStates = [AppointmentStateEnum.CREATED, AppointmentStateEnum.LATE];

            DomainQuery dq = QueryUtils.createDomainQuery(RoadApptsEntity.TRUCK_VISIT_APPOINTMENT)
                    .addDqField(RoadApptsField.TVAPPT_REFERENCE_NBR)
                    .addDqPredicate(PredicateFactory.in(RoadApptsField.TVAPPT_STATE, includeStates))
                    .addDqPredicate(PredicateFactory.eq(RoadApptsField.TVAPPT_GATE, gateGkey))
                    .addDqOrdering(Ordering.asc(RoadApptsField.TVAPPT_REFERENCE_NBR));

            DomainQueryLov dqLov = new DomainQueryLov(dq, Style.LABEL1_PAREN_LABEL2);

            return dqLov;
        }

        return null;
    }
}