/*
 * Copyright (c) 2012 Navis LLC. All Rights Reserved.
 *
 * Date 25/06/14 CSDV-2152 use GateAppointment dynamic flex fields instead of gapptUfv and gapptUnit flex fields for prean specific values
 */
package com.navis.apex.groovy.mv2_rwg.preannouncements.ui;


import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.query.DataQuery;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.presentation.command.VariformUiCommand;
import com.navis.framework.presentation.util.PresentationConstants;
import com.navis.framework.presentation.view.DefaultSharedUiTableManager
import com.navis.external.framework.beans.EBean
import com.navis.framework.query.common.api.QueryCriteria
import com.navis.framework.util.BizViolation
import com.navis.framework.util.ValueHolder
import com.navis.road.RoadApptsField
import com.navis.road.RoadApptsEntity
import com.navis.inventory.business.api.UnitField
import com.navis.framework.portal.query.OuterQueryMetafieldId
import org.jetbrains.annotations.Nullable

public class CustomBeanPreanErrorsViewUiTableManager extends DefaultSharedUiTableManager implements EBean{

    private VariformUiCommand getBaseParentCommand() {
        Object parent = getAttribute(PresentationConstants.PARENT);
        return parent != null && parent instanceof VariformUiCommand ? (VariformUiCommand) parent : null;
    }

    /**
     * @return base entity gkey from which this query is triggered (like: Unit, GateAppointment, RailCarVisit.. etc)
     */
    public Serializable getBaseEntityGkey() {

        List<Serializable> gkeys = (List<Serializable>) getBaseParentCommand().getAttribute(PresentationConstants.SOURCE);
        Serializable entityGkey = gkeys.isEmpty() ? null : gkeys.get(0);
        return entityGkey;
    }

    @Override
    List<? extends ValueHolder> createCustomModel(@Nullable Map inArgs) throws BizViolation {
        return super.createCustomModel(inArgs)
    }

    public DataQuery createQuery() {
        DomainQuery dq = (DomainQuery) super.createQuery();
        Serializable preanKey =  getBaseEntityGkey();

        if (preanKey != null) {
            DomainQuery subQuery = QueryUtils.createDomainQuery(RoadApptsEntity.GATE_APPOINTMENT)
                    .addDqPredicate(PredicateFactory.eq(RoadApptsField.GAPPT_GKEY, preanKey))
                    .addDqPredicate(PredicateFactory.eqProperty(PREAN_VALIDATION_RUN_ID, OuterQueryMetafieldId.valueOf(VALIDATION_RUN_ID)))
                    .addDqPredicate(PredicateFactory.eq(PREAN_STATUS, "NOK"));



            dq = QueryUtils.createDomainQuery(PREAN_VALIDATION_ERROR)
                    .addDqPredicate(PredicateFactory.eq(PREAN_KEY, preanKey))
                    .addDqPredicate(PredicateFactory.subQueryExists(subQuery));

            QueryCriteria


        }
        return dq;
    }

    String getDetailedDiagnostics() {
        return "CustomBeanPreanErrorsViewUiTableManager";
    }

    static MetafieldId PREAN_KEY = MetafieldIdFactory.valueOf("customEntityFields.custompaverrPreanKey");
    static String PREAN_VALIDATION_ERROR = "com.navis.external.custom.CustomPreanValidationError";
    static MetafieldId VALIDATION_RUN_ID = MetafieldIdFactory.valueOf("customEntityFields.custompaverrValidationRunId");

    public static MetafieldId PREAN_STATUS = MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFpreanStatus");
    static MetafieldId PREAN_VALIDATION_RUN_ID =  MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFvalidationRunId");

}
