import com.navis.extension.model.persistence.DynamicHibernatingEntity;
import com.navis.extension.model.persistence.IDynamicHibernatingEntity;
import com.navis.external.framework.AbstractExtensionCallback;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.Ordering;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.UserContext;
import com.navis.framework.portal.query.AggregateFunctionType;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.query.common.api.QueryResult;
import com.navis.framework.util.TransactionParms;
import com.navis.road.business.appointment.model.GateAppointment;
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.springframework.transaction.annotation.Transactional;

/**
 * Modified By: Praveen Babu
 Date 11/09/2017 APMT #25 - Update the right validation run id to Prean and flush the prean validation id.
 */
public class PANPreanErrorUtil extends AbstractExtensionCallback {

    public static void recordError(GateAppointment inPrean, String inAperakCode, String inMsg, Long inValidationRunId) {

        try {

            LOGGER.setLevel(Level.DEBUG)
            LOGGER.debug("Prean$inPrean\n inAperakCode:$inAperakCode\n inMsg:$inMsg \n inValidationRunId:$inValidationRunId");
            IDynamicHibernatingEntity preanValidationError = new DynamicHibernatingEntity(PREAN_VALIDATION_ERROR);
            preanValidationError.setFieldValue(PREAN_KEY, inPrean.getGapptGkey());
            preanValidationError.setFieldValue(PREAN, inPrean);
            preanValidationError.setFieldValue(ERR_APERAK_CODE, inAperakCode);
            preanValidationError.setFieldValue(ERR_APERAK_MSG, inMsg);
            preanValidationError.setFieldValue(VALIDATION_RUN_ID, inValidationRunId);
            preanValidationError.setFieldValue(CREATED, new Date());
            preanValidationError.setFieldValue(CREATOR, _userContext.getUserId());

            HibernateApi.getInstance().save(preanValidationError);
            HibernateApi.getInstance().flush();
            LOGGER.debug("Successfully persisted Prean Validation error for validation id :: "+inValidationRunId);
        }
        catch (Exception e) {
            String msg = e.getMessage();
            LOGGER.error("recordError Exception:$msg");
        }

    }


    public boolean diffErrorsRecorded(Long inPreanKey, Long inLastValidationRunId, String inCurrErrorAperakCodes) {


        boolean msgNeedsToBeSent = false;

        //String currErrorAperakCodes = getErrRefAperakCodesByPreanAndValidationRunId(inPreanKey, inLastValidationRunId);
        String prevErrorAperakCodes;


        if (!inCurrErrorAperakCodes.isEmpty()) {
            prevErrorAperakCodes = findPrevErrorRefList(inPreanKey, inLastValidationRunId);
            if (prevErrorAperakCodes.isEmpty() || !inCurrErrorAperakCodes.equals(prevErrorAperakCodes)) {

                msgNeedsToBeSent = true;
            }
        }


        return msgNeedsToBeSent;
    }


    public String findPrevErrorRefList(Long inPreanKey, Long inCurrValidationRunId) {

        String errList = "";

        DomainQuery dq = QueryUtils.createDomainQuery(PREAN_VALIDATION_ERROR)
                .addDqAggregateField(AggregateFunctionType.MAX, VALIDATION_RUN_ID)
                .addDqPredicate(PredicateFactory.eq(PREAN_KEY, inPreanKey))
                .addDqPredicate(PredicateFactory.lt(VALIDATION_RUN_ID, inCurrValidationRunId));


        QueryResult result = Roastery.getHibernateApi().findValuesByDomainQuery(dq);

        Long prevValidationRunId = result.getTotalResultCount() > 0 ? (Long) result.getValue(0, 0) : (long) 0;

        if (prevValidationRunId > 0) {

            errList = getErrRefAperakCodesByPreanAndValidationRunId(inPreanKey, prevValidationRunId);
        }

        return errList;
    }


    @Transactional(readOnly = true)
    public String getErrRefAperakCodesByPreanAndValidationRunId(Long inPreanKey, Long inValidationRunId) {

        String errRefAperakCodes = "";
        //log("inPreanKey:$inPreanKey + VALIDATION_RUN_ID:$VALIDATION_RUN_ID");

        DomainQuery dq = QueryUtils.createDomainQuery(PREAN_VALIDATION_ERROR)
                .addDqField(ERR_APERAK_CODE)
                .addDqPredicate(PredicateFactory.eq(PREAN_KEY, inPreanKey))
                .addDqPredicate(PredicateFactory.eq(VALIDATION_RUN_ID, inValidationRunId))
                .addDqOrdering(Ordering.asc(ERR_APERAK_CODE));

        QueryResult values = Roastery.getHibernateApi().findValuesByDomainQuery(dq);

        String currAperakCode = "";
        for (Iterator iterator = values.getIterator(); iterator.hasNext();) {
            Object[] v = (Object[]) iterator.next();
            String nextAperakCode = v[0];
            if (!nextAperakCode.equals(currAperakCode)) {
                errRefAperakCodes = errRefAperakCodes + nextAperakCode;
                currAperakCode = nextAperakCode;
            }
        }
        return errRefAperakCodes;
    }


    public static MetafieldId PREAN_KEY = MetafieldIdFactory.valueOf("customEntityFields.custompaverrPreanKey");
        public static MetafieldId PREAN = MetafieldIdFactory.valueOf("customEntityFields.custompaverrPrean");
    public static MetafieldId ERR_APERAK_CODE = MetafieldIdFactory.valueOf("customEntityFields.custompaverrAperakCode");
    public static MetafieldId ERR_APERAK_MSG = MetafieldIdFactory.valueOf("customEntityFields.custompaverrMsg");
    public
    static MetafieldId VALIDATION_RUN_ID = MetafieldIdFactory.valueOf("customEntityFields.custompaverrValidationRunId");
    public static MetafieldId CREATED = MetafieldIdFactory.valueOf("customEntityFields.custompaverrCreated");
    public static MetafieldId CREATOR = MetafieldIdFactory.valueOf("customEntityFields.custompaverrCreator");


    public static String PREAN_VALIDATION_ERROR = "com.navis.external.custom.CustomPreanValidationError";
    private static Logger LOGGER = Logger.getLogger(this.class)

    private static UserContext _userContext = TransactionParms.getBoundParms().getUserContext();
}