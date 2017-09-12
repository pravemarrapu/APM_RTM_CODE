package com.weserve.APM.EDI

import com.navis.external.road.AbstractGateApiHandlerInterceptor;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.portal.Ordering;
import com.navis.framework.portal.QueryUtils;
import com.navis.framework.portal.UserContext;
import com.navis.framework.portal.query.DomainQuery;
import com.navis.framework.portal.query.PredicateFactory;
import com.navis.framework.query.common.api.QueryResult;
import com.navis.framework.util.BizViolation;
import com.navis.framework.util.ValueHolder;
import com.navis.framework.util.message.MessageCollector;
import com.navis.road.RoadApptsField;
import com.navis.road.business.appointment.model.GateAppointment;
import org.jdom.Element;

/**
 * This is custom gate code that updates error messages for create / update web service request for pre announcments (gate appointments).
 *
 * This Groovy is wired in using the Code Extension view and two code extensions are created; one for when an update gate appointment web service
 * comes in, and one for when a create gate appointment web service comes in.
 *
 * The Groovy code extension type must be GATE_API_HANDLER_INTERCEPTOR.
 *
 * To handle the update web service, the Groovy code extension name must be UpdateAppointmentHandlerInterceptor in the code extension.
 *
 * To handle the create web service, the Groovy code extension name must be CreateAppointmentHandlerInterceptor in the code extension.
 *
 * Author: <a href="mailto:amos.parlante@navis.com">Amos Parlante</a>
 * Date: 6/17/13 : 4:13 PM
 * JIRA: CSDV-995
 * SFDC: NA
 * Called from: Gate Web Service API
 */

public class PANUpdateErrorsGateAppointmentHandler extends AbstractGateApiHandlerInterceptor {

  public void execute(UserContext inUserContext,
                               MessageCollector inMessageCollector,
                               Element inCreateAppointment,
                               Element inOutResponse,
                               List<Element> inOutAdditionalResponses,
                               Long inWslogGkey ) throws BizViolation {

     super.execute( inUserContext,inMessageCollector, inCreateAppointment,inOutResponse,inOutAdditionalResponses, inWslogGkey);
     if (inOutResponse == null) {
       // Nothing to do if there is no response.
       return;
     }
     // Get the just created gate appointment.
     Element appointmentNumberElement = inOutResponse.getChild(APPOINTMENT_NUMBER_ELEMENT_NAME);
     if (appointmentNumberElement == null) {
       // Nothing to do if the response doesn't have an appointment number (in this case no appointment was created).
       return;
     }
     String appointmentNumberString = appointmentNumberElement.getValue();
     if (appointmentNumberString == null) {
       // Nothing to do if there is no appointment number (we should never get here so log this case).
       log("When creating an appointment using the update-appointment web service API there was an <appointment-nbr> in the reseponse that does " +
               "not have an appointment number as a value. Unable to check for pre-announcement errors generated when updating this appointment");
       return;
     }
     long appointmentNumber = appointmentNumberString.toLong();
     GateAppointment gateAppointment = GateAppointment.findGateAppointment(appointmentNumber);
     if (gateAppointment == null) {
       // Nothing to do if there is no gate appointment in the system (we should never get here so log this case).
       log("When creating an appointment using the update-appointment web service API, an appointment number was returned that does not exist in " +
               "N4. Unable to check for pre-announcement errors generated when updating this appointment");
       return;
     }
     addAperakErrorsToResponse(gateAppointment, inOutResponse);
   }

  private static void addAperakErrorsToResponse(GateAppointment inPrean, Element inOutResponse) {
    String preanStatus = (String)inPrean.getField(PREAN_STATUS);
    if ("NOK".equals(preanStatus)) {

      List<ValueHolder> errs = getErrs(inPrean.getApptGkey(),Long.parseLong(inPrean.getFieldString(PREAN_VALIDATION_RUN_ID)));

      if ((errs == null) || (errs.isEmpty())) {
        return;
      }
      Element validationErrors = new Element(VALIDATION_ERRORS_ELEMENT_NAME);
      for (ValueHolder err : errs) {
        Element error = new Element(ERROR_ELEMENT_NAME);
        String errorCode = (err.getFieldValue(ERR_APERAK_CODE) != null) ? err.getFieldValue(ERR_APERAK_CODE) : "";
        String errorMessage = (err.getFieldValue(ERR_APERAK_MSG) != null) ? err.getFieldValue(ERR_APERAK_MSG) : "";
        error.setAttribute(ERROR_CODE_ATTRIBUTE, errorCode);
        error.setAttribute(ERROR_MESSAGE_ATTRIBUTE, errorMessage);
        validationErrors.addContent(error);
      }
      inOutResponse.addContent(validationErrors);
    }
  }

  private static List<ValueHolder> getErrs(Long inPreanKey, Long inValidationRunId) {

    List<ValueHolder> errs = new ArrayList<>();

    DomainQuery dq = QueryUtils.createDomainQuery(PREAN_VALIDATION_ERROR)
           .addDqField(ERR_APERAK_CODE)
           .addDqField(ERR_APERAK_MSG)
           .addDqPredicate(PredicateFactory.eq(PREAN_KEY, inPreanKey))
           .addDqPredicate(PredicateFactory.eq(VALIDATION_RUN_ID, inValidationRunId))
           .addDqOrdering(Ordering.asc(ERR_APERAK_CODE));

    QueryResult values  = Roastery.getHibernateApi().findValuesByDomainQuery(dq);

    for (int i = 0; i < values.getCurrentResultCount(); i++) {
      errs.add(values.getValueHolder(i));
    }
    return errs;
  }

  public static MetafieldId PREAN_STATUS = RoadApptsField.GAPPT_UNIT_FLEX_STRING01;
  public static MetafieldId PREAN_VALIDATION_RUN_ID =  RoadApptsField.GAPPT_UNIT_FLEX_STRING09;

  public static MetafieldId PREAN_KEY = MetafieldIdFactory.valueOf("customEntityFields.custompaverrPreanKey");
  public static MetafieldId ERR_APERAK_CODE = MetafieldIdFactory.valueOf("customEntityFields.custompaverrAperakCode");
  public static MetafieldId ERR_APERAK_MSG = MetafieldIdFactory.valueOf("customEntityFields.custompaverrMsg");
  public static MetafieldId VALIDATION_RUN_ID = MetafieldIdFactory.valueOf("customEntityFields.custompaverrValidationRunId");

  public static String PREAN_VALIDATION_ERROR = "com.navis.external.custom.CustomPreanValidationError";

  public static String APPOINTMENT_NUMBER_ELEMENT_NAME = "appointment-nbr";
  public static String VALIDATION_ERRORS_ELEMENT_NAME = "prean-validation-errors";
  public static String ERROR_ELEMENT_NAME = "error";
  public static String ERROR_CODE_ATTRIBUTE = "code";
  public static String ERROR_MESSAGE_ATTRIBUTE = "message";
}
