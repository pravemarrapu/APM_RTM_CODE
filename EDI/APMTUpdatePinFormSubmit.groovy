
import com.navis.external.framework.util.EFieldChanges
import com.navis.framework.metafields.entity.EntityId
import com.navis.framework.util.message.MessageLevel
import com.navis.framework.util.message.MessageCollector
import com.navis.external.framework.ui.AbstractFormSubmissionCommand

/*
Author      : 	Pradeep Arya
Date Written:
Requirements:

Deployment Steps:
a) Administration -> System -> Code Extensions
b) Click on + (Add) button.
c) Fill out all the fields as
	. Facility
	. APMT/PTP/PTP
	. FORM_SUBMIT_INTERCEPTION
	. APMTUpdatePinFormSubmit
d) Paste the Groovy Code and click on Save.

Trigger:
 Form ORD056
*
*/

class APMTUpdatePinFormSubmit extends AbstractFormSubmissionCommand {

    @Override
    public void doAfterSubmit(String inVariformId, EntityId inEntityId, List<Serializable> inGkeys, EFieldChanges inOutFieldChanges,
                              EFieldChanges inNonDbFieldChanges, Map<String, Object> inParams) {
        log("APMTUpdatePinFormSubmit onCreate start...");
        //registerError("$inOutFieldChanges + $inParams")
        Map parmMap = new HashMap();
        Map results = new HashMap();

        parmMap.put("GKEYS", inGkeys);
        parmMap.put("FIELD_CHANGES", inOutFieldChanges);

        MessageCollector messageCollector = executeInTransaction("APMTPINUpdateCallback", parmMap, results);
        if(results.get("ERROR"))
            registerError(results.get("ERROR"));
        // Check results
        if (messageCollector.hasError())
            this.extensionHelper.showMessageDialog(MessageLevel.WARNING, 'ACTION FAILED', 'Your request failed due to: ', messageCollector);

    }

}