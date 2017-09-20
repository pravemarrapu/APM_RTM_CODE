import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.DataSourceEnum
import com.navis.argo.business.atoms.PropertyGroupEnum
import com.navis.argo.business.model.PropertySource
import com.navis.external.services.AbstractGeneralNoticeCodeExtension
import com.navis.framework.metafields.MetafieldId
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.units.Unit
import com.navis.services.business.event.GroovyEvent

/*
 Author      : Pradeep Arya
 Date Written: 23-Mar-17
 Requirements: WF#805275- The unitInterceptor didn't update the dataSource for COREOR
               Hence implemented this groovy to update correct datasource to unitFlexString09

 Deployment Steps:
 a) Administration -> System -> Code Extensions
 b) Click on + (Add) button.
 c) Fill out all the fields as
	. Facility
	. APMT/RTM/RTM
	. GENERAL_NOTICE_CODE_EXTENSION
	. RTMUpdateUnitDataSource
 d) Paste the Groovy Code and click on Save.

 Trigger:
  a) Select code extension RTMUpdateUnitDataSource in General Notice UNIT_CREATE

 */

class RTMUpdateUnitDataSource extends AbstractGeneralNoticeCodeExtension {

    public void execute(GroovyEvent inGroovyEvent) {
        log("RTMUpdateUnitDataSource execute() start..." + new Date());
        Unit unit = inGroovyEvent.getEntity();
        PropertySource propertySource = PropertySource.findPropertySource("Unit", unit.getUnitGkey(), PropertyGroupEnum.ROUTING);
        DataSourceEnum dataSource = propertySource != null ? propertySource.getPrpsrcDataSource() : ContextHelper.getThreadDataSource();
        // DataSourceEnum dataSource = ContextHelper.getThreadDataSource();
        log("Datasource:" + dataSource.getKey());
        unit.setFieldValue(CREATED_BY_PROCESS, dataSource.getKey());
        log("RTMUpdateUnitDataSource execute() end..." + new Date());
    }

    private static MetafieldId CREATED_BY_PROCESS = UnitField.UNIT_FLEX_STRING09;
}


