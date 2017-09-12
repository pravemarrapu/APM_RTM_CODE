
import com.navis.argo.ContextHelper
import com.navis.external.framework.util.ExtensionUtils
import com.navis.external.road.AbstractGateWorkflowInterceptor
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.road.portal.configuration.CachedGateTask

public class GateWorkflowRAILITTpreanDE extends AbstractGateWorkflowInterceptor {

  public void preWorkflow(TransactionAndVisitHolder inWfCtx, List<CachedGateTask> inTasks) {

	_gateWorkflowHelper.preWorkflow(inWfCtx,inTasks);
  }

  public void postWorkflow(TransactionAndVisitHolder inWfCtx) {

	_gateWorkflowHelper.postWorkflow(inWfCtx);
  }

  private static def _gateWorkflowHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateWorkflowHelper");
}