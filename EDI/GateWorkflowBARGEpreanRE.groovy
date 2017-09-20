/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 * Modified By: Praveen Babu
 * Date 20/09/2017 APMT #25 - Additional logging.
 */








package com.navis.external.mv2_rwg.preannouncements.validation.bizflow.gateWorkflows;


import com.navis.argo.ContextHelper
import com.navis.external.framework.util.ExtensionUtils
import com.navis.external.road.AbstractGateWorkflowInterceptor
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.road.portal.configuration.CachedGateTask

public class GateWorkflowBARGEpreanRE extends AbstractGateWorkflowInterceptor {

 public void preWorkflow(TransactionAndVisitHolder inWfCtx, List<CachedGateTask> inTasks) {

    _gateWorkflowHelper.preWorkflow(inWfCtx,inTasks);
  }

  public void postWorkflow(TransactionAndVisitHolder inWfCtx) {

    _gateWorkflowHelper.postWorkflow(inWfCtx);
  }

  private def _gateWorkflowHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateWorkflowHelper");
}