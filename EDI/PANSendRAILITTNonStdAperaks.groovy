package com.weserve.APM.EDI

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.navis.apex.business.model.GroovyInjectionBase


class PANSendRAILITTNonStdAperaks extends GroovyInjectionBase{
	
	private Logger LOGGER = Logger.getLogger(this.class);
	public void execute(Map parameters) {
		
		LOGGER.setLevel(Level.DEBUG);
		LOGGER.debug("Inside the execution for the PANSendRAILITTNonStdAperaks :: ");
		getGroovyClassInstance("PANSendNonStdAperakHelper").execute("RAIL_ITT");
		LOGGER.debug("Inside the execution for the PANSendRAILITTNonStdAperaks :: END");

   }

}
