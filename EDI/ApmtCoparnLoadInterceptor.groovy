package com.weserve.APM.EDI

import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.navis.external.edi.entity.AbstractEdiLoadInterceptor

/**
 *
 * Author: <a href="mailto:bgopal@weservetech.com">Gopalakrishnan B</a>, 18/Aug/2017
 * Sequence GID segment according to EQD segment's sequence
 *
 */

public class ApmtCoparnLoadInterceptor extends AbstractEdiLoadInterceptor {

	private Logger LOGGER = Logger.getLogger(ApmtCoparnLoadInterceptor.class);

	public String beforeEdiLoad(String inFileAsString, Serializable inEdiBatchGkey, String inDelimiter) {
		return sequenceGIDsegment(inFileAsString);
	}


	private String sequenceGIDsegment(String inFileAsString) {
		LOGGER.setLevel(Level.DEBUG);
		String fileString = inFileAsString;
		try {
			String EQD_SEGMENT = "EQD+CN+";
			String RFF_SQ_SEGMENT = "RFF+SQ:";
			String GID_SEGMENT = "GID+";
			String GID_SEGMENT_WITH_ESCAPE = "GID\\+";
			String APORSTAPHE_DELIMITER = "\'";
			String[] lines = fileString.split(APORSTAPHE_DELIMITER);
			List<String> eqdSequenceList = new ArrayList<String>();
			List<String> sectionList = new ArrayList<String>();
			int beginIndex, endIndex, index;

			for(String line : lines) {
				System.out.print(line);
				if(line.indexOf(RFF_SQ_SEGMENT) >= 0) {
					beginIndex = line.indexOf(RFF_SQ_SEGMENT, 0) + RFF_SQ_SEGMENT.length();
					endIndex = ( !(line.substring(beginIndex) != null && line.substring(beginIndex).isEmpty()) )? line.substring(beginIndex).length() + beginIndex : beginIndex;
					String EQD_SEQ = line.substring(beginIndex, endIndex);
					if(!EQD_SEQ.isEmpty()) {
						eqdSequenceList.add(EQD_SEQ);
					}
				}
			}
			index  = fileString.indexOf(GID_SEGMENT);
			if(index > 0) {
				sectionList.add(fileString.substring(0, index));

				beginIndex = fileString.indexOf(GID_SEGMENT);
				endIndex = fileString.indexOf(EQD_SEGMENT);
				if(beginIndex > 0) {
					sectionList.add(fileString.substring(beginIndex, endIndex));
				}

				beginIndex = fileString.indexOf(EQD_SEGMENT);
				if(beginIndex > 0) {
					sectionList.add(fileString.substring(beginIndex));
				}
			}

			Map<String, String> gidMap = new HashMap<String, String>();
			if(!sectionList.get(1).isEmpty()) {
				String GID_SECTION = sectionList.get(1);
				if(!GID_SECTION.isEmpty()) {
					String[] gidArray  = GID_SECTION.split(GID_SEGMENT_WITH_ESCAPE);
					List<String> gidList = new ArrayList<String>();

					int i=0;
					for(String arr : gidArray) {
						if((i++) == 0) {
							continue;
						}
						index = arr.indexOf(APORSTAPHE_DELIMITER);
						if(index > 0) {
							String sequence = arr.substring(0, index);
							if(!sequence.isEmpty()) {
								LOGGER.debug("sequence: "+sequence);
								gidMap.put(sequence, (GID_SEGMENT + arr));
							}
						}
					}
				}
			}

			StringBuilder returnFileAsString = new StringBuilder();
			for(int i=0; i < sectionList.size(); i++) {
				if( i == 1 ) {
					for(String eqdSequence : eqdSequenceList) {
						returnFileAsString.append( gidMap.get(eqdSequence) );
					}
				} else {
					returnFileAsString.append(sectionList.get(i));
				}
			}

			LOGGER.debug("\n\nreturn :\n"+returnFileAsString);
			return returnFileAsString.toString();

		} catch (Exception e) {
			return fileString;
		}

	}
}