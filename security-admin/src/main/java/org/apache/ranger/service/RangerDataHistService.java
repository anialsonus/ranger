package org.apache.ranger.service;

import java.io.IOException;
import java.util.Date;

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXDataHist;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerDataHistService {

	@Autowired
	RESTErrorUtil restErrorUtil;
	
	@Autowired
	RangerDaoManager daoMgr;
	
	public static final String ACTION_CREATE = "Create";
	public static final String ACTION_UPDATE = "Update";
	public static final String ACTION_DELETE = "Delete";
	
	public void createObjectDataHistory(RangerBaseModelObject baseModelObj, String action) {
		if(baseModelObj == null || action == null) {
			throw restErrorUtil
					.createRESTException("Error while creating DataHistory. "
							+ "Object or Action can not be null.",
							MessageEnums.DATA_NOT_FOUND);
		}
		
		
		Integer classType = null;
		String objectName = null;
		String content = null;
		
		Long objectId = baseModelObj.getId();
		String objectGuid = baseModelObj.getGuid();
		Long version = baseModelObj.getVersion();
		Date currentDate = DateUtil.getUTCDate();
		
		XXDataHist xDataHist = new XXDataHist();;
		
		xDataHist.setObjectId(baseModelObj.getId());
		xDataHist.setObjectGuid(objectGuid);
		xDataHist.setCreateTime(currentDate);
		xDataHist.setAction(action);
		xDataHist.setVersion(baseModelObj.getVersion());
		xDataHist.setUpdateTime(currentDate);
		xDataHist.setFromTime(currentDate);

		if(baseModelObj instanceof RangerServiceDef) {
			RangerServiceDef serviceDef = (RangerServiceDef) baseModelObj;
			objectName = serviceDef.getName();
			classType = AppConstants.CLASS_TYPE_XA_SERVICE_DEF;
			content = writeObjectAsString(serviceDef);
		} else if(baseModelObj instanceof RangerService) {
			RangerService service = (RangerService) baseModelObj;
			objectName = service.getName();
			classType = AppConstants.CLASS_TYPE_XA_SERVICE;
			content = writeObjectAsString(service);
		} else if(baseModelObj instanceof RangerPolicy) {
			RangerPolicy policy = (RangerPolicy) baseModelObj;
			objectName = policy.getName();
			classType = AppConstants.CLASS_TYPE_RANGER_POLICY;
			content = writeObjectAsString(policy);
		}
		
		xDataHist.setObjectClassType(classType);
		xDataHist.setObjectName(objectName);
		xDataHist.setContent(content);
		xDataHist = daoMgr.getXXDataHist().create(xDataHist);
		
		if (ACTION_UPDATE.equalsIgnoreCase(action) || ACTION_DELETE.equalsIgnoreCase(action)) {
			XXDataHist prevHist = daoMgr.getXXDataHist().findLatestByObjectClassTypeAndObjectId(classType, objectId);
			
			if(prevHist == null) {
				throw restErrorUtil.createRESTException(
						"Error updating DataHistory Object. ObjectName: "
								+ objectName, MessageEnums.DATA_NOT_UPDATABLE);
			}
			
			prevHist.setVersion(version);
			prevHist.setUpdateTime(currentDate);
			prevHist.setToTime(currentDate);
			prevHist.setObjectName(objectName);
			prevHist = daoMgr.getXXDataHist().update(prevHist);
		}
	}

	public String writeObjectAsString(RangerBaseModelObject vObj) {
		ObjectMapper mapper = new ObjectMapper();

		String jsonStr;
		try {
			jsonStr = mapper.writeValueAsString(vObj);
			return jsonStr;
		} catch (JsonParseException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		} catch (JsonMappingException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		} catch (IOException e) {
			throw restErrorUtil.createRESTException(
					"Invalid input data: " + e.getMessage(),
					MessageEnums.INVALID_INPUT_DATA);
		}
	}
	
}
