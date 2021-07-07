package model;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;
import java.util.Set;

//
// New Class from from Ranger 2.2.0 version
// Class has been edited
//

@JsonAutoDetect(fieldVisibility= JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerRoles implements Serializable {
    private static final long serialVersionUID = 1L;

    private String           serviceName;
    private Long             roleVersion;
    private Date             roleUpdateTime;
    private Set<RangerRole>  rangerRoles;

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public Long getRoleVersion() {
        return roleVersion;
    }

    public void setRoleVersion(Long roleVersion) {
        this.roleVersion = roleVersion;
    }

    public Date getRoleUpdateTime() {
        return roleUpdateTime;
    }

    public void setRoleUpdateTime(Date roleUpdateTime) {
        this.roleUpdateTime = roleUpdateTime;
    }

    public Set<RangerRole> getRangerRoles(){
        return this.rangerRoles;
    }

    public void setRangerRoles(Set<RangerRole> rangerRoles){
        this.rangerRoles = rangerRoles;
    }

    @Override
    public String toString() {
        return "RangerRoles=" + serviceName + ", "
                + "roleVersion=" + roleVersion + ", "
                + "roleUpdateTime=" + roleUpdateTime + ", "
                + "rangerRoles=" + rangerRoles
                ;
    }
}
