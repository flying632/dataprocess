package com.neu.dataprocess.entity;

/**
 * @author fengyuluo
 * @createTime 22:53 2019/4/23
 */
public class OS {
    private String platform;
    private String version;
    private String family;
    private String name;
    private String codename;

    public OS(String platform, String version, String family, String name, String codename) {
        this.platform = platform;
        this.version = version;
        this.family = family;
        this.name = name;
        this.codename = codename;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCodename() {
        return codename;
    }

    public void setCodename(String codename) {
        this.codename = codename;
    }
}
