package com.neu.dataprocess.entity;

/**
 * @author fengyuluo
 * @createTime 10:19 2019/4/23
 */
public class Host {
    private String name;
    private boolean containerized;
    private String architecture;
    private OS os;
    private String id;

    public Host(String name, boolean containerized, String architecture, OS os, String id) {
        this.name = name;
        this.containerized = containerized;
        this.architecture = architecture;
        this.os = os;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isContainerized() {
        return containerized;
    }

    public void setContainerized(boolean containerized) {
        this.containerized = containerized;
    }

    public String getArchitecture() {
        return architecture;
    }

    public void setArchitecture(String architecture) {
        this.architecture = architecture;
    }

    public OS getOs() {
        return os;
    }

    public void setOs(OS os) {
        this.os = os;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
