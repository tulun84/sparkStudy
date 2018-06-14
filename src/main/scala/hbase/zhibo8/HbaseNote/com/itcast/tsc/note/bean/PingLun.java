package hbase.zhibo8.HbaseNote.com.itcast.tsc.note.bean;

import java.util.Objects;

/**
 * 评论 Bean
 */
public class PingLun {
    private String rowKey;
    private String id;
    private String userName;
    private String userId;
    private String parentId;
    private String muId;
    private String fileName;
    private String content;
    private String createTime;
    private String updateTime;
    private String status;
    private String up;
    private String down;
    private String report;
    private String device;
    private String ip;
    private String userInfo;
    private String sysVer;
    private String platform;
    private String appName;
    private String appVer;
    private String figureurl;
    private String level;
    private String uVerified;
    private String room;

    public PingLun(String rowKey, String id, String userName, String userId, String parentId, String muId, String fileName, String content, String createTime, String updateTime, String status, String up, String down, String report, String device, String ip, String userInfo, String sysVer, String platform, String appName, String appVer, String figureurl, String level, String uVerified, String room) {
        this.rowKey = rowKey;
        this.id = id;
        this.userName = userName;
        this.userId = userId;
        this.parentId = parentId;
        this.muId = muId;
        this.fileName = fileName;
        this.content = content;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.status = status;
        this.up = up;
        this.down = down;
        this.report = report;
        this.device = device;
        this.ip = ip;
        this.userInfo = userInfo;
        this.sysVer = sysVer;
        this.platform = platform;
        this.appName = appName;
        this.appVer = appVer;
        this.figureurl = figureurl;
        this.level = level;
        this.uVerified = uVerified;
        this.room = room;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getMuId() {
        return muId;
    }

    public void setMuId(String muId) {
        this.muId = muId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getUp() {
        return up;
    }

    public void setUp(String up) {
        this.up = up;
    }

    public String getDown() {
        return down;
    }

    public void setDown(String down) {
        this.down = down;
    }

    public String getReport() {
        return report;
    }

    public void setReport(String report) {
        this.report = report;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserInfo() {
        return userInfo;
    }

    public void setUserInfo(String userInfo) {
        this.userInfo = userInfo;
    }

    public String getSysVer() {
        return sysVer;
    }

    public void setSysVer(String sysVer) {
        this.sysVer = sysVer;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppVer() {
        return appVer;
    }

    public void setAppVer(String appVer) {
        this.appVer = appVer;
    }

    public String getFigureurl() {
        return figureurl;
    }

    public void setFigureurl(String figureurl) {
        this.figureurl = figureurl;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getuVerified() {
        return uVerified;
    }

    public void setuVerified(String uVerified) {
        this.uVerified = uVerified;
    }

    public String getRoom() {
        return room;
    }

    public void setRoom(String room) {
        this.room = room;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PingLun pingLun = (PingLun) o;
        return Objects.equals(rowKey, pingLun.rowKey) &&
                Objects.equals(id, pingLun.id) &&
                Objects.equals(userName, pingLun.userName) &&
                Objects.equals(userId, pingLun.userId) &&
                Objects.equals(parentId, pingLun.parentId) &&
                Objects.equals(muId, pingLun.muId) &&
                Objects.equals(fileName, pingLun.fileName) &&
                Objects.equals(content, pingLun.content) &&
                Objects.equals(createTime, pingLun.createTime) &&
                Objects.equals(updateTime, pingLun.updateTime) &&
                Objects.equals(status, pingLun.status) &&
                Objects.equals(up, pingLun.up) &&
                Objects.equals(down, pingLun.down) &&
                Objects.equals(report, pingLun.report) &&
                Objects.equals(device, pingLun.device) &&
                Objects.equals(ip, pingLun.ip) &&
                Objects.equals(userInfo, pingLun.userInfo) &&
                Objects.equals(sysVer, pingLun.sysVer) &&
                Objects.equals(platform, pingLun.platform) &&
                Objects.equals(appName, pingLun.appName) &&
                Objects.equals(appVer, pingLun.appVer) &&
                Objects.equals(figureurl, pingLun.figureurl) &&
                Objects.equals(level, pingLun.level) &&
                Objects.equals(uVerified, pingLun.uVerified) &&
                Objects.equals(room, pingLun.room);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKey, id, userName, userId, parentId, muId, fileName, content, createTime, updateTime, status, up, down, report, device, ip, userInfo, sysVer, platform, appName, appVer, figureurl, level, uVerified, room);
    }

    @Override
    public String toString() {
        return "PingLun{" +
                "rowKey='" + rowKey + '\'' +
                ", id='" + id + '\'' +
                ", userName='" + userName + '\'' +
                ", userId='" + userId + '\'' +
                ", parentId='" + parentId + '\'' +
                ", muId='" + muId + '\'' +
                ", fileName='" + fileName + '\'' +
                ", content='" + content + '\'' +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", status='" + status + '\'' +
                ", up='" + up + '\'' +
                ", down='" + down + '\'' +
                ", report='" + report + '\'' +
                ", device='" + device + '\'' +
                ", ip='" + ip + '\'' +
                ", userInfo='" + userInfo + '\'' +
                ", sysVer='" + sysVer + '\'' +
                ", platform='" + platform + '\'' +
                ", appName='" + appName + '\'' +
                ", appVer='" + appVer + '\'' +
                ", figureurl='" + figureurl + '\'' +
                ", level='" + level + '\'' +
                ", uVerified='" + uVerified + '\'' +
                ", room='" + room + '\'' +
                '}';
    }
}
