import java.util.ArrayList;

public class PointInfo {
    private Integer pointId;
    private Integer cellId;
    private Integer x;
    private Integer y;
    private ArrayList<PointDistance> knnList;

    public PointInfo(String pointInfoString, int mode) {
        if (mode == 0) {
            String[] pointInfoArray = pointInfoString.split(",");
            this.pointId = Integer.parseInt(pointInfoArray[0]);
            this.x = Integer.parseInt(pointInfoArray[1]);
            this.y = Integer.parseInt(pointInfoArray[2]);
        } else if (mode == 1) {
            String[] pointInfoArray = pointInfoString.split("\t");
            this.pointId = Integer.parseInt(pointInfoArray[0]);
            pointInfoArray = pointInfoArray[1].split(";");
            this.x = Integer.parseInt(pointInfoArray[0]);
            this.y = Integer.parseInt(pointInfoArray[1]);
            this.cellId = Integer.parseInt(pointInfoArray[2]);
            this.knnList = new ArrayList<>();
            String knnListString = pointInfoArray[3];
            this.knnList = Util.knnListStringToKnnList(knnListString);
        } else if (mode == 2) {
            String[] pointInfoArray = pointInfoString.split(";");
            this.pointId = Integer.parseInt(pointInfoArray[0]);
            this.x = Integer.parseInt(pointInfoArray[1]);
            this.y = Integer.parseInt(pointInfoArray[2]);
        } else if (mode == 3) {
            String[] pointInfoArray = pointInfoString.split(";");
            this.pointId = Integer.parseInt(pointInfoArray[0]);
            this.x = Integer.parseInt(pointInfoArray[1]);
            this.y = Integer.parseInt(pointInfoArray[2]);
            this.cellId = Integer.parseInt(pointInfoArray[3]);
            this.knnList = new ArrayList<>();
            String knnListString = pointInfoArray[4];
            this.knnList = Util.knnListStringToKnnList(knnListString);
        }
    }


    public void setKnnList(ArrayList<PointDistance> knnList) {
        this.knnList = knnList;
    }

    public Integer getPointId() {
        return this.pointId;
    }

    public Integer getCellId() {
        return this.cellId;
    }

    public Integer getX() {
        return this.x;
    }

    public Integer getY() {
        return this.y;
    }


    public ArrayList<PointDistance> getKnnList() {
        return this.knnList;
    }

    @Override
    public String toString() {
        return this.getPointId() + "," + this.getX() + "," + this.getY();
    }
}