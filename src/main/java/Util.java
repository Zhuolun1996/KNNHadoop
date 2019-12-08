import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class Util {
    public static class PointDistanceComparator implements Comparator<PointDistance> {
        public int compare(PointDistance p1, PointDistance p2) {
            return p2.getDistance().compareTo(p1.getDistance());
        }
    }

    private static Float getDistance(PointInfo p1, PointInfo p2) {
        return (float) Math.sqrt(Math.pow(p2.getX() - p1.getX(), 2) + Math.pow(p2.getY() - p1.getY(), 2));
    }

    public static ArrayList<PointDistance> getKnnList(PointInfo pointInfo, ArrayList<PointInfo> pointInfoList, int k) {
        PriorityQueue<PointDistance> pointDistanceQueue = new PriorityQueue<>(new PointDistanceComparator());
        ArrayList<PointDistance> knnList = new ArrayList<>();
        if (pointInfo.getKnnList() != null) {
            pointDistanceQueue.addAll(pointInfo.getKnnList());
        }
        for (PointInfo otherPointInfo : pointInfoList) {
            if (!(otherPointInfo.getPointId() == pointInfo.getPointId())) {
                float distance = getDistance(pointInfo, otherPointInfo);
                PointDistance pointDistance = new PointDistance(otherPointInfo.getPointId(), distance);
                pointDistanceQueue.add(pointDistance);
                if (pointDistanceQueue.size() > k) {
                    pointDistanceQueue.poll();
                }
            }
        }
        while (!pointDistanceQueue.isEmpty()) {
            PointDistance pointDistance = pointDistanceQueue.poll();
            knnList.add(pointDistance);
        }
        return knnList;
    }

    public static ArrayList<PointDistance> knnListStringToKnnList(String knnListString) {
        ArrayList<PointDistance> knnList = new ArrayList<>();
        knnListString = knnListString.replace("[", "").replace("]", "");
        String[] entryArray = knnListString.split(",");
        for (String s : entryArray) {
            String entryString = s.replace(" ", "");
            int _pointId = Integer.parseInt(entryString.split("=")[0]);
            float _distance = Float.parseFloat(entryString.split("=")[1]);
            knnList.add(new PointDistance(_pointId, _distance));
        }

        return knnList;
    }
}
