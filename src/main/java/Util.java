import java.io.*;
import java.util.*;

public class Util {
    public static class PointDistanceComparator implements Comparator<PointDistance> {
        public int compare(PointDistance p1, PointDistance p2) {
            return p1.getDistance().compareTo(p2.getDistance());
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
            }
        }

        return Util.createKnnListFromQueue(pointDistanceQueue, k);
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

    public static String serializeHashMap(HashMap<Integer, HashMap<String, Integer>> cellShape) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(cellShape);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    public static HashMap<Integer, HashMap<String, Integer>> deserializeHashMap(String hashMapSerializedString) throws IOException,
            ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(hashMapSerializedString);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
        @SuppressWarnings("unchecked")
        HashMap<Integer, HashMap<String, Integer>> hashMapObject = (HashMap<Integer, HashMap<String, Integer>>) ois.readObject();
        ois.close();
        return hashMapObject;
    }

    public static ArrayList<PointDistance> createKnnListFromQueue(PriorityQueue<PointDistance> pointDistanceQueue, int k) {
        ArrayList<PointDistance> knnList = new ArrayList<>();
        while (!pointDistanceQueue.isEmpty() && knnList.size() < k) {
            PointDistance pointDistance = pointDistanceQueue.poll();
            if (knnList.size() == 0) {
                knnList.add(pointDistance);
            } else if ((pointDistance.getPointId() != knnList.get(knnList.size() - 1).getPointId()) && !(pointDistance.getDistance().equals(knnList.get(knnList.size() - 1).getDistance()))) {
                knnList.add(pointDistance);
            }
        }
        return knnList;
    }
}
