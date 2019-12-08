import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

public class PointDistance {
    private Map.Entry<Integer, Float> pointDistance;

    public PointDistance(int pointId, float distance) {
        this.pointDistance = new SimpleEntry<>(pointId, distance);
    }

    public Float getDistance() {
        return this.pointDistance.getValue();
    }

    public int getPointId() {
        return this.pointDistance.getKey();
    }

    @Override
    public String toString() {
        return this.pointDistance.toString();
    }
}
