import javafx.util.Pair;

public class PointDistance {
    private Pair<Integer, Float> pointDistance;

    public PointDistance(int pointId, float distance) {
        this.pointDistance = new Pair<>(pointId, distance);
    }

    public Float getDistance() {
        return this.pointDistance.getValue();
    }

    public Integer getPointId() {
        return this.pointDistance.getKey();
    }

    @Override
    public String toString() {
        return this.pointDistance.toString();
    }
}
