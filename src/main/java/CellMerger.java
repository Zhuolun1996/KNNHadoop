import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.HashMap;

public class CellMerger {
    public HashMap<Integer, HashMap<String, Integer>> mergeCell(String filePath) {
        HashMap<Integer, HashMap<String, Integer>> shape = new HashMap<>();
        int k = Integer.parseInt(KnnMapReduce.knnConf.get("k"));
        int size = Integer.parseInt(KnnMapReduce.knnConf.get("size"));
        int grain = Integer.parseInt(KnnMapReduce.knnConf.get("grain"));
        int cellWidth = size / grain;

        try {
            File file = new File(filePath);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = reader.readLine()) != null) {
                int cellId = Integer.parseInt(line.split("\t")[0]);
                int numberOfPoints = Integer.parseInt(line.split("\t")[1]);
                shape.put(cellId, this.createCellInfo(cellId, grain, cellWidth, numberOfPoints));
            }

            for (int i = 0; i < grain * grain; i++) {
                if (shape.get(i) == null) {
                    shape.put(i, this.createCellInfo(i, grain, cellWidth, 0));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < grain * grain; i++) {
            if (shape.get(i) != null) {
                int numberOfPoints = shape.get(i).get("pointsNum");
                if (numberOfPoints < k + 1) {
                    int position = checkPosition(i, grain);
                    int totalNum = 0;
                    HashMap<String, Integer> cellInfo = shape.get(i);
                    if (position == 0) {
                        totalNum += shape.get(i).get("pointsNum");
                        shape.remove(i);
                        totalNum += shape.get(i + 1).get("pointsNum");
                        shape.remove(i + 1);
                        totalNum += shape.get(i + grain).get("pointsNum");
                        shape.remove(i + grain);
                        totalNum += shape.get(i + grain + 1).get("pointsNum");
                        shape.remove(i + grain + 1);
                        cellInfo.replace("pointsNum", totalNum);
                        cellInfo.replace("rightMargin", cellInfo.get("rightMargin") + cellWidth);
                        cellInfo.replace("bottomMargin", cellInfo.get("bottomMargin") + cellWidth);
                        shape.put(i, cellInfo);
                    } else if (position == 1) {
                        totalNum += shape.get(i).get("pointsNum");
                        shape.remove(i);
                        totalNum += shape.get(i - 1).get("pointsNum");
                        shape.remove(i - 1);
                        totalNum += shape.get(i + grain).get("pointsNum");
                        shape.remove(i + grain);
                        totalNum += shape.get(i + grain - 1).get("pointsNum");
                        shape.remove(i + grain - 1);
                        cellInfo.replace("pointsNum", totalNum);
                        cellInfo.replace("leftMargin", cellInfo.get("leftMargin") - cellWidth);
                        cellInfo.replace("bottomMargin", cellInfo.get("bottomMargin") + cellWidth);
                        shape.put(i, cellInfo);
                    } else if (position == 2) {
                        totalNum += shape.get(i).get("pointsNum");
                        shape.remove(i);
                        totalNum += shape.get(i + 1).get("pointsNum");
                        shape.remove(i + 1);
                        totalNum += shape.get(i - grain).get("pointsNum");
                        shape.remove(i - grain);
                        totalNum += shape.get(i - grain + 1).get("pointsNum");
                        shape.remove(i - grain + 1);
                        cellInfo.replace("pointsNum", totalNum);
                        cellInfo.replace("rightMargin", cellInfo.get("rightMargin") + cellWidth);
                        cellInfo.replace("topMargin", cellInfo.get("topMargin") - cellWidth);
                        shape.put(i, cellInfo);
                    } else if (position == 3) {
                        totalNum += shape.get(i).get("pointsNum");
                        shape.remove(i);
                        totalNum += shape.get(i - 1).get("pointsNum");
                        shape.remove(i - 1);
                        totalNum += shape.get(i - grain).get("pointsNum");
                        shape.remove(i - grain);
                        totalNum += shape.get(i - grain - 1).get("pointsNum");
                        shape.remove(i - grain - 1);
                        cellInfo.replace("pointsNum", totalNum);
                        cellInfo.replace("leftMargin", cellInfo.get("leftMargin") - cellWidth);
                        cellInfo.replace("topMargin", cellInfo.get("topMargin") - cellWidth);
                        shape.put(i, cellInfo);
                    }
                }
            }
        }

        HashMap<Integer, HashMap<String, Integer>> reformattedShape = new HashMap<>();
        int cellNum = 0;
        for (int i = 0; i < grain * grain; i++) {
            if (shape.get(i) != null) {
                reformattedShape.put(cellNum, shape.get(i));
                cellNum++;
            }
        }
        return reformattedShape;
    }

    private int checkPosition(int cellId, int grain) {
        int row = cellId / grain;
        int column = cellId % grain;

        if (row % 2 == 0 && column % 2 == 0) {
            return 0;
        } else if (row % 2 == 0 && column % 2 == 1) {
            return 1;
        } else if (row % 2 == 1 && column % 2 == 0) {
            return 2;
        } else if (row % 2 == 1 && column % 2 == 1) {
            return 3;
        }

        return -1;
    }

    private HashMap<String, Integer> createCellInfo(int cellId, int grain, int cellWidth, int numberOfPoints) {
        HashMap<String, Integer> cellInfo = new HashMap<String, Integer>();
        int row = cellId / grain;
        int column = cellId % grain;

        int leftMargin = column * cellWidth;
        int rightMargin = (column + 1) * cellWidth;
        int topMargin = row * cellWidth;
        int bottomMargin = (row + 1) * cellWidth;
        cellInfo.put("pointsNum", numberOfPoints);
        cellInfo.put("leftMargin", leftMargin);
        cellInfo.put("rightMargin", rightMargin);
        cellInfo.put("topMargin", topMargin);
        cellInfo.put("bottomMargin", bottomMargin);
        return cellInfo;
    }
}
