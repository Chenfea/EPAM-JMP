package com.epam.dataservice;

import com.epam.data.RoadAccident;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Gooner_CF on 2016/5/12.
 */
public class AccidentBatchWriter implements Runnable {

    private BlockingQueue<List<RoadAccident>> roadAccidentQueue;
    private FileWriter fileWriter;

    public AccidentBatchWriter(BlockingQueue<List<RoadAccident>> roadAccidentQueue,
                               String outputFile) throws IOException {
        this.roadAccidentQueue = roadAccidentQueue;
        this.fileWriter = new FileWriter(outputFile);
    }

    @Override
    public void run() {
        try {
            while (true) {
                List<RoadAccident> roadAccidents = roadAccidentQueue.take();

                for (RoadAccident roadAccident : roadAccidents) {
                    this.fileWriter.write(roadAccident.toString());
                    this.fileWriter.write("\n");
                }
                this.fileWriter.flush();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
