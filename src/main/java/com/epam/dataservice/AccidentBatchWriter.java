package com.epam.dataservice;

import com.epam.data.RoadAccident;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;



/**
 * Created by Gooner_CF on 2016/5/12.
 */
public class AccidentBatchWriter implements Runnable {

    private BlockingQueue<List<RoadAccident>> roadAccidentQueue;
    private FileWriter fileWriter;

    public static AtomicInteger writeRecordCount= new AtomicInteger(0);

    public AccidentBatchWriter(BlockingQueue<List<RoadAccident>> roadAccidentQueue,
                               String outputFile) throws IOException {
        this.roadAccidentQueue = roadAccidentQueue;
        this.fileWriter = new FileWriter(outputFile);
    }

    @Override
    public void run() {
        try {
            //int writeRecordCount=0;
            boolean isEmptyRoadAccidentQueue=false;
            while (!isEmptyRoadAccidentQueue) {
                //List<RoadAccident> roadAccidents = roadAccidentQueue.take();
                /*if(roadAccidents.isEmpty())
                    isEmptyRoadAccidentQueue=true;*/
                //if(writeRecordCount.intValue()==
                /*if(writeRecordCount.intValue() == 753623) {

                    //executorService.shutdown();
                    roadAccidentQueue.put(new ArrayList<>());
                    isEmptyRoadAccidentQueue=true;
                }*/
                List<RoadAccident> roadAccidents = roadAccidentQueue.take();
                //List<RoadAccident> roadAccidents = roadAccidentQueue.poll(60, TimeUnit.SECONDS);
                /*if(roadAccidents.isEmpty()) {
                    roadAccidentQueue.put(new ArrayList<>());
                    //isEmptyRoadAccidentQueue=true;
                    this.fileWriter.close();
                    return;
                }*/

                for (RoadAccident roadAccident : roadAccidents) {
                    this.fileWriter.write(roadAccident.toString());
                    this.fileWriter.write("\n");
                }
                this.fileWriter.flush();

                writeRecordCount.addAndGet(roadAccidents.size());
                System.out.println(writeRecordCount+" records has been wroten already!");


            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
