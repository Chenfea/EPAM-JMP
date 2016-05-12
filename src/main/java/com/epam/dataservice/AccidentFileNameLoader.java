package com.epam.dataservice;

import com.epam.data.RoadAccident;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Created by Gooner_CF on 2016/5/12.
 */
public class AccidentFileNameLoader implements Runnable {

    private BlockingQueue<List<String>> readAccidentFileNameQueue;
    private BlockingQueue<List<RoadAccident>> readAccidentQueue;
    private List<String> fileNameList;
    public static final int batchSize=5000;
    private ExecutorService executorService;

    public AccidentFileNameLoader(BlockingQueue<List<String>> readAccidentFileQueue,
                                  BlockingQueue<List<RoadAccident>> readAccidentQueue,
                                  List fileNameList){
        this.readAccidentFileNameQueue=readAccidentFileQueue;
        this.readAccidentQueue=readAccidentQueue;
        this.fileNameList=fileNameList;
    }

    @Override
    public void run() {
        try {
            while (true) {
                List<String> fileNameList = readAccidentFileNameQueue.take();

                for (String fileName : fileNameList) {
                   // executorService.execute(new AccidentBatchLoader(batchSize, readAccidentQueue,  fileName));


                }
               // this.fileWriter.flush();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
