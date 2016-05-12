package com.epam.dataservice;

import com.epam.data.RoadAccident;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Gooner_CF on 2016/5/12.
 */
public class MultiThreadProcess {
    private PoliceForceService policeForceService;
    private ExecutorService executorService;

    private ConcurrentLinkedQueue<String> filenameQueue;
    private BlockingQueue<List<RoadAccident>> readAccidentQueue;
    private BlockingQueue<List<RoadAccident>> daytimeWriteAccidentQueue;
    private BlockingQueue<List<RoadAccident>> nighttimeWriteAccidentQueue;

    private int sizeOfReaderThreads;
    private int sizeOfEnricherThreads;
    private int batchSize;
    private String dataDir;

    public MultiThreadProcess(int sizeOfIncomingQueue,
                             int sizeOfOutgoingQueue,
                             int sizeOfThreadPool,
                             int sizeOfReaderThreads,
                             int sizeOfEnricherThreads,
                             int sizeOfBatch,
                             String dataDir){
        this.sizeOfReaderThreads = sizeOfReaderThreads;
        this.sizeOfEnricherThreads = sizeOfEnricherThreads;
        this.batchSize = sizeOfBatch;
        this.dataDir = dataDir;
        this.policeForceService = new PoliceForceService();
        this.filenameQueue = new ConcurrentLinkedQueue<String>(getFileNameList());
        this.readAccidentQueue = new ArrayBlockingQueue<List<RoadAccident>>(sizeOfIncomingQueue);
        this.daytimeWriteAccidentQueue = new ArrayBlockingQueue<List<RoadAccident>>(sizeOfOutgoingQueue);
        this.nighttimeWriteAccidentQueue = new ArrayBlockingQueue<List<RoadAccident>>(sizeOfOutgoingQueue);
        this.executorService = Executors.newFixedThreadPool(sizeOfThreadPool);
    }

    public List<String> getFileNameList(){
        List<String> fileNameList = new ArrayList<String>();
        File directory = new File(dataDir);
        for(File accidentFile: directory.listFiles()){
            String accidentFileName = accidentFile.getName();
            if(accidentFileName.indexOf("DfTRoadSafety_Accidents_")>=0){
                fileNameList.add(accidentFileName);
            }
        }
        return fileNameList;
    }



    private void accidentWriter() throws IOException {
        executorService.execute(new AccidentBatchWriter(daytimeWriteAccidentQueue, dataDir+"/DaytimeAccidents.csv"));
        executorService.execute(new AccidentBatchWriter(nighttimeWriteAccidentQueue, dataDir+"/NighttimeAccidents.csv"));
    }

    private void accidentExtender(){
        for(int i=0;i<sizeOfEnricherThreads;i++){
            executorService.execute(new AccidentBatchExtending(policeForceService, readAccidentQueue, daytimeWriteAccidentQueue, nighttimeWriteAccidentQueue));
        }
    }

    private void accidentReader(){
        for(int i=0;i<sizeOfReaderThreads;i++){
            new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                while (!filenameQueue.isEmpty()) {
                                    String filename = filenameQueue.poll();
                                    System.out.println("Start reading " + filename);
                                    Future<Integer> readerTask = executorService.submit(new AccidentBatchLoader(batchSize, readAccidentQueue, dataDir + "/" + filename));
                                    Integer counter = readerTask.get();
                                    System.out.println("Done of reading " + filename +" counter is " + counter);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
            ).start();
        }
    }

    public void startProcess() throws IOException {
        accidentReader();
        accidentExtender();
        accidentWriter();
    }

    public static void main(String[] args) throws Exception{
        String dataDir = "src/main/resources";
        int sizeOfIncomingQueue = 2;
        int sizeOfOutgoingQueue = 2;
        int sizeOfThreadPool = 20;
        int sizeOfReaderThreads = 3;
        int sizeOfEnricherThreads = 5;
        int sizeOfBatch = 40000;
        MultiThreadProcess multiThreadProcess = new MultiThreadProcess(
                sizeOfIncomingQueue,
                sizeOfOutgoingQueue,
                sizeOfThreadPool,
                sizeOfReaderThreads,
                sizeOfEnricherThreads,
                sizeOfBatch,
                dataDir);

        multiThreadProcess.startProcess();
    }


}
