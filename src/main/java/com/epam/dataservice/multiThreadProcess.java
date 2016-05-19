package com.epam.dataservice;

import com.epam.data.RoadAccident;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Gooner_CF on 2016/5/12.
 */
public class MultiThreadProcess {
    private PoliceForceService policeForceService;
    //private ExecutorService executorService;

    private ConcurrentLinkedQueue<String> filenameQueue;
    private BlockingQueue<List<RoadAccident>> readAccidentQueue;
    private BlockingQueue<List<RoadAccident>> daytimeWriteAccidentQueue;
    private BlockingQueue<List<RoadAccident>> nighttimeWriteAccidentQueue;

    private int sizeOfReaderThreads;
    private int sizeOfEnricherThreads;
    private int batchSize;
    private String dataDir;

    //private Map fileStatus = new HashMap<String, Boolean>();
    private final ExecutorService readExecutor;
    private final ExecutorService extendExecutor;
    private final ExecutorService writeExecutor;
    //private final ScheduledExecutorService writeExecutor;
    private final Collection<Future<Integer>> futures = new ArrayList<>();

    public static AtomicInteger futureCount=new AtomicInteger(0);


    public MultiThreadProcess(int sizeOfIncomingQueue,
                              int sizeOfOutgoingQueue,
                              int sizeOfThreadPool,
                              int sizeOfReaderThreads,
                              int sizeOfEnricherThreads,
                              int sizeOfBatch,
                              String dataDir) {
        this.sizeOfReaderThreads = sizeOfReaderThreads;
        this.sizeOfEnricherThreads = sizeOfEnricherThreads;
        this.batchSize = sizeOfBatch;
        this.dataDir = dataDir;
        this.policeForceService = new PoliceForceService();
        this.filenameQueue = new ConcurrentLinkedQueue<String>(getFileNameList());
        this.readAccidentQueue = new ArrayBlockingQueue<List<RoadAccident>>(sizeOfIncomingQueue);
        this.daytimeWriteAccidentQueue = new ArrayBlockingQueue<List<RoadAccident>>(sizeOfOutgoingQueue);
        this.nighttimeWriteAccidentQueue = new ArrayBlockingQueue<List<RoadAccident>>(sizeOfOutgoingQueue);
        //this.executorService = Executors.newFixedThreadPool(sizeOfThreadPool);
        this.readExecutor = Executors.newFixedThreadPool(sizeOfReaderThreads);
        this.extendExecutor = new ScheduledThreadPoolExecutor(sizeOfThreadPool);//Executors.newFixedThreadPool(sizeOfThreadPool);
        this.writeExecutor = Executors.newFixedThreadPool(sizeOfThreadPool);
        //this.writeExecutor = new ScheduledThreadPoolExecutor(sizeOfThreadPool);//Executors.newFixedThreadPool(sizeOfThreadPool);
    }
/*
    public class FileStatus{
        String fileName;
        int rowCount=0;
        boolean hasLoaded;
        boolean hasProcessed;
        boolean hasWritten;
        public  FileStatus(String fileName){
            this.fileName= fileName;
        }
    }

    public static Map<String, FileStatus> fileStatusMap= new ConcurrentHashMap<>();*/

    public List<String> getFileNameList() {
        List<String> fileNameList = new ArrayList<String>();
        File directory = new File(dataDir);
        for (File accidentFile : directory.listFiles()) {
            String accidentFileName = accidentFile.getName();
            if (accidentFileName.indexOf("DfTRoadSafety_Accidents_") >= 0) {
                fileNameList.add(accidentFileName);
                //fileStatusMap.put(accidentFileName, new FileStatus(accidentFileName));
            }
        }
        return fileNameList;
    }


    private void accidentWriter() throws IOException {
        writeExecutor.execute(new AccidentBatchWriter(daytimeWriteAccidentQueue, dataDir + "/DaytimeAccidents.csv"));
        writeExecutor.execute(new AccidentBatchWriter(nighttimeWriteAccidentQueue, dataDir + "/NighttimeAccidents.csv"));
        /*try{
            if (writeExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("task completed");
            } else {
                System.out.println("Forcing shutdown...");
                writeExecutor.shutdownNow();
            }
        }catch (InterruptedException e){
            System.out.println("Interrupted, so exiting.");
        }*/
        writeExecutor.shutdown();
        try {
            final boolean writeDone = writeExecutor.awaitTermination(40, TimeUnit.SECONDS);
            if(writeDone)
                System.out.println("All write Thread is stop!");
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    private void accidentExtender() {
        /*for (int i = 0; i < sizeOfEnricherThreads; i++) {
            try {
                Future extendFuture = extendExecutor.submit(new AccidentBatchExtending(policeForceService, readAccidentQueue, daytimeWriteAccidentQueue, nighttimeWriteAccidentQueue));
                System.out.println("extendFuture.get()=" + extendFuture.get());
            }catch (InterruptedException e){
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }*/
        for (int i = 0; i < sizeOfEnricherThreads; i++) {
            //AccidentBatchExtending task = new AccidentBatchExtending(policeForceService, readAccidentQueue, daytimeWriteAccidentQueue, nighttimeWriteAccidentQueue);
            extendExecutor.submit(new AccidentBatchExtending(policeForceService, readAccidentQueue, daytimeWriteAccidentQueue, nighttimeWriteAccidentQueue));

        }
        extendExecutor.shutdown();
        try {
            final boolean extendDone = extendExecutor.awaitTermination(30, TimeUnit.SECONDS);
            if(extendDone)
                System.out.println("All extend Thread is stop!");
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        //extendExecutor.awaitTermination()
        /*try {
            if (executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        }catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }*/
        //List<Future<Integer>> futures = executorService.invokeAll(Arrays.asList(new AccidentBatchExtending()), 10, TimeUnit.MINUTES);// Timeout of 10 minutes.
        //executorService.shutdown();
        //executorService.shutdownNow();
        /*try{
            if (extendExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("task completed");
            } else {
                System.out.println("Forcing shutdown...");
                extendExecutor.shutdownNow();
            }
        }catch (InterruptedException e){
            System.out.println("Interrupted, so exiting.");
        }*/
    }

    private void startAccidentReader() {
        try {
            while (!filenameQueue.isEmpty()) {
                String filename = filenameQueue.poll();
                System.out.println("Start reading " + filename);
                futures.add( readExecutor.submit(new AccidentBatchLoader(batchSize, readAccidentQueue, dataDir + "/" + filename)));
//                System.out.println("Done of reading " + filename + " counter is " + counter);
                //fileStatus.put(filename, true);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

            //readExecutor.shutdown();


    }



    public void startProcess() throws IOException {
        startAccidentReader();
        accidentExtender();
        accidentWriter();
        /*if(accidentBatchLoader.hasReadAllFiles()){
            System.out.println("##################All files has been read.");
        }*/
        checkFutures();
        System.out.println("Total extend :"+AccidentBatchExtending.extendRecordCount +" records.");
        //readExecutor.shutdown();

    }

    private void checkFutures() {
        int count = 0;
        int futureCount=0;
        for(Future<Integer> future : futures){
            try {
                count += future.get();
                futureCount++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Total read " + count+", there are "+futureCount+" futures.");

    }

    public static void main(String[] args) throws Exception {
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
