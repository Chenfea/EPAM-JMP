package com.epam.dataservice;


import com.epam.data.RoadAccident;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class AccidentBatchLoader implements Callable<Integer> {

    private Integer batchSize;
    private BlockingQueue<List<RoadAccident>> dataQueue;
    private String dataFileName;
    private RoadAccidentParser roadAccidentParser;

    private Map readFinishedStatus = new HashMap<String,Boolean>();

    public Map getReadFinishedStatus() {
        return readFinishedStatus;
    }

    public void setReadFinishedStatus(Map readFinishedStatus) {
        this.readFinishedStatus = readFinishedStatus;
    }

    public AccidentBatchLoader(int batchSize, BlockingQueue<List<RoadAccident>> dataQueue, String dataFileName){
        this.batchSize = batchSize;
        this.dataQueue = dataQueue;
        this.dataFileName = dataFileName;
        roadAccidentParser = new RoadAccidentParser();
    }

    @Override
    public Integer call() throws Exception {
        int dataCount = 0;
        Iterator<CSVRecord> recordIterator = getRecordIterator();

        int batchCount = 0;
        List<RoadAccident> roadAccidentBatch = null;

        boolean isDataLoadFinished = false;
        while(!isDataLoadFinished){
            roadAccidentBatch = getNextBatch(recordIterator);
            dataCount = dataCount + roadAccidentBatch.size();
            if(roadAccidentBatch.isEmpty()){
                isDataLoadFinished = true;
                //readFinishedStatus.put(dataFileName,true);
            }else{
                ++batchCount;
                System.out.println(" Completed reading " + dataCount + " in " + batchCount + " batches for " + dataFileName);
            }
            dataQueue.put(roadAccidentBatch);
        }
        System.out.println(Thread.currentThread().getName()+" read "+dataCount+" roadAccident data.");

        //dataQueue.put(new ArrayList<>());
        /*
        for (Map.Entry<String, Boolean> entry : readFinishedStatus.entrySet())
        {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }*/
        //dataQueue.put(roadAccidentBatch); //Epmty batch can be used as identifier for end of record production
        return dataCount;
    }

    public boolean hasReadAllFiles(){
        /*boolean hasReadAllFiles=false;
        Iterator it = readFinishedStatus.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry fileReadStatus = (Map.Entry)it.next();
            String fileName= fileReadStatus.getKey();
            System.out.println("-----------------"+fileReadStatus.getKey() + " = " + fileReadStatus.getValue());
            if(fileReadStatus.getValue()){

            }
            it.remove();
        }*/
        for (Object fileName : readFinishedStatus.keySet()) {
            boolean isReadFinished = (boolean) readFinishedStatus.get(fileName);
            if(isReadFinished)
                System.out.println("FileName "+fileName+" has been read finished.");
            else
                return false;
        }
        return true;
    }

    private Iterator<CSVRecord> getRecordIterator() throws Exception{
        Reader reader = new FileReader(dataFileName);
        return new CSVParser(reader, CSVFormat.EXCEL.withHeader()).iterator();
    }

    private List<RoadAccident> getNextBatch(Iterator<CSVRecord> recordIterator){
        List<RoadAccident> roadAccidentBatch = new ArrayList<RoadAccident>();
        int recordCount = 0;
        RoadAccident roadAccidentItem = null;
        while(recordCount <= batchSize && recordIterator.hasNext() ){
            roadAccidentItem = roadAccidentParser.parseRecord(recordIterator.next());
            if(roadAccidentItem != null){
                roadAccidentBatch.add(roadAccidentItem);
                recordCount++;
            }
        }
        return  roadAccidentBatch;
    }
}
