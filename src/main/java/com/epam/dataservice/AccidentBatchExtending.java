package com.epam.dataservice;

import com.epam.data.RoadAccident;
import com.epam.data.TimeOfDay;

import com.epam.dataservice.PoliceForceService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Gooner_CF on 2016/5/12.
 */
public class AccidentBatchExtending implements Runnable {

    private PoliceForceService policeForceService;
    private BlockingQueue<List<RoadAccident>> readAccidentQueue;
    private BlockingQueue<List<RoadAccident>> daytimeWriteAccidentQueue;
    private BlockingQueue<List<RoadAccident>> nighttimeWriteAccidentQueue;
    //private ExecutorService executorService;

    public static AtomicInteger extendRecordCount= new AtomicInteger(0);

    public AccidentBatchExtending(PoliceForceService policeForceService,
                                 BlockingQueue<List<RoadAccident>> readAccidentQueue,
                                 BlockingQueue<List<RoadAccident>> daytimeWriteAccidentQueue,
                                 BlockingQueue<List<RoadAccident>> nighttimeWriteAccidentQueue) {
        this.policeForceService = policeForceService;
        this.readAccidentQueue = readAccidentQueue;
        this.daytimeWriteAccidentQueue = daytimeWriteAccidentQueue;
        this.nighttimeWriteAccidentQueue = nighttimeWriteAccidentQueue;

    }
    public AccidentBatchExtending(){}

    @Override
    public void run() {
        try {
            long counter = 0;
            //AtomicInteger extendingCount=new AtomicInteger(0);
            //while(true){
            boolean isEmptyReadAccidentQueue = false;
            while(!isEmptyReadAccidentQueue){
                /*if(extendRecordCount.intValue() == 753623) {
                    readAccidentQueue.put(new ArrayList<>());
                }*/
                List<RoadAccident> roadAccidents = readAccidentQueue.take();
                /*if(roadAccidents.isEmpty()) {

                    isEmptyReadAccidentQueue = true;
                    return;
                }*/
                //List<RoadAccident> roadAccidents = readAccidentQueue.poll(50, TimeUnit.SECONDS);
                counter += roadAccidents.size();
                List<RoadAccident> daytimeRoadAccidents = new ArrayList<RoadAccident>();
                List<RoadAccident> nighttimeRoadAccidents = new ArrayList<RoadAccident>();

                for (RoadAccident roadAccident : roadAccidents) {
                    enrich(roadAccident); // add attributes forceContact and TimeofDay into roadAccident.
                    if (roadAccident != null) {
                        if (roadAccident.getTimeOfDay().isDayTime())
                            daytimeRoadAccidents.add(roadAccident);
                        else
                            nighttimeRoadAccidents.add(roadAccident);
                    }
                    extendRecordCount.getAndIncrement();
                }
                if(!daytimeRoadAccidents.isEmpty())
                    daytimeWriteAccidentQueue.put(daytimeRoadAccidents);
                if(!nighttimeRoadAccidents.isEmpty())
                    nighttimeWriteAccidentQueue.put(nighttimeRoadAccidents);
                /*if(readAccidentQueue.isEmpty()){
                    isEmptyReadAccidentQueue=true;
                }*/
                //extendingCount.getAndIncrement();

                System.out.println("Extending Thread["+Thread.currentThread().getId()+"] completed Extending " + counter+" records.");
                System.out.println("Totally Extending "+extendRecordCount+" records!");
            }
            //System.out.println("There is "+extendingCount+" records has been extended!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void enrich(RoadAccident roadAccident){
        String forceContact = policeForceService.getContactNo(roadAccident.getPoliceForce());
        roadAccident.setForceContact(forceContact);
        TimeOfDay dayTime = TimeOfDay.getTimeOfDay(roadAccident.getTime());
        roadAccident.setTimeOfDay(dayTime);
    }
}
