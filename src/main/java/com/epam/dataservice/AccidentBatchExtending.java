package com.epam.dataservice;

import com.epam.data.RoadAccident;
import com.epam.data.TimeOfDay;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Gooner_CF on 2016/5/12.
 */
public class AccidentBatchExtending implements Runnable {
    private BlockingQueue<List<RoadAccident>> readAccidentQueue;
    private BlockingQueue<List<RoadAccident>> daytimeWriteAccidentQueue;
    private BlockingQueue<List<RoadAccident>> nighttimeWriteAccidentQueue;

    private PoliceForceService policeForceService;

    public AccidentBatchExtending(PoliceForceService policeForceService,
                                  BlockingQueue<List<RoadAccident>> daytimeWriteAccidentQueue,
                                  BlockingQueue<List<RoadAccident>> nighttimeWriteAccidentQueue,
                                  BlockingQueue<List<RoadAccident>> readAccidentQueue){
        this.readAccidentQueue=readAccidentQueue;
        this.daytimeWriteAccidentQueue=daytimeWriteAccidentQueue;
        this.nighttimeWriteAccidentQueue=nighttimeWriteAccidentQueue;
        this.policeForceService=policeForceService;
    }


    @Override
    public void run() {
        try {
            List<RoadAccident> roadAccidents = readAccidentQueue.take();
            System.out.println("roadAccidents size is "+ roadAccidents.size());
            List<RoadAccident> daytimeRoadAccidents = new ArrayList<RoadAccident>();
            List<RoadAccident> nighttimeRoadAccidents = new ArrayList<RoadAccident>();

            for(RoadAccident roadAccident: roadAccidents){
                extendRoadAccident(roadAccident); // add attributes forceContact and TimeofDay into roadAccident.
                if(roadAccident != null ){
                    if(roadAccident.getTimeOfDay().isDayTime())
                        daytimeRoadAccidents.add(roadAccident);
                    else
                        nighttimeRoadAccidents.add(roadAccident);
                }
            }
            if (!daytimeRoadAccidents.isEmpty())
                daytimeWriteAccidentQueue.put(daytimeRoadAccidents);
            if (!nighttimeRoadAccidents.isEmpty())
                nighttimeWriteAccidentQueue.put(nighttimeRoadAccidents);
            System.out.println("Thread is"+ Thread.currentThread().getId()+", "+ Thread.currentThread().getName());
        }catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void extendRoadAccident(RoadAccident roadAccident){
        String forceContact = policeForceService.getContactNo(roadAccident.getPoliceForce()) ;
        TimeOfDay timeOfDay = TimeOfDay.getTimeOfDay(roadAccident.getTime());
        roadAccident.setForceContact(forceContact);
        roadAccident.setTimeOfDay(timeOfDay);
    }
}
