package com.epam.data;

import java.sql.Time;
import java.time.LocalTime;
import java.util.Date;

/**
 * Created by Gooner_CF on 2016/5/11.
 */

public enum TimeOfDay{
    MORNING,
    AFTERNOON,
    EVENING,
    NIGHT ;

    public static TimeOfDay getTimeOfDay(LocalTime localTime){
        int hour = localTime.getHour();
        if(hour>=6 && hour <12)
            return TimeOfDay.MORNING;
        else if(hour>=12 && hour <18)
            return TimeOfDay.AFTERNOON;
        else if(hour>=18 && hour <24)
            return TimeOfDay.EVENING;
        else if(hour>=0 && hour <6)
            return TimeOfDay.NIGHT;
        return null;
    }

    public boolean isDayTime(){
        if(this.equals(TimeOfDay.MORNING) || this.equals(TimeOfDay.AFTERNOON))
            return true;
        else
            return false;
    }

}

