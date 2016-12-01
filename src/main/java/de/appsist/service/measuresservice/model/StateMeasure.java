package de.appsist.service.measuresservice.model;

import java.util.*;

import com.google.gson.Gson;
public class StateMeasure
{

    // proposed measure for state
    private String measureId = "";

    // anlage with state
    private String anlageId = "";
    // station with state
    private String stationId = "";
    // the state
    private String deviceState = "";
    // priority of the state (higher means more important)
    private int priority = -1;
    // distance 
    private int distance = -1;

    // Map contains all stations for which the current measure must be applied for all anlagen
    private Map<String, Set<String>> anlageStationen = new HashMap<String, Set<String>>();

    public String getMeasureId()
    {
        return measureId;
    }  

    public void setMeasureId(String measureId)
    {
        this.measureId = measureId;
    }

    public String getAnlageId()
    {
        return anlageId;
    }

    public void setAnlageId(String anlageId)
    {
        this.anlageId = anlageId;
    }

    public String getStationId()
    {
        return stationId;
    }

    public void setStationId(String stationId)
    {
        this.stationId = stationId;
    }

    public String getDeviceState()
    {
        return deviceState;
    }

    public void setDeviceState(String deviceState)
    {
        this.deviceState = deviceState;
    }

    public int getPriority()
    {
        return priority;
    }

    public void setPriority(int priority)
    {
        this.priority = priority;
    }

    public int getDistance()
    {
        return distance;
    }

    public void setDistance(int distance)
    {
        this.distance = distance;
    }

    public StateMeasure()
    {

    }

    public StateMeasure(String measureId, String anlageId, String stationId, String deviceState,
            int priority,
            int distance)
    {
        this.measureId = measureId;
        this.anlageId = anlageId;
        this.stationId = stationId;
        this.deviceState = deviceState;
        this.priority = priority;
        this.distance = distance;
    }

    public StateMeasure(String measureId, String deviceState, int priority, int distance, Map<String, Set<String>> anlageStationenMap)
    {
        this.measureId = measureId;
        this.deviceState = deviceState;
        this.priority = priority;
        this.distance = distance;
        this.anlageStationen = anlageStationenMap;
        String someAnlageId = anlageStationenMap.keySet().iterator().next();
        String someStationId = anlageStationenMap.get(someAnlageId).iterator().next();
//        for (String blip : anlageStationenMap.get(someAnlageId)) {
//            System.out.println("Setting stationId to :'" + blip + "'");
//        }

        this.anlageId = someAnlageId;
        this.stationId = someStationId;
    }

    public String toString()
    {
        return "<" + this.measureId + "><" + this.anlageId + "><" + this.stationId + "><"
                + this.deviceState + ">";
    }

    public void setAnlageStationen(Map<String, Set<String>> anlageStationenMap)
    {
        this.anlageStationen.clear();
        this.anlageStationen = anlageStationenMap;
    }

    public Map<String, Set<String>> getAnlageStationen()
    {
        return this.anlageStationen;
    }

    public String printStateMeasure()
    {
        String output = "Maßnahme: " + this.getMeasureId();
        output += " | Priorität: " + this.priority + " | Distanz: " + this.distance + " | " + printAnlageStationenMap();
        return output;
    }

    public String printAnlageStationenMapAsJson()
    {
        String output = "";
        Gson gson = new Gson();

        output += gson.toJson(this.anlageStationen);
        return output;
    }
    public String printAnlageStationenMap()
    {
        String output = "";
        for (String anlage : this.anlageStationen.keySet()) {
            output += " Anlage:" + anlage + "\n";
            for (String station : this.anlageStationen.get(anlage)) {
                output += station + "|";
            }
        }
        return output;
    }
}
