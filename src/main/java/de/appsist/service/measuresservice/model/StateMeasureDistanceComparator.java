package de.appsist.service.measuresservice.model;

import java.util.Comparator;

public class StateMeasureDistanceComparator
    implements Comparator<StateMeasure>
{

    // logger object
    @Override
    public int compare(StateMeasure o1, StateMeasure o2)
    {
        int calculatedPriority = o2.getPriority() - o1.getPriority();
        if (calculatedPriority != 0) {
            return calculatedPriority;
        }
        int calculatedDistance = o1.getDistance() - o2.getDistance();

        if (calculatedDistance == 0) {
            if (!o1.getMeasureId().equals(o2.getMeasureId())) {
                calculatedDistance = -1;
            } else {
                if (!o1.getStationId().equals(o2.getStationId())){
                    calculatedDistance = -1;
                }
            }
        }

        return calculatedDistance;
    }


}
