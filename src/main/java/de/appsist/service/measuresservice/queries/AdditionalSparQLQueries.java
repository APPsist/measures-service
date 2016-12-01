package de.appsist.service.measuresservice.queries;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/*
 * This class implements adaptation rules for the APPsist project as static methods.
 *
 * The rules are defined in document https://www.overleaf.com/3278456rsdxgn#/9204229/
 * 
 */

public class AdditionalSparQLQueries 
{
    // needed prefix string to build SparQL queries
    // app: is the general APPsist ontology namespace
    // ipa: is the customer specific ontology namespace and must be provided as a parameter
    private static final String PREFIXSTRING = "BASE <http://www.appsist.de/ontology/> PREFIX app: <http://www.appsist.de/ontology/>";

    // default customer specific ontology namespace
  
    // Eventbus address
    private static final String SPARQLREQUESTS = "appsist:requests:semwiki";

    // corresponds to SparQL rule in "APPsist Adaptionsregeln" document section 2.1.2.2
    public static void getMeasuresForPosition(String positionGoal, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
 + "SELECT DISTINCT ?m ?fkts ?sg "
                + "WHERE { VALUES ?sg " + positionGoal
	        + "{{ ?sg app:hatAufgabe ?fkt } UNION" + "{ ?sg a app:Stelle . " + "  ?stelle app:hatAufgabe ?fkt}}"
                + " ?fkts rdfs:subClassOf* ?fkt ." + " ?fkts app:hatMassnahme ?m }";
       // System.out.println("getMeasuresForPosition Query: "+ sparqlQuery);
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }


    // sends a Query to the semantic database
    // @param sparQLQuery SparQLQuery in question
    // @param eb Vertx eventbus object
    // @param stringHandler A Vertx String message Handler to deal with results
    private static void sendSparQLQuery(String sparQLQuery, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        // TODO Add logger ("[MeasureService] - BasicSparQLQueries - sending Query:" +
        // sparQLQuery);
        JsonObject sQuery = new JsonObject().putString("query", sparQLQuery);
        eb.send(SPARQLREQUESTS, new JsonObject().putObject("sparql", sQuery),
                stringHandler);
    }


}
