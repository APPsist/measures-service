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

public class BasicSparQLQueries
{
    // needed prefix string to build SparQL queries
    // app: is the general APPsist ontology namespace
    // ipa: is the customer specific ontology namespace and must be provided as a parameter
    private static final String PREFIXSTRING = "BASE <http://www.appsist.de/ontology/> PREFIX app: <http://www.appsist.de/ontology/> ";

    // Eventbus address
    private static final String SPARQLREQUESTS = "appsist:requests:semwiki";
 
    // corresponds to rule 1.1. in "APPsist Adaptationsregeln" document
    // public static void getMeasuresForStates(String states, EventBus eb, String
    // customerPrefix,
    // Handler<Message<String>> stringHandler)  
    // {
    // String sparqlQuery = PREFIXSTRING
    // + "SELECT DISTINCT ?massnahme ?zustand "
    // + "WHERE { VALUES ?zustand " + states + " ?zustand app:bedingt ?massnahme .}";
    // sendSparQLQuery(sparqlQuery, eb, stringHandler);
    // }

    public static void getMeasuresForStates(String states, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
                + "SELECT DISTINCT ?massnahme ?station ?anlage ?massnahmenzustand ?prioritaet (count (?tmp) as ?distance) "
                + "WHERE { VALUES (?zustand ?betriebsmittel ?prioritaet) " + states
                + " ?zustand rdfs:subClassOf* ?tmp ."
                + " ?tmp rdfs:subClassOf* ?massnahmenzustand ."
                + " ?massnahmenzustand app:bedingt ?massnahme ."
                + " {{?betriebsmittel a app:Station ." + " ?betriebsmittel app:isPartOf ?anlage ."
                + " BIND (?betriebsmittel as ?station) }" + " UNION {"
                + " ?betriebsmittel a app:Anlage ." + " BIND (?betriebsmittel as ?anlage)}}}"
                + " GROUP BY ?massnahmenzustand ?station ?anlage ?massnahme ?prioritaet"
                + " ORDER BY DESC(?prioritaet) ?distance";
//        System.out.println("getMeasuresForStates query: "+ sparqlQuery);
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }

    // SparQL query to retrieve all measures stored in the system
    public static void getAllMeasures(EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
                + "SELECT DISTINCT ?massnahme ?zustand "
                + "WHERE { ?massnahme a app:Massnahme  }";
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }

    // Query retrieves all machines and stations in one or many workplace groups
    // corresponds to rule 1.2 in "APPsist Adaptionsregeln" document
    public static void getStationsInWorkplaceGroups(String workplaceGroups,
 EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING + "SELECT DISTINCT ?device "
                +
        "WHERE { VALUES ?apg " + workplaceGroups +
        "{ ?device app:isPartOf* ?apg . " +
        "{ { ?device a <http://www.appsist.de/ontology/Anlage> . }" +
        "UNION {?device a <http://www.appsist.de/ontology/Station> . } } }" +
        "OPTIONAL  {?device app:hatPrioritaet ?p .} } ORDER BY DESC(?p)";

      sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }

    // Query delivers a list of machinestates ordered by severity
    // corresponds to rule 1.3. in "APPsist Adaptationsregeln" document
    public static void getLocalStates(String stationStates, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
 + "SELECT DISTINCT ?z ?station ?p  "
                + "WHERE { VALUES (?z ?station) " + stationStates
 + " ?z  app:hatPrioritaet ?p}"
                + "  ORDER BY DESC(?p)";
        //System.out.println("getLocalStates query: "+sparqlQuery);
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }
    
 // Query determines a list of production items used in a process-step
    // corresponds to rule 1.4. in "APPsist Adaptationsregeln" document
    public static void getProductionItemsForProcessStep(String processes, EventBus eb,  Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
 + "SELECT DISTINCT ?pg "
                + "WHERE { VALUES ?ps" + processes
 + " ?ps  app:verwendet ?pg}";
        //System.out.println("getProductionItemsForProcessStep query: "+sparqlQuery);
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }


    	
 // Query delivers contents for objects with respect to targetgroup
    // corresponds to rule 1.5. in "APPsist Adaptationsregeln" document
    public static void getContentsForObjects(String objects, String position, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING
 + "SELECT DISTINCT ?inhalt ?vorschau "
                + "WHERE { VALUES ?item " + objects
 + " ?inhalt  app:informiertUeber ?item . "
                + "FILTER ((NOT EXISTS {?inhalt app:hatZielgruppe ?y}) || (EXISTS {inhalt app:hatZielgruppe "+position+"}))"
                		+ "OPTIONAL {?inhalt app:hasPreview ?vorschau}}";
        //System.out.println("getLocalStates query: "+sparqlQuery);
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }
    
    /*
     * Method retrieves labels for a set of ontologyConcepts
     * 
     * @param ontologyConcepts SparQL list of ontology concepts
     * 
     * @param lang The language in question
     * 
     * @param eb Vertx eventbus object
     * 
     * @param stringHandler A Vertx String message Handler to deal with results
     * 
     * @return Method returns nothing. Search results need to be handled by stringHandler.
     */

    public static void getLabelFor(String ontologyConcepts, String lang, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        String sparqlQuery = PREFIXSTRING + "SELECT DISTINCT ?oc ?label "
                + "WHERE { VALUES ?oc "
                + ontologyConcepts + " ?oc rdfs:label ?label FILTER(LANGMATCHES(LANG(?label), \""
                + lang + "\")) }";
       // System.out.println("getLabelForQuery: "+ sparqlQuery);
        sendSparQLQuery(sparqlQuery, eb, stringHandler);
    }

    // sends a Query to the semantic database
    // @param sparQLQuery SparQLQuery in question
    // @param eb Vertx eventbus object
    // @param stringHandler A Vertx String message Handler to deal with results
    private static void sendSparQLQuery(String sparQLQuery, EventBus eb,
            Handler<Message<String>> stringHandler)
    {
        // TODO: add logger("[MeasureService] - BasicSparQLQueries - sending Query:" +
        // sparQLQuery);
    	
        JsonObject sQuery = new JsonObject().putString("query", sparQLQuery);
        eb.send(SPARQLREQUESTS, new JsonObject().putObject("sparql", sQuery),
                stringHandler);
    }


}
