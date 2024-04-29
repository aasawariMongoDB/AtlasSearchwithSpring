package com.atlasseach.atlasSearch.service;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Facet;
import com.mongodb.client.model.search.SearchOperator;
import com.mongodb.client.model.search.SearchScore;
import com.mongodb.client.model.search.StringSearchFacet;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.search.SearchCount.total;
import static com.mongodb.client.model.search.SearchFacet.numberFacet;
import static com.mongodb.client.model.search.SearchFacet.stringFacet;
import static com.mongodb.client.model.search.SearchOperator.*;
import static com.mongodb.client.model.search.SearchOptions.searchOptions;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static java.util.Arrays.asList;


@Service
public class MovieAtlasSearchService {

    private final MongoCollection<Document> collection;

    public MovieAtlasSearchService(MongoTemplate mongoTemplate) {
        MongoDatabase database = mongoTemplate.getDb();
        this.collection = database.getCollection("movies");
    }

    public ArrayList<Document> searchMovies(String keywords) {
        List<Document> Query1 = new ArrayList<>();
        Query1.add(new Document("$search", new Document("index", "testIndex01")
                .append("text", new Document("query", keywords)
                        .append("path", "fullplot"))));
        Query1.add(new Document("$project", new Document("title", 1)
                .append("year", 1)
                .append("fullplot", 1)
                .append("imdb.rating", 1)));

        return collection.aggregate(Query1).into(new ArrayList<>());
    }

    public Collection<Document> late90sMovies(String keywords) {
        List<Bson> pipeline = asList(
                search(
                        compound()
                                .must(List.of(numberRange(
                                                fieldPath("year"))
                                                .gteLt(1995, 2000)
                                )).should(List.of(
                                        text(fieldPath("fullplot"), keywords
                                        )
                                )),
                        searchOptions().index("testIndex01")
                ),
                project(fields(
                        excludeId(),
                        include("title", "year", "fullplot", "imdb.rating")
                ))
        );

        return collection.aggregate(pipeline)
                .into(new ArrayList<>());
    }

    public Collection<Document> late90sMovies(String keywords, SearchScore modifier) {
        List<Bson> pipeline = asList(
                search(
                        compound()
                                .must(List.of(
                                        numberRange(
                                                fieldPath("year"))
                                                .gteLt(1995, 2000)
                                ))
                                .should(List.of(
                                        text(
                                                fieldPath("fullplot"), keywords
                                        )
                                                .score(modifier)
                                )),
                        searchOptions()
                                .index("testIndex01")
                ),
                project(fields(
                        excludeId(),
                        include("title", "year", "fullplot", "imdb.rating"),
                        metaSearchScore("score")
                ))
        );

        return collection.aggregate(pipeline)
                .into(new ArrayList<>());
    }

    public Document countLate90sMovies(String keywords) {
        List<Bson> pipeline = List.of(
                searchMeta(
                        compound()
                                .must(asList(
                                        numberRange(
                                                fieldPath("year"))
                                                .gteLt(1995, 2000),
                                        text(
                                                fieldPath("fullplot"), keywords
                                        )
                                )),
                        searchOptions()
                                .index("testIndex01")
                                .count(total())
                )
        );

        return collection.aggregate(pipeline)
                .first();
    }

    public List<Document> genresThroughTheDecades(String genre) {
        List<Bson> pipeline = Arrays.asList(
                new Document("$search",
                        new Document("index", "testIndex02")
                                .append("compound",
                                        new Document("should", Arrays.asList(
                                                new Document("text",
                                                        new Document("query", "horror")
                                                                .append("path", "genres")))))),
                new Document("$facet",
                        new Document("genresFacet", Arrays.asList(
                                new Document("$sortByCount", "$genres"),
                                new Document("$limit", 5L)))
                                .append("yearFacet", Arrays.asList(
                                        new Document("$bucket",
                                                new Document("groupBy", "$year")
                                                        .append("boundaries", Arrays.asList(1900L, 1930L, 1960L, 1990L, 2020L))
                                                        .append("default", "other")
                                                        .append("output",
                                                                new Document("count",
                                                                        new Document("$sum", 1L))))))),
                new Document("$limit", 1L)
        );
        AggregateIterable<Document> aggregationResult = collection.aggregate(pipeline);
        List<Document> resultList = new ArrayList<>();

        // Iterate over the aggregation result and add documents to the list
        for (Document document : aggregationResult) {
            resultList.add(document);
        }
        return resultList;
    }

    public Collection<Document> late90sMovies2(int skip, int limit, String keywords) {
        List<Bson> pipeline = asList(
                search(
                        compound()
                                .must(List.of(numberRange(
                                        fieldPath("year"))
                                        .gteLt(1995, 2000)
                                )).should(List.of(
                                        text(fieldPath("fullplot"), keywords
                                        )
                                )),
                        searchOptions().index("testIndex02")
                ),
                project(fields(
                        excludeId(),
                        include("title", "year", "fullplot", "imdb.rating")
                )),
                facet(
                        new Facet("rows",
                                skip(skip),
                                limit(limit)
                        ),
                        new Facet("totalRows",
                                replaceWith("$$SEARCH_META"),
                                limit(1)
                        )
                )
        );
        return collection.aggregate(pipeline)
                .into(new ArrayList<>());
    }
}
