package com.atlasseach.atlasSearch.service;

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

    public Document genresThroughTheDecades(String genre) {
        List<Bson> pipeline = List.of(
                searchMeta(
                        (SearchOperator) facet(
                                (Facet) text(
                                        fieldPath("genres"), genre
                                ),
                                (Facet) asList(
                                        new StringSearchFacet[]{stringFacet("genresFacet",
                                                fieldPath("genres")
                                        ).numBuckets(5)},
                                        numberFacet("yearFacet",
                                                fieldPath("year"),
                                                asList(1900, 1930, 1960, 1990, 2020)
                                        )
                                )
                        ),
                        searchOptions()
                                .index("testIndex02")
                )
        );

        return collection.aggregate(pipeline)
                .first();
    }

    public Document late90sMovies2(int skip, int limit, String keywords) {
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
                .first();
    }
}
