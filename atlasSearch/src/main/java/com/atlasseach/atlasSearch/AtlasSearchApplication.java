package com.atlasseach.atlasSearch;

import com.atlasseach.atlasSearch.service.MovieAtlasSearchService;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.search.SearchScore;
import org.bson.Document;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static com.mongodb.client.model.search.SearchPath.fieldPath;

@SpringBootApplication
public class AtlasSearchApplication {
	public static void main(String[] args) {
		String uri = "mongodb+srv://theuser:pwd@cluster0.k5dqp.mongodb.net/?retryWrites=true&w=majority&appName=cluster0";

		try (MongoClient mongoClient = MongoClients.create(uri)) {
			// set namespace
			MongoDatabase database = mongoClient.getDatabase("sample_mflix");
			MongoCollection<Document> collection = database.getCollection("movies");

			SearchIndexModel indexOne = new SearchIndexModel("testIndex01",
					new Document("mappings",
							new Document("dynamic", true)));

			SearchIndexModel indexTwo = new SearchIndexModel("testIndex02",
					new Document("mappings",
							new Document("dynamic", true).append("fields",
									new Document().append("genres",
											new Document().append("type", "stringFacet")
									).append("year",
											new Document().append("type", "numberFacet")
									)))
			);

			collection.createSearchIndexes(Arrays.asList(indexOne, indexTwo));
            System.out.println("Wait for three minutes for the search indexes to be created.");
			// Wait for 3 mins to let Atlas Create the indexes.
			Thread.sleep(180000);

			MovieAtlasSearchService searchService = new MovieAtlasSearchService(new MongoTemplate(mongoClient, "sample_mflix"));

			// Call your search methods
			ArrayList<Document> searchResults = searchService.searchMovies("space cowboy");
			Collection<Document> late90sMovies = searchService.late90sMovies("hacker assassin");
			Collection<Document> late90sMoviesWithScore = searchService.late90sMovies("hacker assassin", SearchScore.boost(fieldPath("imdb.votes")));
			Document late90sMoviesCount = searchService.countLate90sMovies("hacker assassin”");


			// Process the results as needed
			System.out.println("Search results: " + searchResults);
			System.out.println("Late 90s movies: " + late90sMovies);
			System.out.println("Late 90s movies with score: " + late90sMoviesWithScore);
			System.out.println("Late 90s movies count: " + late90sMoviesCount);



			//Dropping index after execution.
			collection.dropSearchIndex("testIndex01");
			collection.dropSearchIndex("testIndex02");

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
