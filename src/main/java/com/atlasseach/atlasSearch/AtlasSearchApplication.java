package com.atlasseach.atlasSearch;

import com.atlasseach.atlasSearch.service.MovieAtlasSearchService;
import com.mongodb.client.*;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.search.SearchScore;
import org.bson.Document;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.mongodb.client.model.search.SearchPath.fieldPath;

@SpringBootApplication
public class AtlasSearchApplication {
	public static void main(String[] args) {
		String uri = "mongodb+srv://theuser:pwd@cluster0.k5dqp.mongodb.net/?retryWrites=true&w=majority&appName=cluster0";

		try (MongoClient mongoClient = MongoClients.create(uri)) {
			MongoDatabase database = mongoClient.getDatabase("sample_mflix");
			MongoCollection<Document> collection = database.getCollection("movies");

			SearchIndexModel indexOne = new SearchIndexModel("testIndex01",
					new Document("mappings",
							new Document("dynamic", true)));

			SearchIndexModel indexTwo = new SearchIndexModel("testIndex02",
					new Document("mappings",
							new Document("dynamic", true).append("fields",
									new Document().append("genres",
											Arrays.asList(
													new Document().append("type", "stringFacet"),
													new Document().append("type", "string")
											)
									).append("year",
											Arrays.asList(
													new Document().append("type", "numberFacet"),
													new Document().append("type", "number")
											)
									))));

			//Creating Atlas Search Index to understand the concepts of Autocomplete

			SearchIndexModel indexThree = new SearchIndexModel("testIndex03",
					new Document("mappings",
							new Document("dynamic", false).append("fields",
									new Document().append("fullplot",
											Arrays.asList(
													new Document().append("type", "stringFacet"),
													new Document().append("type", "string"),
													new Document().append("type", "autocomplete")
													.append("tokenization", "nGram")
													.append("minGrams", 3)
													.append("maxGrams", 7)
													.append("foldDiacritics", false)
											))
											.append("title", new Document().append("type", "string")))
					));

			SearchIndexModel indexFlour = new SearchIndexModel("testIndex04",
					new Document("mappings", new Document()
					.append("dynamic", true)
					.append("fields", new Document()
							.append("fullplot", new Document()
									.append("analyzer", "lucene.english")
									.append("type", "string")
							)
					)
			)
					.append("synonyms", List.of(new Document()
							.append("analyzer", "lucene.english")
							.append("name", "synonymName")
							.append("source", new Document()
									.append("collection", "test_synonyms")
							)
					)));

			collection.createSearchIndexes(Arrays.asList( indexFlour));
            System.out.println("Wait for some time for indexes to be created. ");
			// Wait for 3 mins to let Atlas Create the indexes.
			Thread.sleep(300000);

			MovieAtlasSearchService searchService = new MovieAtlasSearchService(new MongoTemplate(mongoClient, "sample_mflix"));

			// Call your search methods
			ArrayList<Document> searchResults = searchService.searchMovies("space cowboy");
			Collection<Document> late90sMovies = searchService.late90sMovies("hacker assassin");

			Collection<Document> late90sMoviesWithScore = searchService.late90sMovies("hacker assassin", SearchScore.boost(fieldPath("imdb.votes")));
			Document late90sMoviesCount = searchService.countLate90sMovies("hacker assassin");
			List<Document> genresThroughTheDecadesResult = searchService.genresThroughTheDecades("horror");
			Collection<Document> late90sMovies2 = searchService.late90sMovies2(1,10,"hacker assassin");

			ArrayList<Document> searchResultsAutocomplete = searchService.searchMoviesWithIncompleteKeyword("spa cowb");
			ArrayList<Document> searchWithMisSpelledTitle = searchService.searchWithMisspelledTitle("The Great Trin Robry");

			ArrayList<Document> searchAllMoviesThatMentionsAnyCountry = searchService.searchWithSynonyms("country");




			// Process the results as needed
			System.out.println("Search results: " + searchResults);
			System.out.println("Late 90s movies: " + late90sMovies);

			System.out.println("Late 90s movies with score: " + late90sMoviesWithScore);
			System.out.println("Late 90s movies count: " + late90sMoviesCount);

			System.out.println("Get horror movies in last decade " + genresThroughTheDecadesResult);
			System.out.println("Late 90s movies with pagination: " + late90sMovies2 );

			System.out.println("Search results where plot has words like spa cowb " + searchResultsAutocomplete);
			System.out.println("Search results when the title is misspelled " + searchWithMisSpelledTitle);

			System.out.println("Search results when the full plot mentions any country " + searchAllMoviesThatMentionsAnyCountry );


			//Dropping index after execution.
			collection.dropSearchIndex("testIndex01");
			collection.dropSearchIndex("testIndex02");
			collection.dropSearchIndex("testIndex03");
			collection.dropSearchIndex("testIndex04");

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
