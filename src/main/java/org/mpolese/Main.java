package org.mpolese;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;


import java.io.*;
import java.net.URL;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private final Driver driver;

    public Main(String uri, String user, String password, Config config) {
        // The driver is a long living object and should be opened during the start of your application
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), config);
    }

    @Override
    public void close() {
        // The driver object should be closed before the application ends.
        driver.close();
    }

    public BufferedReader openCSVFromRessource(String path) {
        InputStream is = Main.class.getClassLoader().getResourceAsStream(path);
        return new BufferedReader(new InputStreamReader(is));
    }

    public void importMovies() throws IOException {
        var br = this.openCSVFromRessource("ml-latest-small/movies.csv");

        try (var session = driver.session(SessionConfig.forDatabase("neo4j"))) {
            session.run("CREATE CONSTRAINT UNIQUE_movie FOR (m:Movie) REQUIRE m.movieId IS UNIQUE");
            session.run("CREATE CONSTRAINT UNIQUE_genre FOR (g:Genre) REQUIRE g.genre IS UNIQUE");

            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                int movieId = Integer.parseInt(values[0]);
                String title = values[1];
                String[] genres = values[2].split("\\|");

                // Create the Movie node
                session.run("CREATE (:Movie {movieId: $movieId, title: $title})",
                        Values.parameters("movieId", movieId, "title", title));
                LOGGER.log(Level.INFO, "id: " + movieId + " - title: " + title);

                for (String genre : genres) {
                    // Create the Genre nodes and relationships
                    LOGGER.log(Level.INFO, "==> movie: " + title + " - genre: " + genre);
                    session.run("MERGE (g:Genre {name: $genre}) " +
                                    "WITH g " +
                                    "MATCH (m:Movie {movieId: $movieId}) " +
                                    "CREATE (m)-[:HAS]->(g)",
                            Values.parameters("genre", genre, "movieId", movieId));
                }
            }
        } catch (Neo4jException ex) {
            LOGGER.log(Level.SEVERE, " raised an exception", ex);
            throw ex;
        }
    }

    public void importRatings() throws IOException {
        var br = this.openCSVFromRessource("ml-latest-small/ratings.csv");

        try (var session = driver.session(SessionConfig.forDatabase("neo4j"))) {
            session.run("CREATE CONSTRAINT UNIQUE_userId FOR (u:User) REQUIRE u.userId IS UNIQUE");
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                int userId = Integer.parseInt(values[0]);
                int movieId = Integer.parseInt(values[1]);
                float rating = Float.parseFloat(values[2]);
                long timestamp = Long.parseLong(values[3]);
                // Create the User node
                LOGGER.log(Level.INFO, "userId: " + userId + " - movieId: " + movieId + " - rating: " + rating + " - timestamp: " + timestamp);

                session.run("MERGE (u:User {userId: $userId}) " +
                                "WITH u " +
                                "MATCH (m:Movie {movieId: $movieId}) " +
                                "CREATE (u)-[r:RATED {rating: $rating, timestamp: $timestamp}]->(m)",
                        Values.parameters("userId", userId, "movieId", movieId, "rating", rating, "timestamp", timestamp));
            }
        } catch (Neo4jException ex) {
            LOGGER.log(Level.SEVERE, " raised an exception", ex);
            throw ex;
        }
        driver.close();
    }

    public static void main(String... args) {
        var uri = "neo4j://localhost:7687";
        var user = "neo4j";
        var password = "...";

        try (var app = new Main(uri, user, password, Config.defaultConfig())) {
            app.importMovies();
            app.importRatings();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}