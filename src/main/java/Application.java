import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;

public class Application {
  private static final String url = "jdbc:postgresql://localhost:5432/postgres";
  private static final String csvFile = "src/main/resources/students/studentsScore.csv";
  private static final Logger logger = Logger.getLogger(Application.class.getName());

  public static void main(String[] args) {

    ExecutorService executorService = Executors.newFixedThreadPool(5);

    try{
      Connection conn = DriverManager.getConnection(url, "postgres", "postgres");
      logger.info("Connection to db successful");

      Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
        .setHeader(studentsCSVHeaders.class).build().parse(new FileReader(csvFile));

      logger.info("CSV file loaded");

      records.iterator().next();

      for(CSVRecord record : records){
        Runnable task = () -> processStudentRecord(record, conn);
        executorService.execute(task);
      }

    } catch(SQLException | IOException e){
      logger.severe(format("Error -> %s", e.getMessage()));
    } finally {
      executorService.shutdown();
    }

    try{
      if(!executorService.awaitTermination(5, TimeUnit.SECONDS)){
        executorService.shutdown();
      }
    } catch (InterruptedException e){
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }

    logger.info("Program finished");

  }

  public static void processStudentRecord(CSVRecord record, Connection conn){
    String name = record.get(studentsCSVHeaders.Name);
    String lastName = record.get(studentsCSVHeaders.LastName);
    int age = Integer.parseInt(record.get(studentsCSVHeaders.Age));
    double grade = Double.parseDouble(record.get(studentsCSVHeaders.Grade));
    double moneySpend = Double.parseDouble(record.get(studentsCSVHeaders.MoneySpend));

    try{
      String query = "INSERT INTO students (name, lastname, age, grade, money_spend) VALUES (?, ?, ?, ?, ?)";
      PreparedStatement preparedStatement = conn.prepareStatement(query);
      preparedStatement.setString(1, name);
      preparedStatement.setString(2, lastName);
      preparedStatement.setInt(3, age);
      preparedStatement.setDouble(4, grade);
      preparedStatement.setDouble(5, moneySpend);
      preparedStatement.executeUpdate();
    } catch (SQLException e){
      logger.severe(format("SQL Error -> %s", e.getMessage()));
    }
  }

  public enum studentsCSVHeaders {
    id,Name,LastName,Age,Grade,MoneySpend
  }
}
