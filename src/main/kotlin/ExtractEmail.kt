import java.io.File
import java.sql.DriverManager
import java.util.regex.Pattern
import java.util.logging.Level
import java.util.logging.Logger

fun extractUniqueEmails(inputFilePath: String, outputFilePath: String) {
    val emailRegex = "[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}"
    val pattern = Pattern.compile(emailRegex)

    // Create or open SQLite database
    val dbUrl = "jdbc:sqlite:emails.db"
    DriverManager.getConnection(dbUrl).use { connection ->
        connection.createStatement().use { statement ->
            // Create table to store unique emails
            statement.execute("CREATE TABLE IF NOT EXISTS emails (email TEXT PRIMARY KEY)")
        }

        // Insert emails into the database
        val insertQuery = "INSERT OR IGNORE INTO emails (email) VALUES (?)"
        connection.prepareStatement(insertQuery).use { preparedStatement ->
            var linesProcessed = 0
            File(inputFilePath).bufferedReader().useLines { lines ->
                lines.forEach { line ->
                    val matcher = pattern.matcher(line)
                    while (matcher.find()) {
                        val email = matcher.group()
                        preparedStatement.setString(1, email)
                        preparedStatement.addBatch()
                    }
                    linesProcessed++
                    if (linesProcessed % 1000 == 0) {
                        preparedStatement.executeBatch()
                    }
                }
                // Execute remaining batch if any
                preparedStatement.executeBatch()
            }
        }

        // Write unique emails to the output file
        val outputFile = File(outputFilePath)
        outputFile.printWriter().use { writer ->
            val pageSize = 1000
            var offset = 0
            var hasMore = true
            var batchNumber = 0

            while (hasMore) {
                connection.prepareStatement("SELECT email FROM emails LIMIT ? OFFSET ?").use { selectStatement ->
                    selectStatement.setInt(1, pageSize)
                    selectStatement.setInt(2, offset)
                    val resultSet = selectStatement.executeQuery()
                    hasMore = resultSet.next() // Check if there are more records
                    while (resultSet.next()) {
                        writer.println(resultSet.getString("email"))
                    }
                }
                offset += pageSize
                batchNumber++
            }
        }
    }
}

fun main() {
    val inputFilePath = "file/path/input.txt"
    val outputFilePath = "file/path/output.txt"

    extractUniqueEmails(inputFilePath, outputFilePath)
}
