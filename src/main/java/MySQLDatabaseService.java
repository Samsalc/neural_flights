import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.math.BigDecimal;

public class MySQLDatabaseService implements DatabaseService {
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private static final int BATCH_SIZE = 500;

    public MySQLDatabaseService(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    @Override
    public void insertData(List<String[]> data) throws SQLException {
        String insertQuery = """
        
                INSERT INTO Vuelos (
            Origin_airport, 
            Destination_airport, 
            Origin_city, 
            Destination_city, 
            Passengers, 
            Seats, 
            Flights, 
            Distance, 
            Fly_date, 
            Origin_population, 
            Destination_population, 
            Org_airport_lat, 
            Org_airport_long, 
            Dest_airport_lat, 
            Dest_airport_long
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {

            int count = 0;

            for (String[] row : data) {
                try {
                    pstmt.setString(1, row[0]);  // Origin_airport
                    pstmt.setString(2, row[1]);  // Destination_airport
                    pstmt.setString(3, row[2]);  // Origin_city
                    pstmt.setString(4, row[3]);  // Destination_city
                    pstmt.setInt(5, Integer.parseInt(row[4]));  // Passengers
                    pstmt.setInt(6, Integer.parseInt(row[5]));  // Seats
                    pstmt.setInt(7, Integer.parseInt(row[6]));  // Flights
                    pstmt.setInt(8, Integer.parseInt(row[7]));  // Distance
                    pstmt.setDate(9, java.sql.Date.valueOf(row[8]));  // Fly_date
                    pstmt.setInt(10, Integer.parseInt(row[9]));  // Origin_population
                    pstmt.setInt(11, Integer.parseInt(row[10]));  // Destination_population

                    // Manejo de BigDecimal con validación
                    pstmt.setBigDecimal(12, validateBigDecimal(row[11]));  // Org_airport_lat
                    pstmt.setBigDecimal(13, validateBigDecimal(row[12]));  // Org_airport_long
                    pstmt.setBigDecimal(14, validateBigDecimal(row[13]));  // Dest_airport_lat
                    pstmt.setBigDecimal(15, validateBigDecimal(row[14]));  // Dest_airport_long

                    pstmt.addBatch();  // Agregar a lote

                    if (++count % BATCH_SIZE == 0) {
                        pstmt.executeBatch();  // Ejecutar el lote
                        System.out.println("Insertando " + count + " filas...");
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Error al convertir fila: " + Arrays.toString(row) + " - " + e.getMessage());
                }
            }
            pstmt.executeBatch();  // Ejecutar cualquier fila restante
            System.out.println("Datos insertados correctamente.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private BigDecimal validateBigDecimal(String value) {
        if (value == null || value.isEmpty() || value.equals("N")) {
            return BigDecimal.ZERO; // O el valor que prefieras para manejar datos faltantes
        }
        return new BigDecimal(value);
    }
}