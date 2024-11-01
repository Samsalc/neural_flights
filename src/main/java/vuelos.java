public class vuelos {
    public static void main(String[] args) {
        String csvFilePath = "Airports2.csv";
        String dbUrl = "jdbc:mysql://localhost:3306/airport";
        String dbUser = "root";
        String dbPassword = "Samuelsal12";

        DatabaseService databaseService = new MySQLDatabaseService(dbUrl, dbUser, dbPassword);
        CSVReaderService csvReaderService = new CSVReaderService();

        try {
            csvReaderService.readCSVAndInsert(csvFilePath, databaseService);
            System.out.println("Datos insertados correctamente.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
