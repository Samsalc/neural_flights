public class vuelos {
    public static void main(String[] args) {
        String csvFilePath = "Airports2.csv";
        String dbUrl = System.getenv("MYSQL_DB_URL");
        String dbUser = System.getenv("MYSQL_DB_USER");
        String dbPassword = System.getenv("MYSQL_DB_PASSWORD");

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
