import com.opencsv.CSVReader;
import java.math.BigDecimal;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class CSVReaderService {
    private static final int BATCH_SIZE = 1000; // Tamaño del lote para insertar datos

    public void readCSVAndInsert(String csvFilePath, DatabaseService databaseService) throws Exception {
        List<String[]> dataBatch = new ArrayList<>(); // Crear un lote de datos
        try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath))) {
            String[] header = csvReader.readNext(); // Leer el encabezado
            String[] row;

            while ((row = csvReader.readNext()) != null) {
                if (row.length != 15) {
                    System.err.println("Fila con número de columnas incorrecto: " + java.util.Arrays.toString(row));
                    continue;  // Ignorar filas con columnas incorrectas
                }

                // Validar y manejar valores nulos
                try {
                    // Validar cada columna numérica
                    int passengers = Integer.parseInt(row[4]);
                    int seats = Integer.parseInt(row[5]);
                    int flights = Integer.parseInt(row[6]);
                    int distance = Integer.parseInt(row[7]);
                    int originPopulation = Integer.parseInt(row[9]);
                    BigDecimal orgAirportLat = parseBigDecimal(row[11]);
                    BigDecimal orgAirportLong = parseBigDecimal(row[12]);
                    BigDecimal destAirportLat = parseBigDecimal(row[13]);
                    BigDecimal destAirportLong = parseBigDecimal(row[14]);

                    dataBatch.add(row); // Agregar cada fila válida al lote

                    // Insertar en la base de datos si se alcanza el tamaño del lote
                    if (dataBatch.size() >= BATCH_SIZE) {
                        databaseService.insertData(dataBatch);
                        dataBatch.clear(); // Limpiar el lote
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Error al convertir fila: " + java.util.Arrays.toString(row) + " - " + e.getMessage());
                }
            }
            // Insertar cualquier dato restante
            if (!dataBatch.isEmpty()) {
                databaseService.insertData(dataBatch);
            }
        }
    }

    private BigDecimal parseBigDecimal(String value) {
        // Manejar "NA" o valores no válidos
        if (value.equals("NA") || value.isEmpty()) {
            return BigDecimal.ZERO; // O un valor por defecto según el tipo de dato
        }
        return new BigDecimal(value);
    }
}
