import java.sql.SQLException;
import java.util.List;

public interface DatabaseService {
    void insertData(List<String[]> data) throws SQLException;
}

