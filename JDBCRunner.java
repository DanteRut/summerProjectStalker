package main.java;

import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "stalker";          // FIXME имя базы


    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных
    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getFactions(connection);System.out.println();
            getLocations(connection);System.out.println();
            getPersons(connection);System.out.println();

            // TODO show with param
            getPersonsInLocation(connection, 2);System.out.println();
            getPersonsInLocationWhereMoreMoneyLimit(connection, 2, 10000, 3);System.out.println();

            // TODO show with join
            getLocationsWhereFaction(connection, 1);System.out.println();
            getPersonsInLocationWhereMoreMoney(connection, 2, 10000);System.out.println();

            // TODO correction
            deleteLocation(connection, "Градирни");
            addLocation(connection, "Градирни", 1);
            addPerson(connection, "Шрам", 10900, 6);
            correctPersonLocation(connection, "Шрам", 5);
            deletePerson(connection, "Шрам");

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }


    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }


    private static void getFactions(Connection connection) throws SQLException {
        String columnName0 = "faction_id", columnName1 = "faction_name", columnName2 = "enemies_id", columnName3 = "friends_id";
        int param0 = -1;
        String param1 = null, param2 = null, param3 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM factions ORDER BY faction_id;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }


    static void getLocations(Connection connection) throws SQLException {
        // значения ячеек
        int param0 = -1, param2 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM locations ORDER BY location_id;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }
    static void getPersons(Connection connection) throws SQLException {

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT name, money, health_point, location_id FROM persons;");

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getInt(2) + " | " + rs.getString(3)+ " | " + rs.getInt(4));
        }
    }
    static void getLocationsWhereFaction(Connection connection, int factionId) throws SQLException {
        // значения ячеек
        if (factionId<0) return;

        PreparedStatement statement = connection.prepareStatement("SELECT locations.location_name, factions.faction_name " +
                "FROM locations JOIN factions ON locations.faction_id = factions.faction_id WHERE locations.faction_id = ?;");
        statement.setInt(1, factionId);

        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2));
        }
    }
    private static void getPersonsInLocationWhereMoreMoneyLimit(Connection connection,int locationId, int money, int limit) throws SQLException {
        if (money < 0 || locationId < 0) return;

        PreparedStatement statement = connection.prepareStatement("SELECT persons.name, persons.money, locations.location_name " +
                "FROM persons JOIN locations ON persons.location_id = locations.location_id WHERE persons.location_id = ? AND persons.money > ? ORDER BY money DESC LIMIT ?;");
        statement.setInt(1, locationId);
        statement.setInt(2, money);
        statement.setInt(3, limit);

        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getInt(2) + " | " + rs.getString(3));
        }
    }
    private static void getPersonsInLocationWhereMoreMoney(Connection connection,int locationId, int money) throws SQLException {
        if (money < 0 || locationId < 0) return;

        PreparedStatement statement = connection.prepareStatement("SELECT persons.name, persons.money, locations.location_name " +
                "FROM persons JOIN locations ON persons.location_id = locations.location_id WHERE persons.location_id = ? AND persons.money > ?;");
        statement.setInt(1, locationId);
        statement.setInt(2, money);

        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getInt(2) + " | " + rs.getString(3));
        }
    }
    static void getPersonsInLocation(Connection connection,int locationId) throws SQLException {
        if (locationId < 0) return;

        PreparedStatement statement = connection.prepareStatement("SELECT name, money FROM persons WHERE location_id = ?;");
        statement.setInt(1, locationId);

        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getInt(2));
        }
    }

    private static void addPerson(Connection connection, String name, int money, int factionId) throws SQLException {
        if (name == null || name.isBlank() || money < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO persons(name, money, faction_id) VALUES (?, ?, ?);");
        statement.setString(1, name);
        statement.setInt(2, money);
        statement.setInt(3, factionId);

        statement.executeUpdate();
    }

    private static void correctPersonLocation(Connection connection, String name, int locationId) throws SQLException {
        if (name == null || name.isBlank() || locationId < 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE persons SET location_id=? WHERE name=?;");
        statement.setInt(1, locationId); // сначала что передаем
        statement.setString(2, name);   // затем по чему ищем

        statement.executeUpdate();
    }

    private static void deletePerson(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("DELETE FROM persons WHERE name = ?");
        statement.setString(1, name);
        statement.executeUpdate();
    }

    private static void addLocation(Connection connection, String name, int factionId) throws SQLException {
        if (name == null || name.isBlank() || factionId < 0) return;

        PreparedStatement statement = connection.prepareStatement("INSERT INTO locations(location_name, faction_id) VALUES (?, ?);");
        statement.setString(1, name);
        statement.setInt(2, factionId);
        statement.executeUpdate();
    }

    private static void deleteLocation(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("DELETE FROM locations WHERE location_name = ?");
        statement.setString(1, name);
        statement.executeUpdate();
    }
}