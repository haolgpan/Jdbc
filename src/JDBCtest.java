import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class JDBCtest {
    static List<List<String>> spices = new ArrayList<List<String>>();

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        String dbURL = "jdbc:postgresql://192.168.1.52/testddbb";
        Connection conn = DriverManager.getConnection(dbURL, "usuario", "password");
        ReadCsv();
        String insInterCuis = "INSERT INTO interCuisine(nameSpices, geozone) VALUES(?,?)";
        PreparedStatement intCui = conn.prepareStatement(insInterCuis);
        for (int i = 1; i < spices.size(); i++) {
            intCui.setString(1,spices.get(i).get(0));
            intCui.setString(2,spices.get(i).get(6));
            intCui.execute();
        }
        //Borrar todas las tablas
        /*String dropTablesSql = "DO $$ DECLARE " +
                "    r RECORD;" +
                "BEGIN " +
                "    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';" +
                "    END LOOP;" +
                "END $$;";
        Statement delete = conn.createStatement();
        delete.executeUpdate(dropTablesSql);*/
    }

    public static void ReadCsv() throws IOException {
        try (CSVReader csvReader = new CSVReader(new FileReader("resources/spices.csv"))) {
            String[] values = null;
            while ((values = csvReader.readNext()) != null) {
                spices.add(Arrays.asList(values));
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }

    }
    public void createAlltables(Connection conn){
        //tabla general
        try {
            String createTableSql = "CREATE TABLE spices"
                    + "("
                    + "Name varchar(100) primary key,"
                    + "Introduction text,"
                    + "Description text,"
                    + "Ingredients text,"
                    + "Basic_Preparation text,"
                    + "Recommended_Applications text,"
                    + "Cuisine text,"
                    + "Product_Style varchar(30),"
                    + "Botanical_Name varchar(50),"
                    + "Fold varchar(20),"
                    + "Notes varchar(150),"
                    + "Shell_Life text,"
                    + "Bottle_Style text,"
                    + "Capacity_Volume text,"
                    + "Dimensions text,"
                    + "Cap text,"
                    + "Caffeine_Free text,"
                    + "Scoville_Heat_Scale text,"
                    + "Handling_Storage text,"
                    + "Country_Of_Origin text,"
                    + "Dietary_Preferences text,"
                    + "Allergen_Information text,"
                    + "Page_Link text"
                    + ")";
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(createTableSql);
            //Tabla product Style
            Statement st = conn.createStatement();
            String createTableProduct = "CREATE TABLE productStyle (format varchar(30) primary key)";
            st.executeUpdate(createTableProduct);
            //Intermediate table product Style
            String createTableIntProduct = "CREATE TABLE interProduct (nameSpices varchar(100) REFERENCES " +
                    "spices(Name) ON UPDATE CASCADE ON DELETE CASCADE, format varchar(30) REFERENCES " +
                    "productStyle(format) ON UPDATE CASCADE ON DELETE CASCADE, CONSTRAINT name_format_pkey" +
                    " PRIMARY KEY(nameSpices, format))";
            Statement intermProd = conn.createStatement();
            intermProd.executeUpdate(createTableIntProduct);
            //Table country origin of spices
            Statement countryst = conn.createStatement();
            String createTableCountry = "CREATE TABLE countryOrigin (country text primary key)";
            countryst.executeUpdate(createTableCountry);
            //Intermediate table country origin and spices
            String createTableIntCountry = "CREATE TABLE interCountry (nameSpices varchar(100) REFERENCES " +
                    "spices(Name) ON UPDATE CASCADE ON DELETE CASCADE, country text REFERENCES " +
                    "countryOrigin(country) ON UPDATE CASCADE ON DELETE CASCADE, CONSTRAINT name_country_pkey" +
                    " PRIMARY KEY(nameSpices, country))";
            Statement intermCount = conn.createStatement();
            intermCount.executeUpdate(createTableIntCountry);
            //Table of cuisine
            Statement cuisinest = conn.createStatement();
            String createTableCuisine = "CREATE TABLE cuisine (geozone text primary key)";
            cuisinest.executeUpdate(createTableCuisine);
            //Intermediate table of cuisine and spices
            String createTableIntCuisine = "CREATE TABLE interCuisine (nameSpices varchar(100) REFERENCES " +
                    "spices(Name) ON UPDATE CASCADE ON DELETE CASCADE, geozone text REFERENCES " +
                    "cuisine(geozone) ON UPDATE CASCADE ON DELETE CASCADE, CONSTRAINT name_cuisine_pkey" +
                    " PRIMARY KEY(nameSpices, geozone))";
            Statement intermCuis = conn.createStatement();
            intermCuis.executeUpdate(createTableIntCuisine);
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("Error en crear les taules");
        }
    }
    public void fillAllDataSpices(Connection conn){
        try {
            //Inserts de la tabla general
            String inserts = "INSERT INTO spices"
                    + "(Name, Introduction, Description, Ingredients, Basic_Preparation, Recommended_Applications, Cuisine, Product_Style, Botanical_Name, Fold, Notes, Shell_Life, Bottle_Style, Capacity_Volume, Dimensions, Cap, Caffeine_Free, Scoville_Heat_Scale, Handling_Storage, Country_Of_Origin, Dietary_Preferences, Allergen_Information, Page_Link)"
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement statement = conn.prepareStatement(inserts);
            for (int i = 1; i < spices.size(); i++) {
                statement.setString(1, spices.get(i).get(0));
                statement.setString(2, spices.get(i).get(1));
                statement.setString(3, spices.get(i).get(2));
                statement.setString(4, spices.get(i).get(3));
                statement.setString(5, spices.get(i).get(4));
                statement.setString(6, spices.get(i).get(5));
                statement.setString(7, spices.get(i).get(6));
                statement.setString(8, spices.get(i).get(7));
                statement.setString(9, spices.get(i).get(8));
                statement.setString(10, spices.get(i).get(9));
                statement.setString(11, spices.get(i).get(10));
                statement.setString(12, spices.get(i).get(11));
                statement.setString(13, spices.get(i).get(12));
                statement.setString(14, spices.get(i).get(13));
                statement.setString(15, spices.get(i).get(14));
                statement.setString(16, spices.get(i).get(15));
                statement.setString(17, spices.get(i).get(16));
                statement.setString(18, spices.get(i).get(17));
                statement.setString(19, spices.get(i).get(18));
                statement.setString(20, spices.get(i).get(19));
                statement.setString(21, spices.get(i).get(20));
                statement.setString(22, spices.get(i).get(21));
                statement.setString(23, spices.get(i).get(22));
                statement.execute();
            }
            //Inserts de product Style
            String insertProd = "INSERT INTO productStyle (format) VALUES (?) ON CONFLICT DO NOTHING";
            PreparedStatement prod = conn.prepareStatement(insertProd);
            for (int i = 1; i < spices.size(); i++) {
                prod.setString(1, spices.get(i).get(7));
                prod.executeUpdate();
            }
            //Inserts Intermediate product Style
            String insInterprod = "INSERT INTO interProduct(nameSpices, format) VALUES(?,?)";
            PreparedStatement intPr = conn.prepareStatement(insInterprod);
            for (int i = 1; i < spices.size(); i++) {
                intPr.setString(1,spices.get(i).get(0));
                intPr.setString(2,spices.get(i).get(7));
                intPr.execute();
            }
            //Insert table country origin
            String insertCountry = "INSERT INTO countryOrigin (country) VALUES (?) ON CONFLICT DO NOTHING";
            PreparedStatement countr = conn.prepareStatement(insertCountry);
            for (int i = 1; i < spices.size(); i++) {
                countr.setString(1, spices.get(i).get(19));
                countr.executeUpdate();
            }
            //Inserts intermediate table country origin
            String insInterCount = "INSERT INTO interCountry(nameSpices, country) VALUES(?,?)";
            PreparedStatement intCo = conn.prepareStatement(insInterCount);
            for (int i = 1; i < spices.size(); i++) {
                intCo.setString(1,spices.get(i).get(0));
                intCo.setString(2,spices.get(i).get(19));
                intCo.execute();
            }
            //Insert table cuisine
            String insertCuisine = "INSERT INTO cuisine (geozone) VALUES (?) ON CONFLICT DO NOTHING";
            PreparedStatement cuis = conn.prepareStatement(insertCuisine);
            for (int i = 1; i < spices.size(); i++) {
                cuis.setString(1, spices.get(i).get(6));
                cuis.executeUpdate();
            }
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("Error en carregar les dades");
        }
    }
}
