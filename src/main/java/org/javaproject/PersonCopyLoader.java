package org.javaproject;

import com.github.javafaker.Faker;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

public class PersonCopyLoader {

    
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/java_faker";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "123";
    private static final long TOTAL_RECORDS = 10_000_000L;

    
    private static final int PIPE_BUFFER = 64 * 1024; // 64 KB
    private static final int WRITE_FLUSH_INTERVAL = 10_000; // flush every N rows

    private static final SimpleDateFormat DOB_FMT = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        
        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, PIPE_BUFFER);
        Thread producer = new Thread(() -> {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(pos, StandardCharsets.UTF_8), 64 * 1024)) {
                Faker faker = new Faker(new Locale("en"));
                Random rnd = new Random();

                for (long i = 0; i < TOTAL_RECORDS; i++) {
                    
                    String first = faker.name().firstName();
                    String last = faker.name().lastName();
                    Date dobDate = faker.date().birthday(18, 90);
                    String dob = DOB_FMT.format(dobDate);

                    String email = sanitizeEmail(faker.internet().emailAddress(first.toLowerCase() + "." + last.toLowerCase()));

                    
                    String line = escapeCsv(first) + ',' + escapeCsv(last) + ',' + dob + ',' + escapeCsv(email);

                    writer.write(line);
                    writer.newLine();

                    if ((i + 1) % WRITE_FLUSH_INTERVAL == 0) {
                        writer.flush();
                    }
                }
                writer.flush();
            } catch (IOException e) {
                
                e.printStackTrace();
            } finally {
                try {
                    pos.close();
                } catch (IOException ignored) {}
            }
        }, "csv-producer");

        producer.start();
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS)) {
            
            PGConnection pgConnection = conn.unwrap(PGConnection.class);
            CopyManager copyManager = pgConnection.getCopyAPI();

            String copySql = "COPY person(firstname,lastname,dob,email) FROM STDIN WITH (FORMAT csv)";

            long rowsCopied = copyManager.copyIn(copySql, pis);

            long elapsed = System.currentTimeMillis() - start;
            System.out.printf("COPY completed, rowsCopied=%d, elapsed_ms=%d%n", rowsCopied, elapsed);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            throw sqle;
        } finally {
            
            producer.join();
            pis.close();
        }
    }
    private static String escapeCsv(String value) {
        if (value == null) return "";
        boolean needQuote = value.indexOf(',') >= 0 || value.indexOf('"') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0;
        if (!needQuote) return value;
        String escaped = value.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    
    private static String sanitizeEmail(String email) {
        if (email == null) return "";
        return email.replaceAll("[\\n\\r,]", "_");
    }
}
