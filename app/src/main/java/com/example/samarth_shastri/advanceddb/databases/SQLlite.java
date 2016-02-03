package com.example.samarth_shastri.advanceddb.databases;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;
import android.os.Debug;
import android.util.Log;

import com.example.samarth_shastri.accessories.sqllitehelper;
import com.example.samarth_shastri.accessories.abstractDb;

import java.util.ArrayList;
import java.util.List;

public class SQLlite extends abstractDb {
    protected SQLiteDatabase db;
    private sqllitehelper helper;
    private long numberOfIterations;
    protected final String SIMPLE_QUERY = "SimplePointQuery";
    protected final String SIMPLE_WRITE = "SimpleWrite";
    protected final String BATCH_WRITE = "BatchWrite";
    protected final String SUM = "Sum";
    protected final String COUNT = "Count";
    protected final String FULL_SCAN = "FullScan";
    public SQLlite(Context context, long numberOfObjects, long numberOfIterations) {
        super(context, numberOfObjects);
        this.numberOfIterations = numberOfIterations;
    }




    public void simpleQuery() {
        Log.i("DataStoreBenchmark", "SQllite invoking simple query");
        intitalize();
         List<Long> values;
         long timeStart;
        values = new ArrayList<>();



                addTable();
                addRows();
                check();




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                Cursor cursor = db.rawQuery(
                        String.format("SELECT * FROM %s WHERE csci = 0 AND points BETWEEN 80 AND 100 AND name = 'student0'",
                                helper.TABLE_NAME), null);
                cursor.moveToFirst();
                while (!cursor.isAfterLast()) {
                    int i = cursor.getInt(0);
                    cursor.moveToNext();
                }
                cursor.close();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }


            deleteRows();

        finalvalues.put(SIMPLE_QUERY, values);

        garbaceColl();
    }


    public void batchWrite() {
        Log.i("DataStoreBenchmark", "SQllite invoking batch write query");
        intitalize();
        List<Long> values;
        long timeStart;
        values = new ArrayList<>();


                addTable();




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();

                addRows();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }



        deleteRows();

        finalvalues.put(BATCH_WRITE, values);

        garbaceColl();
    }


    public void sum() {
        Log.i("DataStoreBenchmark", "SQllite invoking sum");
        intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();

                addTable();
                addRows();
                check();




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                Cursor cursor = db.rawQuery(String.format("SELECT SUM(points) AS sum FROM %s", helper.TABLE_NAME), null);
                cursor.moveToFirst();
                int sum = cursor.getInt(0);
                cursor.close();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }

            deleteRows();

        finalvalues.put(SUM, values);

        garbaceColl();
    }


    public void count() {
        Log.i("DataStoreBenchmark", "SQllite invoking count");
        intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();

                addTable();
                addRows();
                check();



        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
            Cursor cursor = db.rawQuery(String.format("SELECT COUNT(*) AS count FROM %s", helper.TABLE_NAME), null);
            cursor.moveToFirst();
            int count = cursor.getInt(0);
            cursor.close();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }

            deleteRows();


        finalvalues.put(COUNT, values);

        garbaceColl();
    }


    public void fullScan() {
        Log.i("DataStoreBenchmark", "SQllite invoking full scan");
        intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();

                addTable();
                addRows();
                check();




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                Cursor cursor = db.rawQuery("SELECT * FROM " + helper.TABLE_NAME
                        + " WHERE csci = 1 AND points BETWEEN 110 AND 200 AND name = 'Smile1'", null);
                int count = cursor.getCount();
                cursor.close();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
            }

            deleteRows();


        finalvalues.put(FULL_SCAN, values);

        garbaceColl();
    }


    public void write() {
        Log.i("DataStoreBenchmark", "SQllite invoking write");
        intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
           SQLiteStatement stmt;
           int i;


                addTable();
                stmt = db.compileStatement("INSERT INTO " + helper.TABLE_NAME + " VALUES(?1, ?2, ?3, ?4)");
                i = 0;




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                stmt.clearBindings();
                stmt.bindLong(1, i);
                stmt.bindString(2, dataGenerator.getStudentName(i));
                stmt.bindLong(3, dataGenerator.getStudentPoints(i));
                db.beginTransaction();
                stmt.executeInsert();
                db.setTransactionSuccessful();
                db.endTransaction();
                i++;
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }



            deleteRows();


        finalvalues.put(SIMPLE_WRITE, values);

        garbaceColl();
    }
    private void addTable() {
        db.execSQL("DROP TABLE IF EXISTS " + helper.TABLE_NAME);
        db.execSQL("CREATE TABLE " + helper.TABLE_NAME + " ("
                + "id INTEGER, "
                + "name TEXT,"
                + "points INTEGER,"
                + "csci INTEGER"
                + ")");
    }

    private void addRows() {
        SQLiteStatement stmt = db.compileStatement("INSERT INTO " + helper.TABLE_NAME + " VALUES(?1, ?2, ?3, ?4)");
        db.beginTransaction();
        for (int i = 0; i < numberOfObjects; i++) {
            stmt.clearBindings();
            stmt.bindLong(1, i);
            stmt.bindString(2, dataGenerator.getStudentName(i));
            stmt.bindLong(3, dataGenerator.getStudentPoints(i));
            stmt.bindLong(4, dataGenerator.getCsciRegAsInt(i));
            stmt.executeInsert();
        }
        db.setTransactionSuccessful();
        db.endTransaction();
    }

    private void check() {
        Cursor cursor = db.rawQuery(String.format("SELECT COUNT(*) AS count FROM %s", helper.TABLE_NAME), null);
        cursor.moveToFirst();
        int count = cursor.getInt(0);
        cursor.close();
        if (count != numberOfObjects) {
            throw new RuntimeException(String.format("Number of row is %d - %d expected.", count, numberOfObjects));
        }
    }

    private void deleteRows() {
        db.beginTransaction();
        db.execSQL(String.format("DELETE FROM %s", helper.TABLE_NAME));
        db.setTransactionSuccessful();
        db.endTransaction();
    }

    protected void intitalize() {
       // super.intitalize();
        helper = new sqllitehelper(context);
        db = helper.getWritableDatabase();
    }

    protected void garbaceColl() {
        db.close();
        helper.close();
        super.garbaceColl();
    }
}
