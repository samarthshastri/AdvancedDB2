package com.example.samarth_shastri.advanceddb.databases;

import android.content.Context;
import android.os.Debug;
import android.util.Log;

import com.example.samarth_shastri.accessories.SugarOrmStudent;
import com.example.samarth_shastri.accessories.abstractDb;
import com.orm.query.Condition;
import com.orm.query.Select;

import java.util.ArrayList;
import java.util.List;


public class SugarOrm extends abstractDb {

    private long numberOfIterations;
    protected final String SIMPLE_QUERY = "SimplePointQuery";
    protected final String SIMPLE_WRITE = "SimpleWrite";
    protected final String BATCH_WRITE = "BatchWrite";
    protected final String SUM = "Sum";
    protected final String COUNT = "Count";
    protected final String FULL_SCAN = "FullScan";

    public SugarOrm(Context context, long numberOfObjects, long numberOfIterations) {
        super(context, numberOfObjects);
        this.numberOfIterations = numberOfIterations;
    }

    public void write() {
        Log.i("DataStoreBenchmark", "SugarOrm invoking write");
        //intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();

        delete();




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                add();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
            }


        delete();

        finalvalues.put(SIMPLE_WRITE, values);

        System.gc();
    }


    public void simpleQuery() {
        Log.i("DataStoreBenchmark", "SugarOrm invoking Simple Query");
        //intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();

                delete();
                add();
                check();



        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                List<SugarOrmStudent> list = Select.from(SugarOrmStudent.class).where(Condition.prop("csci").eq(0), Condition.prop("points").gt(80).lt(100), Condition.prop("name").eq("student6")).list();
                for (SugarOrmStudent e : list) {
                    long tmp = e.getId();
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
            }

        delete();

        finalvalues.put(SIMPLE_QUERY, values);

        System.gc();
    }


    public void batchWrite() {
        Log.i("DataStoreBenchmark", "SugarOrm invoking batch write");
        //intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
             List<SugarOrmStudent> sugarStudentList;


                delete();
                sugarStudentList = new ArrayList<SugarOrmStudent>();
                for (int i = 0; i < numberOfObjects; i++) {
                    SugarOrmStudent student = new SugarOrmStudent(i, dataGenerator.getStudentName(i), dataGenerator.getStudentPoints(i), dataGenerator.getCsciReg(i));
                    sugarStudentList.add(student);
                }




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                SugarOrmStudent.saveInTx(sugarStudentList);
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
            }


        delete();
        sugarStudentList.clear();

        finalvalues.put(BATCH_WRITE, values);
    }


    public void sum() {
        Log.i("DataStoreBenchmark", "SugarOrm invoking Sum");
        //intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
                delete();
                add();
                check();





        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                List<SugarOrmStudent> sugarStudentList = Select.from(SugarOrmStudent.class).list();
                long sum = 0;
                for (SugarOrmStudent e: sugarStudentList) {
                    sum += e.getPoints();
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
            }


        delete();

        finalvalues.put(SUM, values);
    }


    public void count() {
        Log.i("DataStoreBenchmark", "SugarOrm invoking count");
        //intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
                delete();
                add();
                check();




            for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                long count = SugarOrmStudent.count(SugarOrmStudent.class, null, null, null, null, null);
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }


        delete();

        finalvalues.put(COUNT, values);

        System.gc();
    }


    public void fullScan() {
        Log.i("DataStoreBenchmark", "SugarOrm invoking full Scan");
        //intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
                delete();
                add();
                check();




    for (int j = 0; j < numberOfIterations; j++) {
        System.gc();
        timeStart = Debug.threadCpuTimeNanos();
                Select outcome = Select.from(SugarOrmStudent.class)
                        .where(Condition.prop("name").eq("student1"), Condition.prop("points").gt(100).lt(200), Condition.prop("csci").eq(1));
                long tmp = outcome.list().size();
        long timeStop = Debug.threadCpuTimeNanos();
        long duration = timeStop - timeStart; //  
        values.add(duration);
    }


        delete();

        finalvalues.put(FULL_SCAN, values);

        System.gc();
    }
    private void add() {
        for (int i = 0; i < numberOfObjects; i++) {
            SugarOrmStudent student = new SugarOrmStudent(i,
                    dataGenerator.getStudentName(i),
                    dataGenerator.getStudentPoints(i),
                    dataGenerator.getCsciReg(i));
            student.save();
        }
    }

    private void delete() {
        SugarOrmStudent.deleteAll(SugarOrmStudent.class);
    }

    private void check() {
        if (SugarOrmStudent.count(SugarOrmStudent.class, null, null, null, null, null) != numberOfObjects) {
            throw new RuntimeException(String.format("Object exception.",
                    SugarOrmStudent.count(SugarOrmStudent.class, null, null, null, null, null), numberOfObjects));
        }
    }
}










// Library, abstract db aar and api call borrowed from http://satyan.github.io/sugar/getting-started.html