package com.example.samarth_shastri.advanceddb.databases;

import android.content.Context;
import android.os.Debug;
import android.util.Log;

import com.example.samarth_shastri.accessories.Student;
import com.example.samarth_shastri.accessories.abstractDb;

import java.util.ArrayList;
import java.util.List;

import io.realm.Realm;
import io.realm.RealmConfiguration;
import io.realm.RealmResults;

/**
 * Created by Samarth_Shastri and Bhavtosh on 11/25/15.
 */
public class dbRealm extends abstractDb {


    private long numberOfIterations;
    private RealmConfiguration realmConfiguration;

    protected final String SIMPLE_QUERY = "SimplePointQuery";
    protected final String SIMPLE_WRITE = "SimpleWrite";
    protected final String BATCH_WRITE = "BatchWrite";
    protected final String SUM = "Sum";
    protected final String COUNT = "Count";
    protected final String FULL_SCAN = "FullScan";

    public dbRealm(Context context, long numberOfObjects, long numberOfIterations) {
        super(context, numberOfObjects);
        this.numberOfIterations = numberOfIterations;
        this.realmConfiguration = new RealmConfiguration.Builder(context).build();
    }




    public void simpleQuery() {
        // super.intitalize();
        Realm.deleteRealm(realmConfiguration);
        Log.i("DataStoreBenchmark", "Realm invoking simple query");

             Realm realm;
         List<Long> values;
         long timeStart;
        values = new ArrayList<>();

                realm = Realm.getInstance(realmConfiguration);
                add(realm);
                check(realm);




        for (int i = 0; i < numberOfIterations; i++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                RealmResults<Student> students = realm.where(Student.class).equalTo("csci", false).between("points", 80, 100).findAll();
                for (Student student : students) {
                    long tmp = student.getStudentid();
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart;
            values.add(duration);
            }


                 realm.beginTransaction();         realm.clear(Student.class);         realm.commitTransaction();
                 realm.close();


        finalvalues.put(SIMPLE_QUERY, values);

        System.gc();
    }


    public void sum() {
        Log.i("DataStoreBenchmark", "Realm invoking sum");
          //super.intitalize();         Realm.deleteRealm(realmConfiguration);


            Realm realm;
        List<Long> values;
       long timeStart;
        values = new ArrayList<>();

                realm = Realm.getInstance(realmConfiguration);
                add(realm);
                check(realm);



        for (int i = 0; i < numberOfIterations; i++) {
            timeStart = Debug.threadCpuTimeNanos();
            System.gc();

            long sum = realm.where(Student.class).sumInt("points");
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart;
            values.add(duration);
        }


                realm.beginTransaction();         realm.clear(Student.class);         realm.commitTransaction();
                realm.close();


        finalvalues.put(SUM, values);

         System.gc();
    }


    public void count() {
        Log.i("DataStoreBenchmark", "Realm invoking count");
         // super.intitalize();
        Realm.deleteRealm(realmConfiguration);


            Realm realm;
        List<Long> values;
        long timeStart;
        values = new ArrayList<>();

                realm = Realm.getInstance(realmConfiguration);
                add(realm);
                check(realm);




        for (int i = 0; i < numberOfIterations; i++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
            long count = realm.where(Student.class).count();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart;
            values.add(duration);
        }

                realm.beginTransaction();         realm.clear(Student.class);         realm.commitTransaction();
                realm.close();


        finalvalues.put(COUNT, values);

         System.gc();
    }


    public void fullScan() {
        Log.i("DataStoreBenchmark", "Realm invoking full scan");
          //super.intitalize();
        Realm.deleteRealm(realmConfiguration);

        Realm realm;
        List<Long> values;
        long timeStart;
        values = new ArrayList<>();


                realm = Realm.getInstance(realmConfiguration);
                add(realm);
                check(realm);






        for (int i = 0; i < numberOfIterations; i++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                RealmResults<Student> realmResults = realm.where(Student.class).equalTo("csci", true).between("points", 110, 120).equalTo("name", "S1").findAll();
                long count = realmResults.size();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart;
            values.add(duration);
            }
        realm.beginTransaction();         realm.clear(Student.class);         realm.commitTransaction();
        realm.close();

        finalvalues.put(FULL_SCAN, values);

        Realm.deleteRealm(realmConfiguration);
        super.garbaceColl();
    }



    public void write() {
        Log.i("DataStoreBenchmark", "Realm invoking write");
         // super.intitalize();
        Realm.deleteRealm(realmConfiguration);


        Realm realm;
                int i;
                 List<Long> values;
                long timeStart;
            values = new ArrayList<>();

                realm = Realm.getInstance(realmConfiguration);
                i = 0;



        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
            realm.beginTransaction();
            Student student = realm.createObject(Student.class);
            student.setStudentid(999);
            student.setName("Student999");
            student.setCsci(true);
            student.setPoints(100);
            realm.commitTransaction();
            i++;
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart;
            values.add(duration);
            }



                realm.beginTransaction();         realm.clear(Student.class);         realm.commitTransaction();
                realm.close();


        finalvalues.put(SIMPLE_WRITE, values);
         System.gc();
    }


    public void batchWrite() {
        Log.i("DataStoreBenchmark", "Realm invoking batch write");
          //super.intitalize();
        Realm.deleteRealm(realmConfiguration);


           Realm realm;

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();


                realm = Realm.getInstance(realmConfiguration);




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                realm.beginTransaction();
                for (int i = 0; i < numberOfObjects; i++) {
                    Student student = realm.createObject(Student.class);
                    student.setStudentid(i);
                    student.setName(dataGenerator.getStudentName(i));
                    student.setCsci(dataGenerator.getCsciBool(i));
                    student.setPoints(dataGenerator.getStudentPoints(i));
                }
                realm.commitTransaction();
            long timeStop = Debug.threadCpuTimeNanos();
        long duration = timeStop - timeStart;
        values.add(duration);
    }

    realm.beginTransaction();         realm.clear(Student.class);         realm.commitTransaction();
    realm.close();

        finalvalues.put(BATCH_WRITE, values);

         System.gc();
    }
    private void add(Realm realm) {
        realm.beginTransaction();
        for (int i = 0; i < numberOfObjects; i++) {
            Student student = realm.createObject(Student.class);
            student.setName(dataGenerator.getStudentName(i));
            student.setPoints(dataGenerator.getStudentPoints(i));
            student.setCsci(dataGenerator.getCsciBool(i));
            student.setStudentid(i);
        }
        realm.commitTransaction();
    }

    private void check(Realm realm) {
        RealmResults<Student> realmResults = realm.allObjects(Student.class);
        if (realmResults.size() != numberOfObjects) {
            throw new RuntimeException(String.format("Error in number of objectss.",
                    realmResults.size(), numberOfObjects));

        }
    }

   

}



//Reference- https://realm.io/docs/java/