package com.example.samarth_shastri.advanceddb.databases;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.os.Debug;
import android.util.Log;

import com.example.samarth_shastri.accessories.abstractDb;
import com.example.samarth_shastri.greendao.*;

import java.util.ArrayList;
import java.util.List;




public class GreenDAO extends abstractDb {
    private long numberOfIterations;

    protected final String SIMPLE_QUERY = "SimplePointQuery";
    protected final String SIMPLE_WRITE = "SimpleWrite";
    protected final String BATCH_WRITE = "BatchWrite";
    protected final String SUM = "Sum";
    protected final String COUNT = "Count";
    protected final String FULL_SCAN = "FullScan";
    private SQLiteDatabase db;
    private DaoMaster daoMaster;
    private DaoSession daoSession;
    private StudentDao studentDao;


    public GreenDAO(Context context, long numberOfObjects, long numberOfIterations) {
        super(context, numberOfObjects);
        this.numberOfIterations = numberOfIterations;
    }




    public void write() {
        Log.i("DataStoreBenchmark", "GreenDAO invoking write");
        intitalize();

        List<Long> timings;
        long timeStart;
        timings = new ArrayList<>();






        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                for (int i = 0; i < numberOfObjects; i++) {
                    try {
                        db.beginTransaction();
                        com.example.samarth_shastri.greendao.Student student = new com.example.samarth_shastri.greendao.Student();
                        student.setName(dataGenerator.getStudentName(i));
                        student.setPoints(dataGenerator.getStudentPoints(i));
                        student.setCsci(dataGenerator.getCsciBool(i));
                        studentDao.insert(student);
                        db.setTransactionSuccessful();
                    } catch (Exception e) {
                        throw new RuntimeException(".");
                    } finally {
                        db.endTransaction();
                    }
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            timings.add(duration);
            }

    delete();
        finalvalues.put(SIMPLE_WRITE, timings);

        studentDao.dropTable(db, true);         db.close();
    }


    public void simpleQuery() {
        Log.i("DataStoreBenchmark", "GreenDAO invoking simple query");
        intitalize();


        List<Long> timings;
        long timeStart;
        timings = new ArrayList<>();

                delete();
                add();
                check();





        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                de.greenrobot.dao.query.QueryBuilder qb = studentDao.queryBuilder();
                qb.where(qb.and(StudentDao.Properties.Name.eq("Foo1"),
                        StudentDao.Properties.Points.between(20, 50),
                        StudentDao.Properties.Csci.eq(true)));
                qb.build();
                List<com.example.samarth_shastri.greendao.Student> list = qb.list();
                for (com.example.samarth_shastri.greendao.Student e : list) {
                    long tmp = e.getId();
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            timings.add(duration);

            }

        delete();

        finalvalues.put(SIMPLE_QUERY, timings);

        studentDao.dropTable(db, true);         db.close();
    }


    public void batchWrite() {
        Log.i("DataStoreBenchmark", "GreenDAO invoking batch write");
        intitalize();

        List<Long> timings;
        long timeStart;
        timings = new ArrayList<>();

        delete();



        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                try {
                    db.beginTransaction();
                    for (int i = 0; i < numberOfObjects; i++) {
                        com.example.samarth_shastri.greendao.Student student = new com.example.samarth_shastri.greendao.Student();
                        student.setName(dataGenerator.getStudentName(i));
                        student.setPoints(dataGenerator.getStudentPoints(i));
                        student.setCsci(dataGenerator.getCsciBool(i));
                        studentDao.insert(student);
                    }
                    db.setTransactionSuccessful();
                } catch (Exception e) {
                    throw new RuntimeException(".");
                } finally {
                    db.endTransaction();
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            timings.add(duration);
            }

        delete();

        finalvalues.put(BATCH_WRITE, timings);

        studentDao.dropTable(db, true);         db.close();
    }


    public void sum() {
        Log.i("DataStoreBenchmark", "GreenDAO invoking sum");
        intitalize();

        List<Long> timings;
        long timeStart;
        timings = new ArrayList<>();

                delete();
                add();
                check();



        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                List<com.example.samarth_shastri.greendao.Student> list = studentDao.loadAll();
                int tmp = 0;
                for (com.example.samarth_shastri.greendao.Student e : list) {
                    tmp += e.getPoints();
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            timings.add(duration);
            }


        delete();

        finalvalues.put(SUM, timings);

        studentDao.dropTable(db, true);         db.close();
    }


    public void count() {
        Log.i("DataStoreBenchmark", "GreenDAO invoking count");
        intitalize();

        List<Long> timings;
        long timeStart;
        timings = new ArrayList<>();
                delete();
                add();
                check();




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                List<com.example.samarth_shastri.greendao.Student> list = studentDao.loadAll();
                int tmp = list.size();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            timings.add(duration);
            }


        delete();

        finalvalues.put(COUNT, timings);

        studentDao.dropTable(db, true);         db.close();
    }


    public void fullScan() {
        Log.i("DataStoreBenchmark", "GreenDAO invoking full scan");
        intitalize();

        List<Long> timings;
        long timeStart;
        timings = new ArrayList<>();
                delete();
                add();
                check();




        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                de.greenrobot.dao.query.QueryBuilder qb = studentDao.queryBuilder();
                qb.where(qb.and(StudentDao.Properties.Name.eq("Smile1"),
                        StudentDao.Properties.Points.between(-2, -1),
                        StudentDao.Properties.Csci.eq(false)));
                qb.build();
                List<com.example.samarth_shastri.greendao.Student> list = qb.list();
                int count = list.size();
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            timings.add(duration);
            }



        delete();

        finalvalues.put(FULL_SCAN, timings);

        studentDao.dropTable(db, true);         db.close();
    }
    private void add() {
        try {
            db.beginTransaction();
            for (int i = 0; i < numberOfObjects; i++) {
                com.example.samarth_shastri.greendao.Student student = new com.example.samarth_shastri.greendao.Student();
                student.setName(dataGenerator.getStudentName(i));
                student.setPoints(dataGenerator.getStudentPoints(i));
                student.setCsci(dataGenerator.getCsciBool(i));
                studentDao.insert(student);
            }
            db.setTransactionSuccessful();
        } catch (Exception e) {
            throw new RuntimeException("Cannot add object.");
        } finally {
            db.endTransaction();
        }
    }

    private void delete() {
        try {
            db.beginTransaction();
            studentDao.deleteAll();
            db.setTransactionSuccessful();
        }  catch (Exception e) {
            throw new RuntimeException("delete exception.");
        } finally {
            db.endTransaction();
        }
    }

    private void check() {
        List<com.example.samarth_shastri.greendao.Student> list = studentDao.loadAll();

        if (list.size() < numberOfObjects) {
            throw new RuntimeException(String.format("Format exception.",
                    list.size(), numberOfObjects));
        }
    }
    public void intitalize() {
        DaoMaster.DevOpenHelper helper
                = new DaoMaster.DevOpenHelper(context, "EmployeeGreenDAO.db", null);
        db = helper.getWritableDatabase();
        daoMaster = new DaoMaster(db);
        daoSession = daoMaster.newSession();
        studentDao = daoSession.getStudentDao();
        studentDao.createTable(db, true);
    }

    public void garbaceColl() {
        studentDao.dropTable(db, true);
        db.close();
    }
}
