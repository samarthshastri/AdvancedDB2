package com.example.samarth_shastri.advanceddb.databases;

import android.content.Context;
import android.os.Debug;
import android.util.Log;

import com.example.samarth_shastri.accessories.abstractDb;
import com.example.samarth_shastri.orm.Data;
import com.example.samarth_shastri.orm.Ormhelper;
import com.example.samarth_shastri.orm.Student;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.GenericRawResults;
import com.j256.ormlite.misc.TransactionManager;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class Ormlite extends abstractDb {

    private long numberOfIterations;
    private Ormhelper ormhelper;    protected final String SIMPLE_QUERY = "SimplePointQuery";
    protected final String SIMPLE_WRITE = "SimpleWrite";
    protected final String BATCH_WRITE = "BatchWrite";
    protected final String SUM = "Sum";
    protected final String COUNT = "Count";
    protected final String FULL_SCAN = "FullScan";
    public Ormlite(Context context, long numberOfObjects, long numberOfIterations) {
        super(context, numberOfObjects);
        this.numberOfIterations = numberOfIterations;
    }




    public void write() {
        Log.i("DataStoreBenchmark", "OrmLite invoking write");
        intitalize();

             int i;
        List<Long> values;
        long timeStart;
        values = new ArrayList<>();


                i = 0;


        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                try {
                    Dao<Data, Long> dao = Data.getDao();
                     Data data = new Data();
                    data.setName(dataGenerator.getStudentName(i));
                    data.setCsci(dataGenerator.getCsciBool(i));
                    data.setPoints(dataGenerator.getStudentPoints(i));
                    dao.create(data);
                    i++;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }



                deleteObjects();


        finalvalues.put(SIMPLE_WRITE,values);
        garbaceColl();
    }


    public void simpleQuery() {
        Log.i("DataStoreBenchmark", "ORmlite invoking simple query");
        intitalize();
        List<Long> values;
        long timeStart;
        values = new ArrayList<>();


                add();
                check();



        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                try {
                    Dao<Data, Long> dao = Data.getDao();
                    List<Data> datas = dao.queryBuilder().where().eq("csci", false).and().between("points", 80, 100).and().eq("name", "student5").query();

                    for (Data data : datas) {
                        long id = data.getId();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }



                deleteObjects();

        finalvalues.put(SIMPLE_QUERY, values);
        garbaceColl();
    }


    public void batchWrite() {
        Log.i("DataStoreBenchmark", "ORMlite invoking simple query");
        intitalize();
        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
           ConnectionSource connectionSource;


                connectionSource = ormhelper.getConnectionSource();


        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                try {
                    TransactionManager.callInTransaction(connectionSource, new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            Dao<Student, Long> dao = Student.getDao();
                            for (int i = 0; i < numberOfObjects; i++) {
                                Student student = new Student();
                                student.setName(dataGenerator.getStudentName(i));
                                student.setCsci(dataGenerator.getCsciBool(i));
                                student.setPoints(dataGenerator.getStudentPoints(i));
                                dao.create(student);
                            }
                            return null;
                        }
                    });
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }


                deleteObjects();


        finalvalues.put(BATCH_WRITE, values);
        garbaceColl();
        }






    public void sum() {
        Log.i("DataStoreBenchmark", "ORMliteinvoking simple query");
        intitalize();

        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
        add();
                check();

        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                try {
                    Dao<Data, Long> dao = Data.getDao();
                    GenericRawResults<String[]> queryResult = dao.queryBuilder().selectRaw("SUM(points)").queryRaw();
                    String sum = queryResult.getFirstResult()[0];
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }



                deleteObjects();

        finalvalues.put(SUM, values);
        garbaceColl();
    }


    public void count() {
        Log.i("DataStoreBenchmark", "OrmLiteinvoking simple query");
        intitalize();
        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
                add();
                check();


        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                try {
                    long count = Data.getDao().countOf();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            long timeStop = Debug.threadCpuTimeNanos();
            long duration = timeStop - timeStart; //  
            values.add(duration);
        }




                deleteObjects();

        finalvalues.put(COUNT, values);
        garbaceColl();
    }


    public void fullScan() {
        Log.i("DataStoreBenchmark", "ORmlite invoking simple query");
        intitalize();
        List<Long> values;
        long timeStart;
        values = new ArrayList<>();
                add();
                check();


        for (int j = 0; j < numberOfIterations; j++) {
            System.gc();
            timeStart = Debug.threadCpuTimeNanos();
                try {
                    Dao<Data, Long> dao = Data.getDao();
                    List<Data> datas = dao.queryBuilder().where().eq("csci", true).and().between("points", 200, 300).and().eq("name", "Smile1").query();
                    int size = datas.size();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                long timeStop = Debug.threadCpuTimeNanos();
                long duration = timeStop - timeStart; //  
                values.add(duration);
            }


                deleteObjects();

        finalvalues.put(FULL_SCAN, values);
        garbaceColl();
    }

    private void add() {
        try {
            for (int i = 0; i < numberOfObjects; i++) {
                Data data = new Data();
                data.setName(dataGenerator.getStudentName(i));
                data.setCsci(dataGenerator.getCsciBool(i));
                data.setPoints(dataGenerator.getStudentPoints(i));
                Data.getDao().create(data);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

  
    private void check() {
        List<Data> datas;
        try {
           datas = Data.getDao().queryForAll();
            if (datas.size() != numberOfObjects) {
                throw new RuntimeException(String.format("exception.",
                        datas.size(), numberOfObjects));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    
    private void deleteObjects() {
        ConnectionSource connectionSource = ormhelper.getConnectionSource();
        try {
            TableUtils.dropTable(connectionSource, Data.class, true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void intitalize() {
        //super.intitalize();
        ormhelper.init(context, false);
        ormhelper = Ormhelper.getInstance();
        ConnectionSource connectionSource = ormhelper.getConnectionSource();
        try {
            TableUtils.dropTable(connectionSource, Data.class, true);
            TableUtils.createTable(connectionSource, Data.class);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void garbaceColl() {
        ConnectionSource connectionSource = ormhelper.getConnectionSource();
        try {
            TableUtils.dropTable(connectionSource, Data.class, true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        super.garbaceColl();
    }

}
