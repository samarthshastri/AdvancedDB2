package com.example.samarth_shastri.advanceddb;

import android.os.Bundle;
import android.os.Environment;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.Toast;

import com.example.samarth_shastri.advanceddb.databases.GreenDAO;
import com.example.samarth_shastri.advanceddb.databases.Ormlite;
import com.example.samarth_shastri.advanceddb.databases.SQLlite;
import com.example.samarth_shastri.advanceddb.databases.SugarOrm;
import com.example.samarth_shastri.advanceddb.databases.dbRealm;

import java.io.File;
import java.io.FileOutputStream;


//Embedded Mobile Database Benchmarking by Bhavtosh and Samarth
public class MainActivity extends AppCompatActivity {
    final private long ITERATIONS = 10;
    final private long OBJECTS = 1000;
    Button b5,b6;
    ImageButton b1,b2,b3,b4;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        b1=(ImageButton)findViewById(R.id.Button);
        b2=(ImageButton)findViewById(R.id.Button2);
        b3=(ImageButton)findViewById(R.id.Button3);
        b4=(ImageButton)findViewById(R.id.Button4);
        b5=(Button)findViewById(R.id.button5);
        b6=(Button)findViewById(R.id.button6);



        File directory = new File(Environment.getExternalStorageDirectory() + "/datastore");
        directory.mkdirs();

        String FILE = "datastore";



        b1.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new Thread(new Runnable() {

                    public void run() {
                        String FILE = "datastore";
                        dbRealm dbrealm = new dbRealm(getApplicationContext(), OBJECTS, ITERATIONS);

                        dbrealm.batchWrite();
                        dbrealm.count();
                        dbrealm.fullScan();
                        dbrealm.simpleQuery();
                        dbrealm.sum();
                        dbrealm.write();


                        dbrealm.saveHeader(FILE);
                        dbrealm.saveValues("realm", FILE);
                        new Thread(new Runnable() {

                            public void run() {
                                Toast.makeText(getApplicationContext(),
                                        "Button is clicked", Toast.LENGTH_LONG).show();
                            }

                        });
                    }
                }).start();
            }
        });

        b2.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new Thread(new Runnable() {

                    public void run() {
                        String FILE = "datastore";
                        SQLlite SqLite = new SQLlite(getApplicationContext(), OBJECTS, ITERATIONS);
                        SqLite.write();
                        SqLite.sum();
                        SqLite.simpleQuery();
                        SqLite.fullScan();
                        SqLite.batchWrite();
                        SqLite.count();
                        SqLite.saveValues("sqlite", FILE);

                    }
                }).start();
            }
        });

        b3.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new Thread(new Runnable() {

                    public void run() {
                        String FILE = "datastore";
                        Ormlite ormLite = new Ormlite(getApplicationContext(), OBJECTS, ITERATIONS);
                        ormLite.count();
                        ormLite.batchWrite();
                        ormLite.fullScan();
                        ormLite.simpleQuery();
                        ormLite.sum();
                        ormLite.write();
                        ormLite.saveValues("ormlite", FILE);

                    }
                }).start();
            }
        });

        b4.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new Thread(new Runnable() {

                    public void run() {
                        String FILE = "datastore";

                        GreenDAO greenDAO = new GreenDAO(getApplicationContext(), OBJECTS, ITERATIONS);
                        greenDAO.write();
                        greenDAO.sum();
                        greenDAO.simpleQuery();
                        greenDAO.fullScan();
                        greenDAO.batchWrite();
                        greenDAO.count();

                        greenDAO.saveValues("greendao", FILE);

                    }
                }).start();
            }
        });

        b5.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new Thread(new Runnable() {

                    public void run() {
                        String FILE = "datastore";
                        SugarOrm sugarOrm = new SugarOrm(getApplicationContext(), OBJECTS, ITERATIONS);
                        sugarOrm.batchWrite();
                        sugarOrm.count();
                        sugarOrm.write();
                        sugarOrm.fullScan();
                        sugarOrm.simpleQuery();
                        sugarOrm.sum();
                        sugarOrm.saveValues("sugarorm", FILE);

                    }
                }).start();
            }
        });

        b6.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new Thread(new Runnable() {

                    public void run() {
                        String FILE = "datastore";
                        dbRealm dbrealm = new dbRealm(getApplicationContext(), OBJECTS, ITERATIONS);
                        dbrealm.batchWrite();
                        dbrealm.count();
                        dbrealm.fullScan();
                        dbrealm.simpleQuery();
                        dbrealm.sum();
                        dbrealm.write();

                        dbrealm.saveHeader(FILE);
                        dbrealm.saveValues("realm", FILE);

                        SQLlite SqLite = new SQLlite(getApplicationContext(), OBJECTS, ITERATIONS);
                        SqLite.write();
                        SqLite.sum();
                        SqLite.simpleQuery();
                        SqLite.fullScan();
                        SqLite.batchWrite();
                        SqLite.count();
                        SqLite.saveValues("sqlite", FILE);

                        Ormlite ormLite = new Ormlite(getApplicationContext(), OBJECTS, ITERATIONS);
                        ormLite.count();
                        ormLite.batchWrite();
                        ormLite.fullScan();
                        ormLite.simpleQuery();
                        ormLite.sum();
                        ormLite.write();
                        ormLite.saveValues("ormlite", FILE);

                        GreenDAO greenDAO = new GreenDAO(getApplicationContext(), OBJECTS, ITERATIONS);
                        greenDAO.write();
                        greenDAO.sum();
                        greenDAO.simpleQuery();
                        greenDAO.fullScan();
                        greenDAO.batchWrite();
                        greenDAO.count();
                        greenDAO.saveValues("greendao", FILE);

                        SugarOrm sugarOrm = new SugarOrm(getApplicationContext(), OBJECTS, ITERATIONS);
                        sugarOrm.batchWrite();
                        sugarOrm.count();
                        sugarOrm.write();
                        sugarOrm.fullScan();
                        sugarOrm.simpleQuery();
                        sugarOrm.sum();
                        sugarOrm.saveValues("sugarorm", FILE);
                    }
                }).start();
            }
        });

        try {
            File file = new File(Environment.getExternalStorageDirectory() + "/datastore", "timer.txt");
            FileOutputStream fileOutputStream = new FileOutputStream(file, false);
            fileOutputStream.write(String.format("%d\n", ITERATIONS).getBytes());
            fileOutputStream.write(String.format("%d\n", OBJECTS).getBytes());
            fileOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }




    }




}






//Note- Library and API call code borrowed and implemented accordingly, from open source database documentations Realm,Sqllite,SugarOrm,GreenDAO and Ormlite documentation.
//www.realm.io, https://www.sqlite.org/,http://greendao-orm.com/,http://ormlite.com/,http://satyan.github.io/sugar/