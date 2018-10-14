package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by prettyphilip on 5/2/18.
 */

public class DynamoDBHelper extends SQLiteOpenHelper {

    //DB-Table Details
    private static final String DB_NAME = "DS_DYNAMO_DB";
    private static final int DATABASE_VERSION = 1;
    private static final String TABLE_NAME = "DYNAMO";

    //Column name
    private static final String KEY = "key";
    private static final String VALUE = "value";

    public DynamoDBHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
        super(context, DB_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + "("
                + KEY + " TEXT PRIMARY KEY," + VALUE + " TEXT)";
        sqLiteDatabase.execSQL(CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(sqLiteDatabase);
    }

    public void insertDBHelper(SQLiteDatabase sqLiteDatabase, ContentValues contentValues){
        sqLiteDatabase.insertWithOnConflict(TABLE_NAME,null, contentValues,SQLiteDatabase.CONFLICT_REPLACE);
        //Print all the values in db
        Cursor cursorA = sqLiteDatabase.query(TABLE_NAME, null, null,
                null, null, null, null);
        //Log.v("MessengerDBHelper","INSERT | Cursor all Object"+DatabaseUtils.dumpCursorToString(cursorA));
    }

    public Cursor queryDBHelperSingleKey(SQLiteDatabase sqLiteDatabase, String key){
        /*Cursor cursorA = sqLiteDatabase.query(TABLE_NAME, null, null, null, null, null, null);
        Log.v("MessengerDBHelper","Cursor all Object"+DatabaseUtils.dumpCursorToString(cursorA));*/
        Cursor cursor = sqLiteDatabase.query(TABLE_NAME, null, KEY + "=?",
                new String[] {key }, null, null, null, null);
        //Log.v("MessengerDBHelper","QUERY | Cursor Object"+DatabaseUtils.dumpCursorToString(cursor));
        //Log.v("SINGLE KEY QUERY","key: "+key+" cursor length:"+cursor.getCount());
        return cursor;
    }

    public Cursor queryDBHelperAllKey(SQLiteDatabase sqLiteDatabase){
        Cursor cursor = sqLiteDatabase.query(TABLE_NAME, null, null,
                null, null, null, null, null);
        //Log.v("MessengerDBHelper","QUERY | Cursor Object"+ DatabaseUtils.dumpCursorToString(cursor));
        /*cursor = sqLiteDatabase.query(TABLE_NAME, null, KEY + " BETWEEN ? AND ?",
                new String[] {range1,range2 }, null, null, null, null);
        Log.v("MessengerDBHelper","QUERY | Cursor Object"+ DatabaseUtils.dumpCursorToString(cursor));*/
        return cursor;
    }

    public void deleteDBHelperKey(SQLiteDatabase sqLiteDatabase, String key){
        /*Cursor cursor = sqLiteDatabase.query(TABLE_NAME, null, null,
                null, null, null, null);*/
        //Log.v("MessengerDBHelper","QUERY | Cursor Object"+DatabaseUtils.dumpCursorToString(cursor));
        sqLiteDatabase.delete(TABLE_NAME,KEY + "=?", new String[] {key});
    }
    public void deleteDBHelperAllKey(SQLiteDatabase sqLiteDatabase){
        /*Cursor cursor = sqLiteDatabase.query(TABLE_NAME, null, null,
                null, null, null, null);*/
        //Log.v("MessengerDBHelper","DELETE | ALL");
        sqLiteDatabase.delete(TABLE_NAME,null, null);
    }
}
