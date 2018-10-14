package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

/**
 * Created by prettyphilip on 5/3/18.
 */

public class OnGDumpClickListener implements OnClickListener {

    private static final String TAG = OnTestClickListener.class.getName();
    private static final int TEST_CNT = 5;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;
    private final ContentValues[] mContentValues;

    public OnGDumpClickListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        mContentValues = initTestValues();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private ContentValues[] initTestValues() {
        ContentValues[] cv = new ContentValues[TEST_CNT];
        for (int i = 0; i < TEST_CNT; i++) {
            cv[i] = new ContentValues();
            cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
            cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
        }

        return cv;
    }

    @Override
    public void onClick(View v) {
        new OnGDumpClickListener.Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            /*if(testInsert()){
                publishProgress("Insert Global Success\n");
            } else {
                publishProgress("Insert Global fail\n");
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            //testQuery();
            if (testDelete()) {
                publishProgress("Delete Global success\n");
            } else {
                publishProgress("Delete Global fail\n");
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);

            return;
        }

        private boolean testInsert() {
            try {
                for (int i = 0; i < TEST_CNT; i++) {
                    mContentResolver.insert(mUri, mContentValues[i]);
                }
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }

        private void testQuery() {
            try {

                Cursor resultCursor = mContentResolver.query(mUri, null,
                        "*", null, null);
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }
                /*if(resultCursor.getCount()!=TEST_CNT) {
                    Log.e(TAG,"Wrong No of rows: "+resultCursor.getCount());
                    throw new Exception();
                }*/
                resultCursor.moveToFirst();
                int i=1;
                while(!resultCursor.isAfterLast()) {
                    publishProgress(i+":"+resultCursor.getString(0)+"\t"+resultCursor.getString(1)+"\n");
                    resultCursor.moveToNext();
                    i++;
                }
                resultCursor.close();
            } catch (Exception e) {
                e.printStackTrace();
                publishProgress("Query Global fail\n");
                Log.e(TAG,"Exception occured");
            }
        }

        private boolean testDelete() {
            try {
                mContentResolver.delete(mUri, "*",null);
                Cursor resultCursor = mContentResolver.query(mUri, null,
                        "*", null, null);
                //Log.v("MessengerDBHelper","INSERT | Cursor all Object"+ DatabaseUtils.dumpCursorToString(resultCursor));
                if (resultCursor.getCount() != 0) {
                    Log.e(TAG, "Result should be empty");
                    throw new Exception();
                }
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }
    }
}