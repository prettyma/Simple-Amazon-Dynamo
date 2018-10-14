package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDynamoProvider extends ContentProvider {

    String myPort;
    TreeMap<String, String> activeNodeMap;
    Comparator<String> chordRingComparator;
    DynamoDBHelper dynamoDBHelper;
    SQLiteDatabase sqLiteDatabase;
    private Semaphore recoveryLock;
    private Context context;

    public static String succ="";
    public static String pred="";
    static final int SERVER_PORT = 10000;
    public static final String CONTENT_KEY = "key";
    public static final String CONTENT_VALUE = "value";
    public static final String FAILURE = "Failure";
    public static final String SUCCESS = "Success";

    //Message's JSON keys
    static final String MESSAGE_TYPE = "type";
    static final String INSERT_TYPE = "insert_type";
    static final String NODES_LIST = "node_list";
    static final String HOST_PORT = "host_port";
    static final String DELETE_TYPE = "delete_type";

    static final int DEFAULT_TIMEOUT = 5000;
    static final int QUERY_TIMEOUT = 1500;

    public static enum MESSAGE_ENUM {
        INSERT, QUERY, DELETE, RECOVERY
    }

    public static enum INSERT_ENUM {
        COORD, REPLICA1, REPLICA2
    }

    public static enum DELETE_ENUM {
        COORD, REPLICA1, REPLICA2
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        Log.v("DELETE DB","Selection: "+selection);
        try {
            //boolean isFirstNode = false;
            if (selection.compareTo("@") == 0) {
                recoveryLock.acquire();
                dynamoDBHelper.deleteDBHelperAllKey(sqLiteDatabase);
                recoveryLock.release();
                return 1;
            } else if (selection.compareTo("*") == 0) {
                recoveryLock.acquire();
                dynamoDBHelper.deleteDBHelperAllKey(sqLiteDatabase);
                recoveryLock.release();
                JSONArray nodesToSend = getAllNodesListExceptCurr(myPort);
                String msgToSend = createMsgForDelete(DELETE_ENUM.COORD, selection, myPort, nodesToSend);
                String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msgToSend,nodesToSend.getString(nodesToSend.length()-1)).get();
                if(resp.equalsIgnoreCase(FAILURE)){
                    nodesToSend.remove(nodesToSend.length()-1);
                    msgToSend = createMsgForDelete(DELETE_ENUM.COORD, selection, myPort, nodesToSend);
                    resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msgToSend,nodesToSend.getString(nodesToSend.length()-1)).get();
                }
                return 1;
            } else {
                String port =getCoordNode(selection);
                JSONArray nodesToSend = getAllNodesListExceptCurr(myPort);
                String msgToSend = createMsgForDelete(DELETE_ENUM.COORD, selection, myPort, nodesToSend);
                String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msgToSend,port).get();
                if(resp.equalsIgnoreCase(FAILURE)){
                    //nodesToSend.remove(nodesToSend.length()-1);
                    msgToSend = createMsgForDelete(DELETE_ENUM.REPLICA1, selection, myPort, nodesToSend);
                    resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msgToSend,getNextNode(port)).get();
                    if(resp.equalsIgnoreCase(FAILURE)){
                        msgToSend = createMsgForDelete(DELETE_ENUM.COORD, selection, myPort, nodesToSend);
                        resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msgToSend,port).get();
                    }
                }
                return 1;
            }
        } /*catch (NoSuchAlgorithmException e){
            Log.e("QUERY",String.valueOf(e));
            e.printStackTrace();
        } */catch (JSONException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.v("DB | INSERT","Start "+values.toString());
        String key = values.get(CONTENT_KEY).toString();
        String coordNode = "";
        try {
            JSONArray nodesToSend = getNodesListFromKeyHash(key, coordNode, MESSAGE_ENUM.INSERT);
            Log.v("DB | INSERT","for: "+values.toString()+ " at nodes: "+nodesToSend.toString());
            String msgToSend = createMsgForInsert(key,values.get(CONTENT_VALUE).toString(), INSERT_ENUM.COORD, nodesToSend);

            String port  = getCoordNode(key);
            String resp;
            if(port.compareTo(myPort)==0){
                Log.v("DB | INSERT", "Coord is myPort so sending to succ node :"+succ);
                recoveryLock.acquire();
                dynamoDBHelper.insertDBHelper(sqLiteDatabase,values);
                Log.v("INSERT | DB","came with coord as me key "+key+" with value"+values.get(CONTENT_VALUE).toString());
                recoveryLock.release();
                msgToSend = createMsgForInsert(key,values.get(CONTENT_VALUE).toString(), INSERT_ENUM.REPLICA1, nodesToSend);
                resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, succ).get();
                Log.v("DB | INSERT","KEY :"+values.toString()+"DB INSERT IS DONE AND RECEIVED : "+resp+" in Replica1");
                if(resp.equalsIgnoreCase(FAILURE)){
                    msgToSend = createMsgForInsert(key,values.get(CONTENT_VALUE).toString(), INSERT_ENUM.REPLICA2, nodesToSend);
                    resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, getNextNode(succ)).get();
                    Log.v("DB | INSERT","KEY :"+values.toString()+"DB INSERT IS DONE AND RECEIVED : "+resp+" in Replica2");
                }
            } else{
                Log.v("DB | INSERT", "Coord is NOT myPort so sending to cooird node :"+port);
                resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, port).get();
                Log.v("DB | INSERT","KEY :"+values.toString()+"DB INSERT IS DONE AND RECEIVED : "+resp+" in Coord");
                if(resp.equalsIgnoreCase(FAILURE)){
                    //port = getNextNode(port);
                    msgToSend = createMsgForInsert(key,values.get(CONTENT_VALUE).toString(), INSERT_ENUM.REPLICA1, nodesToSend);
                    resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, getNextNode(port)).get();
                    Log.v("DB | INSERT","KEY :"+values.toString()+"DB INSERT IS DONE AND RECEIVED : "+resp+" in Replica1");
                    if(resp.equalsIgnoreCase(FAILURE)){
                        msgToSend = createMsgForInsert(key,values.get(CONTENT_VALUE).toString(), INSERT_ENUM.COORD, nodesToSend);
                        resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, port).get();
                        Log.v("DB | INSERT","KEY :"+values.toString()+"DB INSERT IS DONE AND RECEIVED : "+resp+" in Replica2");
                    }
                }
            }
            Log.v("DB | INSERT","KEY :"+values.toString()+" DB INSERT IS DONE AND RECEIVED : "+resp+" FINAL");
            return uri;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }catch (Exception e) {
            Log.e("DB | INSERT", String.valueOf(e));
            e.printStackTrace();
        }
        Log.v("DB | INSERT","Done");
        return null;
    }

    private synchronized String getCoordNode(String key){
        try {
            String coordNode="";
            String coordKey="";
            String currKey="";
            String genKey = genHash(key);
            for (Map.Entry<String, String> entry : activeNodeMap.entrySet()) {
//                Log.v("INSERT","genKey "+genKey);
                currKey = entry.getKey();
                if (currKey.compareTo(activeNodeMap.lastKey()) == 0) {
                    coordNode = activeNodeMap.firstKey();
                    break;
                }
                coordKey = activeNodeMap.higherKey(currKey);
                if (genKey.compareTo(coordKey) < 0 && genKey.compareTo(currKey) > 0) {
                    coordNode = coordKey;
                    break;
                }
            }
            return activeNodeMap.get(coordNode);
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return null;
    }

    private synchronized String getQueryNode(String key){
        try {
            String coordNode="";
            String coordKey="";
            String currKey="";
            String genKey = genHash(key);
            for (Map.Entry<String, String> entry : activeNodeMap.entrySet()) {
                currKey = entry.getKey();
                if (currKey.compareTo(activeNodeMap.lastKey()) == 0) {
                    coordNode = activeNodeMap.firstKey();
                    break;
                }
                coordKey = activeNodeMap.higherKey(currKey);
                if (genKey.compareTo(coordKey) < 0 && genKey.compareTo(currKey) > 0) {
                    coordNode = coordKey;
                    break;
                }
            }
            String replicaNode1 = "";
            String replicaNode2 = "";
            //if coordinator is the last key and the replica should be the first two nodes in the TreeMap
            if(coordNode.compareTo(activeNodeMap.lastKey())==0){
                replicaNode1 = activeNodeMap.firstKey();
            } else {
                replicaNode1 = activeNodeMap.higherKey(coordNode);
            }
            //if the coordinator is the second last key and the last replica should be the first node in the TreeMap
            if(replicaNode1.compareTo(activeNodeMap.lastKey())==0){
                replicaNode2 = activeNodeMap.firstKey();
            } else {
                replicaNode2 = activeNodeMap.higherKey(replicaNode1);
            }
            return activeNodeMap.get(replicaNode2);
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return null;
    }
    private synchronized String getNextNode(String key){
        try {
            key = Integer.toString(Integer.parseInt(key)/2);
            String replicaNode1="";
            Log.v("GET-NEXT-NODE","with key:"+key);
            String keyHash = genHash(key);
            Log.v("GET-NEXT-NODE","with keyhASH:"+keyHash);
            if(keyHash.compareTo(activeNodeMap.lastKey())==0){
                replicaNode1 = activeNodeMap.firstKey();
            } else {
                replicaNode1 = activeNodeMap.higherKey(keyHash);
            }
            String value = activeNodeMap.get(replicaNode1);
            //value = Integer.toString(Integer.parseInt(value)*2);
            return value;
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    private synchronized String getPrevNode(String key){
        try {
            key = Integer.toString(Integer.parseInt(key)/2);
            String prev="";
            Log.v("GET-PREV-NODE","with key:"+key);
            String keyHash = genHash(key);
            Log.v("GET-PREV-NODE","with keyHash:"+keyHash);
            if(keyHash.compareTo(activeNodeMap.firstKey())==0){
                prev = activeNodeMap.lastKey();
            } else {
                prev = activeNodeMap.lowerKey(keyHash);
            }
            Log.v("GET-PREV-NODE","with prev:"+prev);
            String value = activeNodeMap.get(prev);
            //value = Integer.toString(Integer.parseInt(value)*2);
            return value;
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    private synchronized JSONArray getNodesListFromKeyHash(String key, String coordNode, MESSAGE_ENUM msgType) throws NoSuchAlgorithmException, JSONException {
        String genKey;
        String currKey;
        String coordKey;
        genKey = genHash(key);
        for(Map.Entry<String,String> entry : activeNodeMap.entrySet()) {
//                Log.v("INSERT","genKey "+genKey);
            currKey = entry.getKey();
            if(currKey.compareTo(activeNodeMap.lastKey())==0){
                coordNode = activeNodeMap.firstKey();
                break;
            }
            coordKey = activeNodeMap.higherKey(currKey);
            if(genKey.compareTo(coordKey)<0 && genKey.compareTo(currKey)>0) {
                coordNode = coordKey;
                break;
            }
        }
        //send to all three avds
        String replicaNode1 = "";
        String replicaNode2 = "";
        //if coordinator is the last key and the replica should be the first two nodes in the TreeMap
        if(coordNode.compareTo(activeNodeMap.lastKey())==0){
            replicaNode1 = activeNodeMap.firstKey();
        } else {
            replicaNode1 = activeNodeMap.higherKey(coordNode);
        }
        //if the coordinator is the second last key and the last replica should be the first node in the TreeMap
        if(replicaNode1.compareTo(activeNodeMap.lastKey())==0){
            replicaNode2 = activeNodeMap.firstKey();
        } else {
            replicaNode2 = activeNodeMap.higherKey(replicaNode1);
        }
        JSONArray nodesToSend = new JSONArray();
        nodesToSend.put(2,activeNodeMap.get(replicaNode2));
        nodesToSend.put(1,activeNodeMap.get(replicaNode1));
        nodesToSend.put(0,activeNodeMap.get(coordNode));
        return nodesToSend;
    }

    @Override
    public boolean onCreate() {
        recoveryLock = new Semaphore(1, true);
        context = getContext();
        dynamoDBHelper = new DynamoDBHelper(getContext(), null, null, 0);
        sqLiteDatabase = dynamoDBHelper.getWritableDatabase();
        chordRingComparator = new Comparator<String>() {
            public int compare(String hash1, String hash2) {
                return hash1.compareTo(hash2);
            }
        };
        activeNodeMap = new TreeMap<String, String>(chordRingComparator);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        setNodeRingMap();
        Log.v("my port",myPort);
        for (Map.Entry<String, String> entry : activeNodeMap.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ". Value: " + entry.getValue());
            if(entry.getValue().compareTo(myPort)==0){
                if(activeNodeMap.firstKey().compareTo(entry.getKey())==0){
                    pred=activeNodeMap.get(activeNodeMap.lastKey());
                } else {
                    pred=activeNodeMap.get(activeNodeMap.lowerKey(entry.getKey()));
                }
                if(activeNodeMap.lastKey().compareTo(entry.getKey())==0){
                    succ=activeNodeMap.get(activeNodeMap.firstKey());
                } else {
                    succ=activeNodeMap.get(activeNodeMap.higherKey(entry.getKey()));
                }
                System.out.println("pred: " + pred);
                System.out.println("succ: " + succ);
            }
        }

        try {
            recoveryLock.acquire();
            new NodeRecoveryOnCreateTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (SocketException e) {
            Log.e("On Create", "Can't create a ServerSocket");
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            Log.e("On Create", "Can't create a ServerSocket");
            e.printStackTrace();
            return false;
        }



        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Log.v("Query DB","Selection: "+selection);
        try {
            boolean isFirstNode = false;
            if (selection.compareTo("@") == 0) {
                recoveryLock.acquire();
                Cursor cursorAt =  dynamoDBHelper.queryDBHelperAllKey(sqLiteDatabase);
                recoveryLock.release();
                return cursorAt;
            } else if (selection.compareTo("*") == 0) {
                try {
                    JSONArray nodeList = getAllNodesListExceptCurr(myPort);
                    String msgToSend = createMsgForQuery(selection, myPort,nodeList);
                    MatrixCursor cursorFinal = new MatrixCursor(new String[] {CONTENT_KEY,CONTENT_VALUE});

                    String response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,msgToSend,nodeList.getString(nodeList.length()-1)).get();
                    nodeList.remove(nodeList.length()-1);
                    msgToSend = createMsgForQuery(selection, myPort,nodeList);
                    if(response.equalsIgnoreCase(FAILURE)){
                        response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,nodeList.getString(nodeList.length()-1)).get();
                    }
                    Log.v("QUERY ALL DB","Response received for * "+response);
                    Cursor cursor = getCursorFromJSONArray(response);
                    cursor.moveToFirst();
                    while (!cursor.isAfterLast()) {
                        Object[] values = {cursor.getString(cursor.getColumnIndex(CONTENT_KEY)), cursor.getString(cursor.getColumnIndex(CONTENT_VALUE))};
                        cursorFinal.addRow(values);
                        cursor.moveToNext();
                    }
                    cursor.close();
                    Cursor myCursor;
                    recoveryLock.acquire();
                    myCursor = dynamoDBHelper.queryDBHelperAllKey(sqLiteDatabase);
                    recoveryLock.release();
                    myCursor.moveToFirst();
                    while (!myCursor.isAfterLast()) {
                        Object[] values = {myCursor.getString(myCursor.getColumnIndex(CONTENT_KEY)), myCursor.getString(myCursor.getColumnIndex(CONTENT_VALUE))};
                        cursorFinal.addRow(values);
                        myCursor.moveToNext();
                    }
                    myCursor.close();
                    return cursorFinal;
                } catch (JSONException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    JSONArray nodesToSend = getNodesListFromKeyHash(selection, "", MESSAGE_ENUM.QUERY);
                    Log.v("DB | QUERY KEY","at nodes: "+nodesToSend.toString());
                    String msgToSend = createMsgForQuery(selection, myPort,nodesToSend);
                    String port = getQueryNode(selection);
                    Log.v("DB | QUERY KEY","sending to node: "+port);
                    if(nodesToSend.toString().contains(myPort)){
                        return dynamoDBHelper.queryDBHelperSingleKey(sqLiteDatabase, selection);
                    }
                    String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, port).get();
                    Log.v("DB | QUERY KEY","got message from node: "+port+" response:"+resp);
                    if (resp.equals(FAILURE)) {
                        //nodesToSend.remove(nodesToSend.length()-1);
                        String replica1Port = getPrevNode(port);
                        resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, replica1Port).get();
                        //nodesToSend.remove(nodesToSend.length()-1);
                        if (resp.equals(FAILURE)) {
                            String replica2Port = getPrevNode(replica1Port);
                            resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, replica2Port).get();
                            Log.d("DB | QUERY KEY", selection + " TO " + port);
                            //return getCursorFromJSONArray(resp);
                        } else if (resp.equals(FAILURE)) {
//                              String replica2Port = getPrevNode(replica1Port);
                                resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, port).get();
                                Log.d("DB | QUERY KEY", selection + " TO " + port);
                                return getCursorFromJSONArray(resp);
                        }
                        Log.d("DB | QUERY KEY", selection + " TO " + port);
                        return getCursorFromJSONArray(resp);


                    } else {
                        Log.d("DB | QUERY KEY", selection + " TO " + port);
                        return getCursorFromJSONArray(resp);
                    }
                    //return getCursorFromJSONArray(resp);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    Log.e("QUERY KEY",selection);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private synchronized JSONArray getAllNodesListExceptCurr(String myPort) throws JSONException {
        int i=0;
        JSONArray nodesToSend = new JSONArray();
        for(Map.Entry<String,String> entry:activeNodeMap.entrySet()){
            if(myPort.equalsIgnoreCase(entry.getValue())){
                continue;
            }
            nodesToSend.put(i,entry.getValue());
            i++;
        }
        return nodesToSend;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private synchronized String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private synchronized void setNodeRingMap(){
        try {
            for (int i = 5554; i < 5564; i = i + 2) {
                String nodeI = Integer.toString(i);
                String hashI = genHash(nodeI);
                activeNodeMap.put(hashI,Integer.toString(i*2));
            }
        } catch (NoSuchAlgorithmException e){
            Log.e("Set Tree Map",e.getMessage());
            e.printStackTrace();
        }
    }
    private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {
        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            Log.v("Server","Server AsyncTask started");
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String s = dataInputStream.readUTF();

                    JSONObject msg = new JSONObject(s);
                    MESSAGE_ENUM message_type = MESSAGE_ENUM.valueOf(msg.getString(MESSAGE_TYPE));
                    recoveryLock.acquire();
                    switch (message_type) {
                        case INSERT:
                            String response = insertServer(msg, dataOutputStream);
                            Log.v("INSERT | Server Task","Response received from insertServer: "+response+" for message: "+s);
                            dataOutputStream.writeUTF(response);
                            socket.close();
                            break;
                        case QUERY:
                            Log.v("QUERY | Server",s);
                            String responseQ = queryServer(msg);
                            Log.v("QUERY | SERVER"," query for "+s+" with response:" +responseQ);
                            dataOutputStream.writeUTF(responseQ);
                            socket.close();
                            Log.v("QUERY | Server","Done");
                            break;
                        case DELETE:
                            Log.v("DELETE | Server",s);
                            int x = deleteServer(msg);
                            dataOutputStream.writeUTF(Integer.toString(x));
                            socket.close();
                            Log.v("DELETE | Server","Done");
                            break;
                        case RECOVERY:
                            Log.v("RECOVERY | Server",s);
                            Cursor cursorRec;
                            //recoveryLock.acquire();
                            cursorRec= dynamoDBHelper.queryDBHelperAllKey(sqLiteDatabase);
                            //recoveryLock.release();
                            JSONObject jsonResponse = new JSONObject();
                            JSONArray keyArray = new JSONArray();
                            JSONArray valueArray = new JSONArray();
                            cursorRec.moveToFirst();
                            if (cursorRec.getCount() > 0) {
                                int j = 0;
                                while (!cursorRec.isAfterLast()) {
                                    keyArray.put(j, cursorRec.getString(cursorRec.getColumnIndex(CONTENT_KEY)));
                                    valueArray.put(j, cursorRec.getString(cursorRec.getColumnIndex(CONTENT_VALUE)));
                                    j++;
                                    cursorRec.moveToNext();
                                }
                                jsonResponse.put(CONTENT_KEY, keyArray);
                                jsonResponse.put(CONTENT_VALUE, valueArray);
                                cursorRec.close();
                            }
                            dataOutputStream.writeUTF(jsonResponse.toString());
                            Log.v("RECOVERY | SERVER"," query for "+s+" with response:" +jsonResponse.toString());
                            socket.close();
                            Log.v("RECOVERY | Server","Done");
                            break;
                    }
                    //socket.close();
                    recoveryLock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    Log.e("Server Task", "Outmost catch Exception | " + String.valueOf(e));
                }
            }
        }
    }
    private class ClientTask extends AsyncTask<String, Void, String> {
        @Override
        protected String doInBackground(String... msg) {
            String request = msg[0];
            String remotePort = msg[1];
            Log.v("CLIENT","Request "+request+" for port "+remotePort);
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                // https://developer.android.com/reference/java/io/DataOutputStream
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF(request);
                dataOutputStream.flush();
                String resp = "";
                JSONObject jsonObject = new JSONObject(request);
                //if(MESSAGE_ENUM.valueOf(jsonObject.getString(MESSAGE_TYPE)).compareTo(MESSAGE_ENUM.QUERY)==0) {
                    DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                    if(MESSAGE_ENUM.valueOf(jsonObject.getString(MESSAGE_TYPE)).compareTo(MESSAGE_ENUM.QUERY)==0){
                        socket.setSoTimeout(QUERY_TIMEOUT);
                    } else {
                        socket.setSoTimeout(5000);
                    }
                    resp = dataInputStream.readUTF();
                //}
                socket.close();
                Log.v("CLIENT","Response received for "+request+" Response: "+resp);
                return resp;
            } catch (SocketTimeoutException e) {
                e.printStackTrace();
                Log.e("CLIENT TASK Failed","port: "+remotePort+" message "+request);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                Log.d("IOEXCEPTION", "Timeout on " + remotePort);
                e.printStackTrace();
            } catch (JSONException e){
                Log.d("JSONException", "Timeout on " + remotePort);
                e.printStackTrace();
            }

            return FAILURE;
        }
    }

    private synchronized String createMsgForInsert(String key, String value, INSERT_ENUM insert_enum, JSONArray nodesToSend) {
        JSONObject jsonMsg = new JSONObject();
        try{
            jsonMsg.put(MESSAGE_TYPE, MESSAGE_ENUM.INSERT);
            jsonMsg.put(CONTENT_KEY,key);
            jsonMsg.put(CONTENT_VALUE, value);
            jsonMsg.put(INSERT_TYPE, insert_enum);
            jsonMsg.put(NODES_LIST, nodesToSend);
        }catch (JSONException e) {
            Log.v("Create Insert Message",e.getMessage());
            e.printStackTrace();
        }
        return jsonMsg.toString();
    }

    private synchronized MatrixCursor getCursorFromJSONArray(String msgReceived) throws JSONException {
        JSONObject jsonReceived = new JSONObject(msgReceived);
        JSONArray keyArray = jsonReceived.getJSONArray(CONTENT_KEY);
        JSONArray valueArray = jsonReceived.getJSONArray(CONTENT_VALUE);
        MatrixCursor cursorFinal = new MatrixCursor(new String[] {CONTENT_KEY,CONTENT_VALUE});
        int i=0;
        while(i<keyArray.length()){
            cursorFinal.addRow(new Object[]{keyArray.getString(i), valueArray.getString(i)});
            i++;
        }
        cursorFinal.close();
        return cursorFinal;
    }

    private synchronized String createMsgForQuery(String selection, String hostPort, JSONArray nodeList) {
        JSONObject jsonMsg = new JSONObject();
        try{
            jsonMsg.put(MESSAGE_TYPE, MESSAGE_ENUM.QUERY);
            jsonMsg.put(CONTENT_KEY,selection);
            jsonMsg.put(NODES_LIST,nodeList);
            jsonMsg.put(HOST_PORT,hostPort);
        }catch (JSONException e) {
            Log.v("Create Query Message",e.getMessage());
            e.printStackTrace();
        }
        return jsonMsg.toString();
    }

    private synchronized String createMsgForDelete(DELETE_ENUM coord, String selection, String myPort, JSONArray nodeList) {
        JSONObject jsonMsg = new JSONObject();
        try{
            jsonMsg.put(MESSAGE_TYPE, MESSAGE_ENUM.DELETE);
            jsonMsg.put(CONTENT_KEY,selection);
            jsonMsg.put(HOST_PORT, myPort);
            jsonMsg.put(NODES_LIST, nodeList);
            jsonMsg.put(DELETE_TYPE, coord);
        }catch (JSONException e) {
            Log.v("Create Delete Message",e.getMessage());
            e.printStackTrace();
        }
        return jsonMsg.toString();
    }

    private synchronized String insertServer(JSONObject msg, DataOutputStream dataOutputStream) {
        try {
            //recoveryLock.acquire();
            ContentValues values = new ContentValues();
            values.put(CONTENT_KEY, msg.getString(CONTENT_KEY));
            values.put(CONTENT_VALUE, msg.getString(CONTENT_VALUE));
            dynamoDBHelper.insertDBHelper(sqLiteDatabase,values);
            Log.v("Insert Server","inserted in "+myPort + " key"+msg.getString(CONTENT_KEY));
            INSERT_ENUM insertType = INSERT_ENUM.valueOf(msg.getString(INSERT_TYPE));
            Log.v("INSERT | DB","came with "+" as me key "+msg.getString(CONTENT_KEY)+" with value"+values.get(CONTENT_VALUE).toString());
            JSONArray nodeList = msg.getJSONArray(NODES_LIST);
            String response = FAILURE;
            //while(response.equalsIgnoreCase("Failure")){
            switch(insertType){
                case COORD:
                    msg.put(INSERT_TYPE,INSERT_ENUM.REPLICA1);
                    response = sendMessage(msg, Integer.parseInt(succ), DEFAULT_TIMEOUT);
                    Log.v("Insert Server","Inserted in coord and send to Replica 1 wiht response: "+response);
                    /*if(response.equalsIgnoreCase("Failure")){
                        msg.put(INSERT_TYPE,INSERT_ENUM.REPLICA2);
                        response = sendMessage(msg, Integer.parseInt(getNextNode(succ)));
                    }*/
                    break;
                case REPLICA1:
                    msg.put(INSERT_TYPE,INSERT_ENUM.REPLICA2);
                    response = sendMessage(msg, Integer.parseInt(succ), DEFAULT_TIMEOUT);
                    //if(response.equalsIgnoreCase("Failure")){
                        //response = "Success";
                    //}
                    Log.v("Insert Server","Inserted in replica1 and send to Replica 2 with response: "+response);
                    break;
                case REPLICA2:
                    response = SUCCESS;
                    Log.v("Insert Server","Inserted in replica2 with response: "+response);
                    break;
            }

            if(response.equalsIgnoreCase(FAILURE)){
                insertType = INSERT_ENUM.valueOf(msg.getString(INSERT_TYPE));
                switch(insertType){
                    case REPLICA1:
                        msg.put(INSERT_TYPE,INSERT_ENUM.REPLICA2);
                        response = sendMessage(msg, Integer.parseInt(getNextNode(succ)), DEFAULT_TIMEOUT);
                        if(response.equalsIgnoreCase(FAILURE)){
                            response = sendMessage(msg, Integer.parseInt(succ), DEFAULT_TIMEOUT);
                        }
                        Log.v("Insert Server","Inserted in replica1 and send to Replica 2 with response: "+response);
                        break;
                    case REPLICA2:
                        response = SUCCESS;
                        Log.v("Insert Server","Inserted in replica2 with response: "+response);
                        break;
                }

            }
            //}
            //recoveryLock.release();
            return response;
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
        //recoveryLock.release();
        return FAILURE;
    }

    private synchronized String queryServer(JSONObject msg) {
        try {
            //recoveryLock.acquire();
            String selection = msg.getString(CONTENT_KEY);
            if (!selection.equalsIgnoreCase("*")) {
                Cursor cursor = dynamoDBHelper.queryDBHelperSingleKey(sqLiteDatabase,selection);
                cursor.moveToFirst();
                String resp ="";
                if (cursor.getCount() > 0){
                    int j = 0;
                    JSONObject jsonResponse = new JSONObject();
                    JSONArray keyArray = new JSONArray();
                    JSONArray valueArray = new JSONArray();
                    while (!cursor.isAfterLast()) {
                        keyArray.put(j, cursor.getString(cursor.getColumnIndex(CONTENT_KEY)));
                        valueArray.put(j, cursor.getString(cursor.getColumnIndex(CONTENT_VALUE)));
                        j++;
                        cursor.moveToNext();
                    }
                    jsonResponse.put(CONTENT_KEY, keyArray);
                    jsonResponse.put(CONTENT_VALUE, valueArray);
                    cursor.close();
                    //recoveryLock.release();
                    return jsonResponse.toString();

                } else {
                    //Fetch from predecessor or predecessor2!!
                    //MessageRequest request = new MessageRequest("Query", Integer.toString(myPort), key);
                    //JSONArray nodeList = msg.getJSONArray(NODES_LIST);
                    //nodeList.remove(nodeList.length()-1);
                    //msg.put(NODES_LIST,nodeList);
                    resp = sendMessage(msg, Integer.parseInt(pred), QUERY_TIMEOUT);
                    if (resp.equals(FAILURE)) {
                        resp = sendMessage(msg, Integer.parseInt(getPrevNode(pred)), QUERY_TIMEOUT);
                    }
                    //recoveryLock.release();
                    return resp;
                }
                //cursor.close();
                ////recoveryLock.release();
            } else {
                //* Query received.
                JSONArray nodeList = msg.getJSONArray(NODES_LIST);
                String resp = null;
                nodeList.remove(nodeList.length()-1);
                if(nodeList.length()!=0){
                    msg.put(NODES_LIST, nodeList);
                    resp = sendMessage(msg,Integer.parseInt(nodeList.getString(nodeList.length()-1)), QUERY_TIMEOUT);
                    if(resp.equalsIgnoreCase(FAILURE)){
                        nodeList.remove(nodeList.length()-1);
                        msg.put(NODES_LIST, nodeList);
                        resp = sendMessage(msg,Integer.parseInt(nodeList.getString(nodeList.length()-1)), QUERY_TIMEOUT);
                    }
                }
                Cursor cursorAllKey = dynamoDBHelper.queryDBHelperAllKey(sqLiteDatabase);
                cursorAllKey.moveToFirst();

                JSONArray kArray = new JSONArray();
                JSONArray vArray = new JSONArray();
                int i = 0;
                if (resp != null) {
                    try {
                        JSONObject jsonObject = new JSONObject(resp);
                        kArray = jsonObject.getJSONArray(CONTENT_KEY);
                        vArray = jsonObject.getJSONArray(CONTENT_VALUE);
                        i = kArray.length();
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
                while (!cursorAllKey.isAfterLast()) {
                    try {
                        kArray.put(i, cursorAllKey.getString(0));
                        vArray.put(i, cursorAllKey.getString(1));
                        i++;
                        cursorAllKey.moveToNext();
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }

                }
                JSONObject response = new JSONObject();
                try {
                    response.put(CONTENT_KEY, kArray);
                    response.put(CONTENT_VALUE, vArray);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                //recoveryLock.release();
                return response.toString();
            }
        }catch (JSONException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //recoveryLock.release();
        return "";
    }

    private synchronized int deleteServer(JSONObject msg) {
        try {
            //recoveryLock.acquire();
            if (!msg.getString(CONTENT_KEY).equals("*")) {
                dynamoDBHelper.deleteDBHelperKey(sqLiteDatabase, msg.getString(CONTENT_KEY));
                DELETE_ENUM deleteType = DELETE_ENUM.valueOf(msg.getString(DELETE_TYPE));
                JSONArray nodeList = msg.getJSONArray(NODES_LIST);
                if (!deleteType.equals(DELETE_ENUM.REPLICA2)) {
                    if (deleteType.equals(DELETE_ENUM.COORD)) {
                        msg.put(DELETE_TYPE,DELETE_ENUM.REPLICA1);
                    } else {
                        msg.put(DELETE_TYPE,DELETE_ENUM.REPLICA2);
                    }
                    //nodeList.remove(nodeList.length()-1);
                    String resp = sendMessage(msg, Integer.parseInt(succ), DEFAULT_TIMEOUT);
                    if (resp.equals(FAILURE)) {
                        if (deleteType.equals(DELETE_ENUM.REPLICA1)) {
                            msg.put(DELETE_TYPE,DELETE_ENUM.REPLICA2);
                            //nodeList.remove(nodeList.length()-1);
                            resp = sendMessage(msg, Integer.parseInt(getNextNode(succ)), DEFAULT_TIMEOUT);
                            if (resp.equals(FAILURE)) {
                                msg.put(DELETE_TYPE,DELETE_ENUM.REPLICA1);
                                resp = sendMessage(msg, Integer.parseInt(succ), DEFAULT_TIMEOUT);
                            }
                        }
                    }
                }
                //recoveryLock.release();
                return 1;
            } else {
                Log.v("DELETE FN","* PORT "+myPort);
                dynamoDBHelper.deleteDBHelperAllKey(sqLiteDatabase);
                JSONArray nodeList = msg.getJSONArray(NODES_LIST);
                nodeList.remove(nodeList.length()-1);
                if (nodeList.length()!=0) {
                    msg.put(NODES_LIST,nodeList);
                    String resp = sendMessage(msg, Integer.parseInt(nodeList.getString(nodeList.length()-1)), DEFAULT_TIMEOUT);
                    if (resp.equals(FAILURE)) {
                        nodeList.remove(nodeList.length()-1);
                        msg.put(NODES_LIST,nodeList);
                        if (nodeList.length()!=0) {
                            resp = sendMessage(msg, Integer.parseInt(nodeList.getString(nodeList.length()-1)), DEFAULT_TIMEOUT);
                        }
                    }
                }
                Log.v("DELETE FN","* END");
                //recoveryLock.release();
                return 1;
            }
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //recoveryLock.release();
        return 0;
    }

    private synchronized String sendMessage(JSONObject jsonObject, int port, int timeout) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    port);
            // https://developer.android.com/reference/java/io/DataOutputStream
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeUTF(jsonObject.toString());
            dataOutputStream.flush();
            String resp = "";
            //if(MESSAGE_ENUM.valueOf(jsonObject.getString(MESSAGE_TYPE)).compareTo(MESSAGE_ENUM.QUERY)==0) {
                DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                socket.setSoTimeout(timeout);
                resp = dataInputStream.readUTF();
            //}
            Log.v("Send Message","Send msg for: "+jsonObject.toString()+" with response: "+resp);
            socket.close();
            return resp;
        } catch (SocketTimeoutException e) {
            e.printStackTrace();
            Log.e("SendMessage Failed","port: "+Integer.toString(port)+" message "+jsonObject.toString());
        } catch (IOException e) {
            e.printStackTrace();
            Log.e("SendMessage Failed","port: "+Integer.toString(port)+" message "+jsonObject.toString());
        }
        return FAILURE;
    }

    private class NodeRecoveryOnCreateTask extends AsyncTask<Void, Void, Void> {

        @Override
        protected Void doInBackground(Void... voids) {
            try {
                //Reference : https://developer.android.com/training/data-storage/shared-preferences
                SharedPreferences preferences = context.getSharedPreferences("DynamoPref", Context.MODE_PRIVATE);
                if (!preferences.contains("FirstRun")) {
                    SharedPreferences.Editor editor = preferences.edit();
                    editor.putInt("FirstRun", 0);
                    editor.commit();
                    Log.d("RECOVERY", "No need");
                    recoveryLock.release();
                    return null;
                }
                Log.d("RECOVERY", "Fetching from Others!");
                JSONObject recoveryQuery = new JSONObject();
                recoveryQuery.put(MESSAGE_TYPE,MESSAGE_ENUM.RECOVERY);
                //Send to predecessor and successor
                String predResp = ""; String succResp = "";
                predResp = sendMessage(recoveryQuery, Integer.parseInt(pred), DEFAULT_TIMEOUT);
                succResp = sendMessage(recoveryQuery, Integer.parseInt(succ), DEFAULT_TIMEOUT);
                if (predResp.equals(FAILURE)) {
                    predResp = sendMessage(recoveryQuery, Integer.parseInt(pred), DEFAULT_TIMEOUT);
                }
                if (succResp.equals(FAILURE)) {
                    succResp = sendMessage(recoveryQuery, Integer.parseInt(succ), DEFAULT_TIMEOUT);
                }
                Log.v("RECOVERY","pred resp and succ resp "+predResp.toString()+" "+succResp.toString());
                JSONObject predJSONObj = new JSONObject(predResp);
                JSONArray predKeyArr = predJSONObj.getJSONArray(CONTENT_KEY);
                JSONArray predValArr = predJSONObj.getJSONArray(CONTENT_VALUE);

                JSONObject succJSONObj = new JSONObject(succResp);
                JSONArray succKeyArr = succJSONObj.getJSONArray(CONTENT_KEY);
                JSONArray succValArr = succJSONObj.getJSONArray(CONTENT_VALUE);

                String key="",val="",keyHash="";
                Log.v("RECOVERY","Getting all ports of pred:"+pred);
                String prevNode1 = getPrevNode(pred);
                Log.v("RECOVERY","PREV-NODE-1:"+prevNode1);
                String prevNode2 = getPrevNode(prevNode1);
                Log.v("RECOVERY","PREV-NODE-2:"+prevNode1);
                String prevNode3 = getPrevNode(prevNode2);
                Log.v("RECOVERY","PREV-NODE-3:"+prevNode1);

                int predI = 0;
                while(predI<predKeyArr.length()){
                    key = predKeyArr.getString(predI);
                    val = predValArr.getString(predI);
                    //keyHash = genHash(key);
                    JSONArray nodes = getNodesListFromKeyHash(key,"",MESSAGE_ENUM.RECOVERY);
                    int nodeI = 0;
                    while(nodeI<nodes.length()){
                        if(myPort.equalsIgnoreCase(nodes.getString(nodeI))){
                            ContentValues contentValues = new ContentValues();
                            contentValues.put(CONTENT_KEY, key);
                            contentValues.put(CONTENT_VALUE, val);
                            Log.v("RECOVERY","Adding key "+key+" with value"+val+" in port"+myPort);
                            dynamoDBHelper.insertDBHelper(sqLiteDatabase,contentValues);
                        }
                        nodeI++;
                    }
                    predI++;
                }

                int succI = 0;
                while(succI<succKeyArr.length()){
                    key = succKeyArr.getString(succI);
                    val = succValArr.getString(succI);
                    JSONArray nodes = getNodesListFromKeyHash(key,"",MESSAGE_ENUM.RECOVERY);
                    int nodeI = 0;
                    while(nodeI<nodes.length()){
                        if(myPort.equalsIgnoreCase(nodes.getString(nodeI))){
                            ContentValues contentValues = new ContentValues();
                            contentValues.put(CONTENT_KEY, key);
                            contentValues.put(CONTENT_VALUE, val);
                            Log.v("RECOVERY","Adding key "+key+" with value"+val+" in port"+myPort);
                            dynamoDBHelper.insertDBHelper(sqLiteDatabase,contentValues);
                        }
                        nodeI++;
                    }
                    succI++;
                }
            } catch (JSONException e) {
                e.printStackTrace();
                Log.e("RECOVERY","Failed wile creating query msg");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                Log.e("RECOVERY","Failed wile creating query msg");
            } catch (Exception e) {
                e.printStackTrace();
                Log.e("RECOVERY","Failed wile creating query msg");
            }
            Log.v("","");
            recoveryLock.release();
            return null;

        }
    }
}

