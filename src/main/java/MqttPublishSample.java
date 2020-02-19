import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class MqttPublishSample {
    public static void main(String[] args) {

        String topic        = "track/SDD neural";
        String content      = "Message from MqttPublishSample";
        int qos             = 2;
        String broker       = "tcp://localhost:61616";
        String clientId     = "send";
        MemoryPersistence persistence = new MemoryPersistence();
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        Date date=new Date();


        final List<LatLong> latLongList = new ArrayList<LatLong>();

        latLongList.add(new LatLong(37.881668,41.129881));
        latLongList.add(new LatLong(37.881948,41.130407));
        latLongList.add(new LatLong(37.882549,41.132041));
        latLongList.add(new LatLong(37.883082,41.133264));
        latLongList.add(new LatLong(37.883743,41.134776));
        latLongList.add(new LatLong(37.884691,41.136965));
        latLongList.add(new LatLong(37.885589,41.137770));
        latLongList.add(new LatLong(37.886131,41.136654));
        latLongList.add(new LatLong(37.886656,41.135431));
        latLongList.add(new LatLong(37.887646,41.133575));






        try {
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setUserName("admin");
            connOpts.setPassword("admin".toCharArray());
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: "+broker);
            sampleClient.connect(connOpts);
            System.out.println("Connected");

            for (int i=0;i<latLongList.size();i++){
                Gson gsonBuilder= new GsonBuilder().create();
                Map personMap = new HashMap();
                personMap.put("lat",latLongList.get(i).getLat());
                personMap.put("lon", latLongList.get(i).getLong());
                personMap.put("createdAt",dateFormat.format(date));
                String jsonFromJavaMap = gsonBuilder.toJson(personMap);
                System.out.println("json -- "+jsonFromJavaMap);
                MqttMessage message = new MqttMessage(jsonFromJavaMap.getBytes());
                message.setQos(qos);
                sampleClient.publish(topic, message);
                System.out.println("Message published");
            }



            sampleClient.disconnect();
            System.out.println("Disconnected");
            System.exit(0);
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
    }
}
