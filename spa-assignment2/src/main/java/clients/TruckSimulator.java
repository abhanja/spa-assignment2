package clients;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;


class Truck {
    private String id;
    private double latitude;
    private double longitude;
    private double speed;
    private Instant previousTimestamp;
    private double previousLatitude;
    private double previousLongitude;
    private Random random;

    public Truck(String id, double latitude, double longitude, double speed) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speed = speed;
        this.previousTimestamp = Instant.now();
        this.previousLatitude = latitude;
        this.previousLongitude = longitude;
        this.random = new Random();
    }

    // Getters and setters
    public String getId() {
        return id;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getSpeed() {
        return speed;
    }

    // Simulate truck movement with variability
    public void move() {
        double latitudeChange = random.nextGaussian() * 0.0002; // Gaussian distribution for latitude change
        double longitudeChange = random.nextGaussian() * 0.0002; // Gaussian distribution for longitude change
        latitude += latitudeChange;
        longitude += longitudeChange;
    }

    // Calculate speed based on distance traveled and time elapsed
    public double calculateSpeed() {
        Instant currentTimestamp = Instant.now();
        double distance = calculateDistance(previousLatitude, previousLongitude, latitude, longitude);
        long timeElapsedSeconds = currentTimestamp.getEpochSecond() - previousTimestamp.getEpochSecond();
        double speed = (distance / timeElapsedSeconds) * 3600; // Convert to km/h
        // Update previous location and timestamp
        previousLatitude = latitude;
        previousLongitude = longitude;
        previousTimestamp = currentTimestamp;
        return speed;
    }

    // Helper method to calculate distance between two points using Haversine formula
    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        double earthRadius = 6371; // Radius of the Earth in kilometers
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return earthRadius * c;
    }
}




public class TruckSimulator {
    public static void main(String[] args) throws MqttException {
    	
        String broker = "tcp://localhost:1883"; // MQTT broker address
        String clientId = "JavaPublisher"; // Unique client ID
        String topic = "test/topic"; // MQTT topic to publish to
        // Create an MQTT client instance
        MqttClient client = null;
		try {
			client = new MqttClient(broker, clientId);
	        // Connect to the MQTT broker
	        client.connect();
		} catch (MqttException e) {
			e.printStackTrace();
		}

    	
        // Create trucks
        List<Truck> trucks = new ArrayList<>();
        trucks.add(new Truck("Truck1", 40.62057403353648, -74.6466649660757, 60)); // Starting speed: 60 km/h
        trucks.add(new Truck("Truck2", 40.61889720310687, -74.65830522722011, 50)); // Starting speed: 50 km/h
        trucks.add(new Truck("Truck3", 40.61722037286792, -74.66994519623455, 55)); // Starting speed: 55 km/h
        trucks.add(new Truck("Truck4", 40.615543542819644, -74.68158487314368, 65)); // Starting speed: 65 km/h
        trucks.add(new Truck("Truck5", 40.61386671296209, -74.69322425797216, 70)); // Starting speed: 70 km/h
        trucks.add(new Truck("Truck6", 40.61218988329527, -74.70486335074469, 45)); // Starting speed: 45 km/h
        trucks.add(new Truck("Truck7", 40.6115130537192, -74.7165021514865, 55)); // Starting speed: 55 km/h
        trucks.add(new Truck("Truck8", 40.610836224233854, -74.72814065922222, 75)); // Starting speed: 75 km/h
        trucks.add(new Truck("Truck9", 40.61015939483919, -74.73977887297658, 50)); // Starting speed: 50 km/h
        trucks.add(new Truck("Truck10", 40.60948256553524, -74.75141679177428, 65)); // Starting speed: 65 km/h

        // Simulation parameters
        int timeStep = 5; // Time step in seconds

        // Continuous simulation
        while (true) {
            for (Truck truck : trucks) {
                truck.move(); // Simulate truck movement
                double speed = truck.calculateSpeed(); // Calculate truck speed
                // Generate and print JSON data
                String jsonData = generateJSON(truck, speed);
                
                // put to the MQTT broker
                System.out.println(jsonData);
                MqttMessage message = new MqttMessage(jsonData.getBytes());
                // Publish the message to the topic
                try {
					client.publish(topic, message);
				} catch (Exception e) {
					e.printStackTrace();
				}
            }
            try {
                Thread.sleep(timeStep * 1000); // Sleep for the time step duration in milliseconds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static String generateJSON(Truck truck, double speed) {
        String timestamp = Instant.now().toString(); // Get current timestamp in ISO-8601 format
        double minOccupancy = 0;
        double maxOccupancy = 100;
        double occupancy = minOccupancy + Math.random() * (maxOccupancy - minOccupancy);
        return "{" +
                "\"timestamp\": \"" + timestamp + "\"," +
                "\"truck_id\": \"" + truck.getId() + "\"," +
                "\"route_id\": \"1009\"," +
                "\"occupancy\": " + occupancy + "," + // Random occupancy value between 0 and 100
                "\"speed\": " + speed + "," + // Include truck speed
                "\"location\": {" +
                "\"latitude\": " + truck.getLatitude() + "," +
                "\"longitude\": " + truck.getLongitude() +
                "}" +
                "}";
    }
}


