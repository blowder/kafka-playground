mvn exec:java -Dexec.mainClass="com.blowder.kafka.RandNumberConsumer" -Dexec.args="0" &
mvn exec:java -Dexec.mainClass="com.blowder.kafka.RandNumberConsumer" -Dexec.args="1" &
mvn exec:java -Dexec.mainClass="com.blowder.kafka.RandNumberConsumer" -Dexec.args="2" &
mvn exec:java -Dexec.mainClass="com.blowder.kafka.RandNumberConsumer" -Dexec.args="3" &
mvn exec:java -Dexec.mainClass="com.blowder.kafka.RandNumberConsumer" -Dexec.args="4" &


for job in `jobs -p`
do
echo $job
    wait $job
done
