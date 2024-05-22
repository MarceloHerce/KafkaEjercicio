package org.example;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringSerializer;
import java.lang.invoke.MethodHandles;
import java.sql.Timestamp;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.example.model.Camion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(final String[] args) throws IOException {
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final String topic = "reparto";

        String[] matriculas = {"E141111EEE", "E141234QWE", "E143452FGR", "E149876WWW", "E143456LOL"};
        List<Camion> items = new ArrayList<>();
        final Producer<String, String> producer = new KafkaProducer<>(props);

        final Random rnd = new Random();
        final Long numRegistros = 10L;

        for (Long i = 0L; i < matriculas.length; i++) {
            float km = 10+rnd.nextFloat()*(20-10);
            Date date = new Date();
            Camion camion = new Camion(matriculas[i.intValue()],km,new Timestamp(date.getTime()));
            items.add(camion);
        }

        for (Long i = 0L; i < numRegistros; i++) {
            for (Long j = 0L; j < items.size(); j++) {
                float km = 1.3f;
                Date date = new Date();
                Camion camion = items.get(j.intValue());
                Timestamp fecha = new Timestamp(date.getTime());
                fecha.setTime(1);
                camion.setFecha(fecha);
                String matricula = camion.getMatricula();

                ObjectMapper objectMapper = new ObjectMapper();
                String item = objectMapper.writeValueAsString(camion);

                producer.send(
                        new ProducerRecord<>(topic, matricula, item),
                        (event, ex) -> {getFutureRecordMetadata(matricula, item, event, ex);}
                );

                camion.setKmActual(camion.getKmActual() +km);
            }
        }
        System.out.printf("%s events were produced to topic %s%n", numRegistros, topic);

        producer.close();

    }

    public static void getFutureRecordMetadata(String user, String item,RecordMetadata metadata, Exception exception) {
        if (exception != null)
            exception.printStackTrace();
        else
            System.out.printf("Produced event to topic %s: user= %-10s value = %-20s partition=%d%n",
                    metadata.topic(), user, item, metadata.partition());
    }


}