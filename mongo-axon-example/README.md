# Mongo Axon Springboot Example

This is an example SpringBoot application using the Mongo Axon extension. It uses Mongo as the Event- and Token Store.

> Although Mongodb might be a good fit for it, we have encountered some inefficiencies in regards to the Mongo Event Store implementation that you can read more about [here](https://docs.axoniq.io/reference-guide/extensions/mongo)

## How to run

### Preparation

You will need `docker` and `docker-compose` to run this example.

Please run:

```bash
docker-compose -f ./mongo-axon-example/docker-compose.yaml up -d
```

This will start Mongo with default values.

Now build the application by running:

```bash
mvn clean package -f ./mongo-axon-example
```

### Running example application

You can start the application by running `java -jar ./mongo-axon-example/target/mongo-axon-example.jar`.

You can access the mongo-express UI on [http://localhost:8081/db/axonframework/](http://localhost:8081/db/axonframework/)
where you can see the tables used by axon and inspect events, tokens and snapshots.
