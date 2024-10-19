import {
  Kafka,
  logLevel,
  Message,
  Producer,
  ProducerBatch,
  TopicMessages,
} from "kafkajs";

export interface IReservationEvent {
  type: KafkaReservationEvents;
  roomId: number;
  roomType: string;
  floor: string;
  startDate?: string;
  endDate?: string;
  timestamp: number;
}

export enum KafkaTopics {
  ReservationEvents = "reservation-events",
  UserEvents = "user-events",
  RoomEvents = "room-events",
}

export enum KafkaReservationEvents {
  CreateReservation = "CREATE_RESERVATION",
  UpdateReservation = "UPDATE_RESERVATION",
  DeleteReservation = "DELETE_RESERVATION",
}

export class KafkaProducer {
  private producer: Producer;
  private static _instance: KafkaProducer;

  constructor() {
    this.producer = this.createProducer();
  }

  public static get Instance() {
    return this._instance || (this._instance = new this());
  }

  public async start(): Promise<void> {
    try {
      await this.producer.connect();
    } catch (error) {
      console.log("Error connecting the producer: ", error);
    }
  }

  public async shutdown(): Promise<void> {
    await this.producer.disconnect();
  }

  public async sendMessage(
    topic: KafkaTopics,
    message: IReservationEvent
  ): Promise<void> {
    try {
      await this.producer.connect();
      await this.producer.send({
        topic,
        messages: [{ value: JSON.stringify(message) }],
      });
      console.log("Message sent successfully");
    } catch (error) {
      console.log("Error sending message: ", error);
    } finally {
      await this.producer.disconnect();
    }
  }

  public async sendBatch(
    topic: KafkaTopics,
    messages: Array<IReservationEvent>
  ): Promise<void> {
    try {
      await this.producer.connect();
      const kafkaMessages: Array<Message> = messages.map((message) => {
        return {
          value: JSON.stringify(message),
        };
      });

      const topicMessages: TopicMessages = {
        topic,
        messages: kafkaMessages,
      };

      const batch: ProducerBatch = {
        topicMessages: [topicMessages],
      };

      await this.producer.sendBatch(batch);
    } catch (error) {
      console.log("Error sending batch: ", error);
    } finally {
      await this.producer.disconnect();
    }
  }

  private createProducer(): Producer {
    const kafkaHost = process.env.KAFKA_HOST || "kafka";
    const kafkaPort = process.env.KAFKA_PORT || "9092";
    const broker = `${kafkaHost}:${kafkaPort}`;
    console.log("Broker: ", broker);

    const kafka = new Kafka({
      logLevel: logLevel.DEBUG,
      clientId: "hotel-reservation-app-producer",
      brokers: [broker],
    });

    return kafka.producer();
  }
}
