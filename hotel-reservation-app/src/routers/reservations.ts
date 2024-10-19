import { Request, Response, Router } from "express";
import db from "../db";
import {
  IReservationEvent,
  KafkaReservationEvents,
  KafkaProducer,
  KafkaTopics,
} from "../kafka";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  try {
    console.log("req.headers", req.headers);
    const userId = req.headers["user-id"]; // token
    console.log("userId", userId);

    const userReservations = await db.query(
      `SELECT * FROM reservations WHERE user_id = $1`,
      [userId]
    );

    res.status(200).json(userReservations.rows);
  } catch (err) {
    res
      .status(500)
      .json({ message: "Error fetching reservations", error: err });
  }
});

router.post("/", async (req: Request, res: Response) => {
  const { roomId, userId, startDate, endDate } = req.body;

  try {
    const checkRoomAvailability = await db.query(
      `SELECT * FROM reservations WHERE room_id = $1 AND (start_date, end_date) OVERLAPS ($2, $3)`,
      [roomId, startDate, endDate]
    );

    if (checkRoomAvailability.rows.length > 0) {
      res
        .status(400)
        .json({ message: "Room is not available for the selected dates." });
      return;
    }

    const newReservation = await db.query(
      `INSERT INTO reservations (room_id, user_id, start_date, end_date) VALUES ($1, $2, $3, $4) RETURNING *`,
      [roomId, userId, startDate, endDate]
    );

    const room = await db.query(`SELECT * FROM rooms WHERE id = $1`, [roomId]);

    const event: IReservationEvent = {
      type: KafkaReservationEvents.CreateReservation,
      roomId,
      roomType: room.rows[0].category,
      floor: room.rows[0].room_number.substring(
        0,
        room.rows[0].room_number.length - 2
      ),
      timestamp: Date.now(),
    };

    const producer = KafkaProducer.Instance;

    producer.sendMessage(KafkaTopics.ReservationEvents, event);

    res.status(201).json(newReservation.rows[0]);
  } catch (err) {
    res.status(500).json({ message: "Error creating reservation", error: err });
  }
});

router.put("/:id", async (req: Request, res: Response) => {
  const { roomId, startDate, endDate } = req.body;
  const { id } = req.params;

  try {
    const checkRoomAvailability = await db.query(
      `SELECT * FROM reservations WHERE room_id = $1 AND (start_date, end_date) OVERLAPS ($2, $3)`,
      [roomId, startDate, endDate]
    );

    if (checkRoomAvailability.rows.length > 0) {
      res
        .status(400)
        .json({ message: "Room is not available for the selected dates." });
      return;
    }

    const updatedReservation = await db.query(
      `UPDATE reservations SET room_id = $1, start_date = $2, end_date = $3 WHERE id = $4 RETURNING *`,
      [roomId, startDate, endDate, id]
    );

    const room = await db.query(`SELECT * FROM rooms WHERE id = $1`, [roomId]);

    const event: IReservationEvent = {
      type: KafkaReservationEvents.UpdateReservation,
      roomId,
      roomType: room.rows[0].category,
      floor: room.rows[0].room_number.substring(
        0,
        room.rows[0].room_number.length - 2
      ),
      startDate,
      endDate,
      timestamp: Date.now(),
    };

    const producer = KafkaProducer.Instance;

    producer.sendMessage(KafkaTopics.ReservationEvents, event);

    res.status(200).json(updatedReservation.rows[0]);
  } catch (err) {
    res.status(500).json({ message: "Error updating reservation", error: err });
  }
});

router.delete("/:id", async (req: Request, res: Response) => {
  const { id } = req.params;

  try {
    const deletedReservation = await db.query(
      `DELETE FROM reservations WHERE id = $1 RETURNING *`,
      [id]
    );

    const roomId = deletedReservation.rows[0].room_id;
    const room = await db.query(`SELECT * FROM rooms WHERE id = $1`, [roomId]);

    const event: IReservationEvent = {
      type: KafkaReservationEvents.DeleteReservation,
      roomId,
      roomType: room.rows[0].category,
      floor: room.rows[0].room_number.substring(
        0,
        room.rows[0].room_number.length - 2
      ),
      timestamp: Date.now(),
    };

    const producer = KafkaProducer.Instance;
    producer.sendMessage(KafkaTopics.ReservationEvents, event);

    res.status(200).json(deletedReservation.rows[0]);
  } catch (err) {
    res.status(500).json({ message: "Error deleting reservation", error: err });
  }
});

export default router;
