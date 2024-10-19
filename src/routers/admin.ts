import { Request, Response, Router } from "express";
import db from "../db";

const router = Router();

router.get("/reservations", async (req: Request, res: Response) => {
  try {
    const reservations = await db.query(`SELECT * FROM reservations`);

    res.status(200).json(reservations.rows);
  } catch (err) {
    res
      .status(500)
      .json({ message: "Error fetching reservations", error: err });
  }
});

export default router;
