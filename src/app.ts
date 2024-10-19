import express, { Request, Response } from "express";
import db from "./db";
import reservationsRouter from "./routers/reservations";
import adminRouter from "./routers/admin";

const app = express();
app.use(express.json());

app.get("/healthcheck", async (req: Request, res: Response) => {
  try {
    const result = await db.query("SELECT NOW()");
    res.json({ message: "Healthy!", db: "OK!", time: result.rows[0].now });
  } catch (err) {
    console.error("Error connecting to the database", err);
    res.status(500).send("Internal Server Error");
  }
});

app.use("/reservations", reservationsRouter);
app.use("/admin", adminRouter);

export default app;
