import dotenv from "dotenv";
dotenv.config();

import app from "./app";
import { setupDatabase } from "./db";


const initApp = async () => {
  const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(`Server running on port ${port}`);
  });
}

const start = async () => {
  await setupDatabase();
  await initApp();
}

start()
