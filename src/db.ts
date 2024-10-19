import { Pool } from "pg";
import { getRandomIntegerInclusive } from "./utils";

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

export default pool;

const createUsersTable = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name VARCHAR(100) NOT NULL,
      email VARCHAR(100) UNIQUE NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  await pool.query(createTableQuery);
};

const createRoomsTable = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS rooms (
      id SERIAL PRIMARY KEY,
      room_number VARCHAR(50) UNIQUE NOT NULL,
      capacity INT NOT NULL,
      category VARCHAR(10) CHECK (category IN ('basic', 'luxury')) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  await pool.query(createTableQuery);
};

const createReservationsTable = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS reservations (
      id SERIAL PRIMARY KEY,
      room_id INT NOT NULL,
      user_id INT NOT NULL,
      start_date TIMESTAMP NOT NULL,
      end_date TIMESTAMP NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
      FOREIGN KEY (room_id) REFERENCES rooms (id) ON DELETE CASCADE
    );
  `;
  await pool.query(createTableQuery);
};

const populateUsersTable = async () => {
  try {
    const users = [{ name: "Alice" }, { name: "Bob" }, { name: "Charlie" }];

    const usersStrings = users.map((user, index) => {
      const userEmail = `${user.name}${index + 1}@email.com`;
      return `('${user.name}', '${userEmail}')`;
    });

    const insertUserQuery = `
      INSERT INTO users (name, email) VALUES
        ${usersStrings.join(", ")}
        ;
    `;
    await pool.query(insertUserQuery);
  } catch (error) {
    console.error("Error populating users table", error);
  }
};

const populateRoomsTable = async () => {
  try {
    const capacities = [2, 4];
    const categories = ["basic", "luxury"];

    const rooms = [];
    for (let i = 1; i <= 11; i++) {
      // floors 1 to 11
      for (let j = 1; j <= 10; j++) {
        // rooms 01 to 10
        rooms.push({
          room_number: `${i}${j < 10 ? `0${j}` : `${j}`}`,
          capacity: capacities.at(getRandomIntegerInclusive(0, 1)),
          category: categories.at(getRandomIntegerInclusive(0, 1)),
        });
      }
    }

    const roomsStrings = rooms.map(
      (room) =>
        `('${room.room_number}', '${room.capacity}', '${room.category}')`
    );

    const insertRoomQuery = `
      INSERT INTO rooms (room_number, capacity, category) VALUES
        ${roomsStrings.join(", ")}
        ;
    `;
    await pool.query(insertRoomQuery);
  } catch (error) {
    console.error("Error populating rooms table", error);
  }
};

export const setupDatabase = async () => {
  await createUsersTable();
  await createRoomsTable();
  await createReservationsTable();

  await populateUsersTable();
  await populateRoomsTable();

  console.log("Database setup complete!");
};
