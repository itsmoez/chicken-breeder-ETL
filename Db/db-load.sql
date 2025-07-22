-- Create the chickens table
CREATE TABLE IF NOT EXISTS chickens (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INTEGER NOT NULL,
    breed VARCHAR(100) NOT NULL,
    birthday VARCHAR(50),
    size VARCHAR(50)
);