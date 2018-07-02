DROP KEYSPACE IF EXISTS socialNetwork;

CREATE KEYSPACE socialNetwork WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

USE socialNetwork;

DROP TABLE IF EXISTS users;

CREATE TABLE users(id int PRIMARY KEY, updatedOn date, image text, username text, deleted boolean);

INSERT INTO users(id, updatedOn, image, username, deleted) VALUES (1000, '2017-05-05', '', 'barthiex', false);

SELECT * FROM users;
