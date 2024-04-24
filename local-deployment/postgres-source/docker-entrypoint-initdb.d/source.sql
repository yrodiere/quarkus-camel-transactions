CREATE TABLE data(
    ID INT PRIMARY KEY,
    NAME VARCHAR(20) NOT NULL
);

INSERT INTO data(ID, NAME)
    VALUES
        (0, 'Hello'),
        (1, 'World');