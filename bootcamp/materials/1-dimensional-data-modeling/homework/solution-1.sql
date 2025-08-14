CREATE TABLE actors (
    actor VARCHAR,
    actorid INTEGER,
    year INTEGER,
    films ARRAY(STRUCT(
        film VARCHAR,
        votes INTEGER,
        rating DOUBLE,
        filmid INTEGER
    )),
    quality_class VARCHAR,
    is_active BOOLEAN,
    PRIMARY KEY (actorid, year)
);