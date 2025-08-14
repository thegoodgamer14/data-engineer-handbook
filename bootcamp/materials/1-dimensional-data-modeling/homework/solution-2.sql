INSERT INTO actors
WITH yearly_actor_films AS (
    SELECT 
        actor,
        actorid,
        year,
        ARRAY_AGG(STRUCT(film, votes, rating, filmid)) as films,
        AVG(rating) as avg_rating
    FROM actor_films 
    WHERE year = {{ current_year }}
    GROUP BY actor, actorid, year
),
quality_classified AS (
    SELECT 
        actor,
        actorid,
        year,
        films,
        CASE 
            WHEN avg_rating > 8 THEN 'star'
            WHEN avg_rating > 7 THEN 'good'
            WHEN avg_rating > 6 THEN 'average'
            ELSE 'bad'
        END as quality_class
    FROM yearly_actor_films
),
current_year_actors AS (
    SELECT DISTINCT actorid 
    FROM actor_films 
    WHERE year = {{ current_year }}
)
SELECT 
    qc.actor,
    qc.actorid,
    qc.year,
    qc.films,
    qc.quality_class,
    CASE WHEN cya.actorid IS NOT NULL THEN TRUE ELSE FALSE END as is_active
FROM quality_classified qc
LEFT JOIN current_year_actors cya ON qc.actorid = cya.actorid;