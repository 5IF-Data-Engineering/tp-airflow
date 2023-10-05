CREATE TABLE IF NOT EXISTS "character" (
    "id" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "name" varchar(255) NOT NULL,
    "attributes" varchar(255) NOT NULL,
    "race" varchar(255) NOT NULL,
    "languages" varchar(255) NOT NULL,
    "class" varchar(255) NOT NULL,
    "proficiency_choices" varchar(255) NOT NULL,
    "level" varchar(255) NOT NULL,
    "spells" varchar(255)
);

CREATE TABLE IF NOT EXISTS "country" (
    "id" BIGSERIAL,
    "country" varchar(255) NOT NULL,
    "latitude" float NOT NULL,
    "longitude" float NOT NULL,
    "name" varchar(255) NOT NULL,
    "continent" varchar(255) NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "users" (
    "id" BIGSERIAL,
    "gender" varchar(255) NOT NULL,
    "title" varchar(255) NOT NULL,
    "first_name" varchar(255) NOT NULL,
    "last_name" varchar(255) NOT NULL,
    "country" varchar(255) NOT NULL,
    "age" int NOT NULL,
    PRIMARY KEY ("id")    
);