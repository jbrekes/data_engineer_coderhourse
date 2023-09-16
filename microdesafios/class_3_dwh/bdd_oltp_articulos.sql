CREATE SCHEMA articulos;

CREATE TABLE articulos.titulos
(titulo_id char(6) NOT NULL,
titulo varchar(80) NOT NULL,
tipo char(20) NOT NULL);

-- Insertar valores manualmente(OJO esto se puede hacer con COPY o con un asistente)
INSERT INTO articulos.titulos VALUES ('1', 'Consultas SQL','bbdd');
INSERT INTO articulos.titulos VALUES ('3', 'Grupo recursos Azure','administracion');
INSERT INTO articulos.titulos VALUES ('4', '.NET Framework 4.5','programacion');
INSERT INTO articulos.titulos VALUES ('5', 'Programacion C#','dev');
INSERT INTO articulos.titulos VALUES ('7', 'Power BI','BI');
INSERT INTO articulos.titulos VALUES ('8', 'Administracion Sql server','administracion');


-- 4. crear tabla autores
CREATE TABLE articulos.autores
(TituloId char(6) NOT NULL,
NombreAutor varchar(100) NOT NULL,
ApellidosAutor varchar(100) NOT NULL,
TelefonoAutor varChar(25)
);

-- Insertar en la tabla autores en essquema articulos
INSERT INTO articulos.autores VALUES ('3', 'David', 'Saenz', '99897867');
INSERT INTO articulos.autores VALUES ('8', 'Ana', 'Ruiz', '99897466');
INSERT INTO articulos.autores VALUES ('2', 'Julian', 'Perez', '99897174');
INSERT INTO articulos.autores VALUES ('1', 'Andres', 'Calamaro', '99876869');
INSERT INTO articulos.autores VALUES ('4', 'Cidys', 'Castillo', '998987453');
INSERT INTO articulos.autores VALUES ('5', 'Pedro', 'Molina', '99891768');

SELECT * FROM articulos.autores