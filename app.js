const express  = require('express');
const app      = express();
const cors     = require('cors');
const path     = require('path');
const fs       = require('fs');
const yauzl    = require('yauzl');

const gtfs2geojson = require('./gtfs2geojson.js');

app.use(cors());
app.use(express.static('public'));

const DATA_DIR = __dirname;
const ZIP_FILE = path.join(DATA_DIR, 'gtfs_rodalies.zip');

// --------------------------------------------------
// utilidades
// --------------------------------------------------

function filePath(name) {
    return path.join(DATA_DIR, name);
}

function existeArchivo(name) {
    return fs.existsSync(filePath(name));
}

app.get("/api/trenes", async (req, res) => {
    try {
        const response = await fetch("https://gtfsrt.renfe.com/vehicle_positions.json");
        
        if (!response.ok) {
            return res.status(response.status).send("Error al obtener datos de Renfe");
        }

        const data = await response.json();
        res.json(data);

    } catch (err) {
        console.error("Error proxy trenes:", err);
        res.status(500).json({ error: "Error obteniendo trenes en tiempo real" });
    }
});
// --------------------------------------------------
// rutas
// --------------------------------------------------

app.all("/getdata", function(req, res) {

    if (!fs.existsSync(ZIP_FILE)) {
        return res.status(500).json({
            error: "No existe gtfs_rodalies.zip"
        });
    }

    Promise.all([
        leerZip('shapes.txt'),
        leerZip('stops.txt'),
        leerZip('trips.txt'),
        leerZip('routes.txt')
    ])
    .then(() => {
        res.json({ msg: "Archivos GTFS extraídos correctamente" });
    })
    .catch(err => {
        res.status(500).json({ error: err.message });
    });
});

app.get("/stops", function(req, res) {

    if (!existeArchivo('stops.txt')) {
        return res.status(400).json({
            error: "stops.txt no existe. Llama antes a /getdata"
        });
    }

    gtfs2geojson.stops(
        fs.readFileSync(filePath('stops.txt'), 'utf8'),
        result => res.json(result)
    );
});

app.get("/shapes", function(req, res) {

    if (
        !existeArchivo('shapes.txt') ||
        !existeArchivo('trips.txt') ||
        !existeArchivo('routes.txt')
    ) {
        return res.status(400).json({
            error: "GTFS incompleto. Llama antes a /getdata"
        });
    }

    gtfs2geojson.linesWithRoutes(
        fs.readFileSync(filePath('shapes.txt'), 'utf8'),
        fs.readFileSync(filePath('trips.txt'), 'utf8'),
        fs.readFileSync(filePath('routes.txt'), 'utf8'),
        result => res.json(result)
    );
});

app.get("/trips", function(req, res) {

    if (!existeArchivo('trips.txt')) {
        return res.status(400).json({
            error: "trips.txt no existe. Llama antes a /getdata"
        });
    }

    gtfs2geojson.lines(
        fs.readFileSync(filePath('trips.txt'), 'utf8'),
        result => res.json(result)
    );
});



// --------------------------------------------------
// servidor
// --------------------------------------------------

app.listen(3000, () => {
    console.log("Servidor escuchando en http://localhost:3000");
});

// --------------------------------------------------
// ZIP
// --------------------------------------------------

function leerZip(archivo) {
    return new Promise((resolve, reject) => {

        yauzl.open(ZIP_FILE, { lazyEntries: true }, function(err, zipfile) {
            if (err) return reject(err);

            zipfile.readEntry();

            zipfile.on("entry", function(entry) {

                if (/\/$/.test(entry.fileName)) {
                    zipfile.readEntry();
                    return;
                }

                if (entry.fileName === archivo) {
                    zipfile.openReadStream(entry, function(err, readStream) {
                        if (err) return reject(err);

                        const output = fs.createWriteStream(filePath(archivo));
                        readStream.pipe(output);

                        output.on('finish', () => {
                            zipfile.close();
                            resolve();
                        });
                    });
                } else {
                    zipfile.readEntry();
                }
            });
        });
    });
}
