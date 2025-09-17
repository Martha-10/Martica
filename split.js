import fs from "fs";
import csv from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";

const inputFile = "agrovida_martha.csv";

// Helpers
const unique = (arr, key) => {
  const seen = new Set();
  return arr.filter((item) => {
    const val = item[key];
    if (seen.has(val)) return false;
    seen.add(val);
    return true;
  });
};

const writers = {
  tipo_suelo: createObjectCsvWriter({
    path: "outputs/tipo_suelo.csv",
    header: [{ id: "nombre_tipo_suelo", title: "nombre_tipo_suelo" }],
  }),
  sistema_riego: createObjectCsvWriter({
    path: "outputs/sistema_riego.csv",
    header: [{ id: "nombre_sistema_riego", title: "nombre_sistema_riego" }],
  }),
  fertilizante: createObjectCsvWriter({
    path: "outputs/fertilizante.csv",
    header: [{ id: "nombre_fertilizante", title: "nombre_fertilizante" }],
  }),
  tecnico: createObjectCsvWriter({
    path: "outputs/tecnico.csv",
    header: [{ id: "nombre_tecnico", title: "nombre_tecnico" }],
  }),
  cultivo: createObjectCsvWriter({
    path: "outputs/cultivo.csv",
    header: [{ id: "tipo_cultivo", title: "tipo_cultivo" }],
  }),
  variedad_cultivo: createObjectCsvWriter({
    path: "outputs/variedad_cultivo.csv",
    header: [
      { id: "tipo_cultivo", title: "tipo_cultivo" },
      { id: "nombre_variedad", title: "nombre_variedad" },
    ],
  }),
  tipo_sensor: createObjectCsvWriter({
    path: "outputs/tipo_sensor.csv",
    header: [{ id: "nombre_tipo_sensor", title: "nombre_tipo_sensor" }],
  }),
  finca: createObjectCsvWriter({
    path: "outputs/finca.csv",
    header: [
      { id: "nombre_finca", title: "nombre_finca" },
      { id: "region", title: "region" },
      { id: "es_organico", title: "es_organico" },
      { id: "tipo_suelo", title: "tipo_suelo" },
      { id: "sistema_riego", title: "sistema_riego" },
      { id: "fertilizante", title: "fertilizante" },
    ],
  }),
  sensor: createObjectCsvWriter({
    path: "outputs/sensor.csv",
    header: [
      { id: "id_sensor", title: "id_sensor" },
      { id: "estado_sensor", title: "estado_sensor" },
      { id: "fecha_mantenimiento", title: "fecha_mantenimiento" },
      { id: "tipo_sensor", title: "tipo_sensor" },
      { id: "finca", title: "finca" },
    ],
  }),
  medicion: createObjectCsvWriter({
    path: "outputs/medicion.csv",
    header: [
      { id: "id_sensor", title: "id_sensor" },
      { id: "valor", title: "valor" },
      { id: "fecha_hora", title: "fecha_hora" },
    ],
  }),
  finca_cultivo: createObjectCsvWriter({
    path: "outputs/finca_cultivo.csv",
    header: [
      { id: "finca", title: "finca" },
      { id: "variedad", title: "variedad" },
      { id: "produccion_toneladas", title: "produccion_toneladas" },
    ],
  }),
  asignacion_tecnico: createObjectCsvWriter({
    path: "outputs/asignacion_tecnico.csv",
    header: [
      { id: "finca", title: "finca" },
      { id: "tecnico", title: "tecnico" },
      { id: "fecha_inicio", title: "fecha_inicio" },
    ],
  }),
};

const rows = [];
fs.createReadStream(inputFile)
  .pipe(csv())
  .on("data", (row) => rows.push(row))
  .on("end", async () => {
    console.log("CSV cargado, procesando...");

    await writers.tipo_suelo.writeRecords(
      unique(rows.map((r) => ({ nombre_tipo_suelo: r["Tipo de Suelo"] })), "nombre_tipo_suelo")
    );

    await writers.sistema_riego.writeRecords(
      unique(rows.map((r) => ({ nombre_sistema_riego: r["Sistema de Riego"] })), "nombre_sistema_riego")
    );

    await writers.fertilizante.writeRecords(
      unique(rows.map((r) => ({ nombre_fertilizante: r["Fertilizante Usado"] })), "nombre_fertilizante")
    );

    await writers.tecnico.writeRecords(
      unique(rows.map((r) => ({ nombre_tecnico: r["Técnico Responsable"] })), "nombre_tecnico")
    );

    await writers.cultivo.writeRecords(
      unique(rows.map((r) => ({ tipo_cultivo: r["Tipo de Cultivo"] })), "tipo_cultivo")
    );

    await writers.variedad_cultivo.writeRecords(
      unique(
        rows.map((r) => ({
          tipo_cultivo: r["Tipo de Cultivo"],
          nombre_variedad: r["Variedad del Cultivo"],
        })),
        (v) => v.tipo_cultivo + v.nombre_variedad
      )
    );

    await writers.tipo_sensor.writeRecords(
      unique(rows.map((r) => ({ nombre_tipo_sensor: r["Tipo de Sensor"] })), "nombre_tipo_sensor")
    );

    await writers.finca.writeRecords(
      unique(
        rows.map((r) => ({
          nombre_finca: r["Nombre de la Finca"],
          region: r["Región"],
          es_organico: r["Es Orgánico"].toLowerCase() === "sí" || r["Es Orgánico"].toLowerCase() === "si",
          tipo_suelo: r["Tipo de Suelo"],
          sistema_riego: r["Sistema de Riego"],
          fertilizante: r["Fertilizante Usado"],
        })),
        "nombre_finca"
      )
    );

    await writers.sensor.writeRecords(
      unique(
        rows.map((r) => ({
          id_sensor: r["ID del Sensor"],
          estado_sensor: r["Estado del Sensor"],
          fecha_mantenimiento: r["Fecha de Mantenimiento"],
          tipo_sensor: r["Tipo de Sensor"],
          finca: r["Nombre de la Finca"],
        })),
        "id_sensor"
      )
    );

    await writers.medicion.writeRecords(
      rows.map((r) => ({
        id_sensor: r["ID del Sensor"],
        valor: r["Valor"],
        fecha_hora: r["Fecha y Hora"],
      }))
    );

    await writers.finca_cultivo.writeRecords(
      unique(
        rows.map((r) => ({
          finca: r["Nombre de la Finca"],
          variedad: r["Variedad del Cultivo"],
          produccion_toneladas: r["Producción (Toneladas)"],
        })),
        (v) => v.finca + v.variedad
      )
    );

    await writers.asignacion_tecnico.writeRecords(
      unique(
        rows.map((r) => ({
          finca: r["Nombre de la Finca"],
          tecnico: r["Técnico Responsable"],
          fecha_inicio: "2025-01-01",
        })),
        (v) => v.finca + v.tecnico
      )
    );

    console.log("✅ Archivos CSV generados con éxito");
  });
