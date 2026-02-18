const fs = require("fs");
const { parse } = require("csv-parse");
const { stringify } = require("csv-stringify/sync");

/**
 * Sauvegarde un tableau d'objets en CSV dans un fichier.
 * @param {Array<Object>} data - Données à exporter
 * @param {string} filePath - Chemin du fichier de sortie
 */
function saveAsCsv(data, filePath) {
  if (!data || !data.length) return;
  const csv = stringify(data, { header: true });
  fs.writeFileSync(filePath, csv, "utf8");
}
/**
 * Fusionne tous les champs de record avec certains champs de predicted et scored, sans écraser record.
 * @param {Object} record - L'objet de base
 * @param {Object} predicted - Objet predicted indexé par member
 * @param {Object} scored - Objet scored indexé par member
 * @returns {Object} - Objet fusionné pour export
 */
function buildExport(record, predicted, scored) {
  const predictedFields = [
    "vp_annual_mean_before_current_year",
    "vp_annual_sum_before_current_year",
    "vp_mean_current_year",
    "vl_annual_mean_before_current_year",
    "vl_annual_sum_before_current_year",
    "vl_mean_current_year",
    "vl_sum_current_year",
    "vp_sum_current_year",
    "vp_sum_current_year_pred",
    "vp_sum_current_year_diff",
    "vp_sum_current_year_diff_pct",
  ];
  const scoredFields = [
    "cluster_id",
    "cluster_description",
    "vp_sum_current_year",
    "vp_mean_current_year",
    "vl_sum_current_year",
    "cluster_vp_mean",
    "cluster_vp_median",
    "cluster_vp_std",
    "cluster_vp_min",
    "cluster_vp_max",
    "cluster_score",
    "cluster_percentile",
    "vp_diff_from_cluster_mean",
    "vp_diff_from_cluster_median",
  ];
  const exportObj = { ...record };
  if (predicted) {
    for (const f of predictedFields) {
      if (!(f in exportObj) && predicted[f] !== undefined)
        exportObj[f] = predicted[f];
    }
  }
  if (scored) {
    for (const f of scoredFields) {
      if (!(f in exportObj) && scored[f] !== undefined)
        exportObj[f] = scored[f];
    }
  }
  return exportObj;
}
/**
 * Indexe un tableau d'objets par le champ 'member' dans une Map.
 * @param {Array<Object>} arr - Tableau d'objets à indexer
 * @returns {Map<string, Object>} - Map indexée par member
 */
function indexByMember(arr) {
  return new Map(arr.map((obj) => [obj.member, obj]));
}

/**
 * Charge un fichier CSV et le convertit en tableau d'objets JS.
 * @param {string} filePath - Chemin du fichier CSV
 * @returns {Promise<Array<Object>>} - Résultat du parsing
 */
function loadCsvAsObjects(filePath) {
  return new Promise((resolve, reject) => {
    const records = [];
    fs.createReadStream(filePath)
      .pipe(
        parse({
          columns: true,
          skip_empty_lines: true,
          relax_quotes: true,
          quote: '"',
          escape: '"',
          trim: true,
        }),
      )
      .on("data", (row) => records.push(row))
      .on("end", () => resolve(records))
      .on("error", reject);
  });
}

async function main() {
  try {
    const extract = await loadCsvAsObjects("../data/extract.csv");
    const scored = await loadCsvAsObjects("../data/08_scored_members.csv");
    const predicted = await loadCsvAsObjects("../data/09_predictions_2025.csv");
    console.log(scored[0]);
    console.log(predicted[0]);

    const scoredByMember = indexByMember(scored);
    const predictedByMember = indexByMember(predicted);

    let ns = 0,
      np = 0,
      two = 0;
    const exportData = [];
    for (const record of extract) {
      const member = record.MATRICULE_ANONYME;
      if (scoredByMember.get(member)) ns++;
      if (predictedByMember.get(member)) np++;
      if (scoredByMember.get(member) && predictedByMember.get(member)) {
        exportData.push(
          buildExport(
            record,
            predictedByMember.get(member),
            scoredByMember.get(member),
          ),
        );
        two++;
      }
      //   record.score = scoredByMember.get(member)?.score || null;
      //   record.prediction = predictedByMember.get(member)?.prediction || null;
    }
    console.log(`Scored: ${ns} / ${extract.length}`);
    console.log(`Predicted: ${np} / ${extract.length}`);
    console.log(`Both: ${two} / ${extract.length}`);
    // Sauvegarde les données exportées en CSV
    saveAsCsv(exportData, "../data/export.csv");
    // console.log(extract.length, "records loaded");
    // console.log(extract[0]);
  } catch (error) {
    console.error("Error loading CSV:", error);
  }
}

main();
