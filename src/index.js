/**
 * Indexe un tableau d'objets par le champ 'member' dans une Map.
 * @param {Array<Object>} arr - Tableau d'objets à indexer
 * @returns {Map<string, Object>} - Map indexée par member
 */
function indexByMember(arr) {
  return new Map(arr.map((obj) => [obj.member, obj]));
}
const fs = require("fs");
const { parse } = require("csv-parse");

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
    for (const record of extract) {
      const member = record.MATRICULE_ANONYME;
      if (scoredByMember.get(member)) ns++;
      if (predictedByMember.get(member)) np++;
      if (scoredByMember.get(member) && predictedByMember.get(member)) two++;
      //   record.score = scoredByMember.get(member)?.score || null;
      //   record.prediction = predictedByMember.get(member)?.prediction || null;
    }
    console.log(`Scored: ${ns} / ${extract.length}`);
    console.log(`Predicted: ${np} / ${extract.length}`);
    console.log(`Both: ${two} / ${extract.length}`);
    // console.log(extract.length, "records loaded");
    // console.log(extract[0]);
  } catch (error) {
    console.error("Error loading CSV:", error);
  }
}

main();
