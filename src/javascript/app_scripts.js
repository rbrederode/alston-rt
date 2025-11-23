/**
* Test helper for onEdit(e). Simulates an edit on cell B34 on the
* "OBS DESIGN" sheet by manually constructing an event object.
* Useful for debugging without manually editing the sheet.
*/
function testOnEdit() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("OBS DESIGN");;
  const e = {
    range: sheet.getRange("D3"),
    value: "TRUE"
  };
  onEdit(e);
}

/**
* Update the title of the first chart on the active sheet.
* Reads the new title from cell D8.
*/
function updateChartTitle() {
  var sheet = SpreadsheetApp.getActiveSheet()
  var newTitle = sheet.getRange("D8").getValue(); 
  var chart = sheet.getCharts()[0]; // Change [0] if it's not the first chart
  var updatedChart = chart.modify()
    .setOption('title', newTitle)
    .build();
  sheet.updateChart(updatedChart);
}

/**
* Remove non-numeric characters from a string and return a float.
* @param {string} str - String possibly containing numbers with units or symbols.
* @return {number} cleaned numeric value.
*/
function cleanNumber(str) {
  return parseFloat(str.replace(/[^\d\.\-]/g, ""));
}

/**
* Generate altitude data for the target specified in OBS DESIGN.
*
* Reads RA/Dec from cells D6/D7, observer latitude and longitude,
* computes target altitude and Sun altitude throughout the day at
* fixed time steps, writes results to AltitudeData sheet.
*/
function generateAltitudeData(forDate) {
  const srcSheetName = "OBS DESIGN";   // sheet where RA/Dec are entered
  const raCell = "D6";
  const decCell = "D7";
  const outSheetName = "AltitudeData";
  const stepMinutes = 15;          // sampling every 15 minutes (change as desired)

  const latCell = "B10"
  const lonCell = "B11"
  
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tz = ss.getSpreadsheetTimeZone(); // will typically be your local timezone (Europe/London)
  const src = ss.getSheetByName(srcSheetName);
  if (!src) throw new Error("Source sheet '" + srcSheetName + "' not found.");

  const raStr = src.getRange(raCell).getDisplayValue().toString().trim();
  const decStr = src.getRange(decCell).getDisplayValue().toString().trim();
  if (!raStr || !decStr) throw new Error("Please put RA in " + raCell + " and Dec in " + decCell + " on " + srcSheetName + ".");

  const raDeg = parseRAtoDeg(raStr);
  const decDeg = parseDECtoDeg(decStr);

  const observerLat = cleanNumber(src.getRange(latCell).getDisplayValue());
  const observerLon = cleanNumber(src.getRange(lonCell).getDisplayValue());

  Logger.log("Observer Lat: " + observerLat);
  Logger.log("Observer Lon: " + observerLon);

  if (isNaN(observerLat) || isNaN(observerLon)) {
    throw new Error("Observer latitude or longitude is not a valid number.");
  }

  // build time series: forDate calendar day in sheet timezone
  let now;
  if (forDate) {
    // if it's already a Date, keep it; else convert from number
    now = forDate instanceof Date ? forDate : new Date(forDate);
  } else {
    now = new Date();
  }

  const startLocalStr = Utilities.formatDate(now, tz, "yyyy-MM-dd") + "T00:00:00";
  const start = new Date(startLocalStr);
  // Note: Date(...) interprets as local time of script runtime; using formatted string based on tz is OK for spreadsheet use

  const points = Math.ceil((24*60) / stepMinutes);
  const rows = [];
  rows.push(["Time (local)", "Time (UTC)", "LST (deg)", "Hour Angle (deg)", "Target Alt (deg)", "Sun Alt (deg)"]);

  let sunrise = null;
  let sunset = null;

  for (let i = 0; i <= points; i++) {

    let t;

    if (i < points) {
      t = new Date(start.getTime() + i * stepMinutes * 60 * 1000);
    } else {
      t = new Date(start.getTime() + (i * stepMinutes -1) * 60 * 1000);
    }

    // compute LST in degrees at this UTC moment for observer longitude
    const lstDeg = computeLSTforDate(t, observerLon);
    // hour angle (deg) = LST - RA
    let H = lstDeg - raDeg;
    H = wrap180(H); // wrap to [-180,180] for trig stability
    const altDeg = computeAltitudeDeg(raDeg, decDeg, observerLat, lstDeg);

    // --- Compute Sun altitude ---
    const { raDeg: sunRA, decDeg: sunDec } = getSunRADec(t);
    const sunAlt = computeAltitudeDeg(sunRA, sunDec, observerLat, lstDeg);

    // Detect horizon crossings
    if (i > 0) {
      if (lastSunAlt < 0 && sunAlt >= 0 && sunrise === null) {
        sunrise = new Date(t); // sunrise moment
      }
      if (lastSunAlt > 0 && sunAlt <= 0 && sunset === null) {
        sunset = new Date(t); // sunset moment
      }
    }
    var lastSunAlt = sunAlt;

    rows.push([
      Utilities.formatDate(t, tz, "HH:mm:ss"),
      Utilities.formatDate(t, "UTC", "yyyy-MM-dd HH:mm:ss"),
      roundTo(lstDeg, 4),
      roundTo(H, 4),
      roundTo(altDeg, 4),
      roundTo(sunAlt, 4)
    ]);
  }

  // write to output sheet (overwrite range)
  let out = ss.getSheetByName(outSheetName);
  if (!out) out = ss.insertSheet(outSheetName);
  out.getRange("A1:E100").clearContent(); // remove previous content and charts
  out.getRange(1, 1, rows.length, rows[0].length).setValues(rows);

  out.getRange(rows.length + 2, 1).setValue("Sunrise (local)");
  out.getRange(rows.length + 2, 2).setValue(sunrise ?
    Utilities.formatDate(sunrise, tz, "HH:mm:ss") : "No sunrise");

  out.getRange(rows.length + 3, 1).setValue("Sunset (local)");
  out.getRange(rows.length + 3, 2).setValue(sunset ?
      Utilities.formatDate(sunset, tz, "HH:mm:ss") : "No sunset");

  // make a chart for Altitude vs Time (local)
  try {
    //createAltitudeChart(out, src, rows.length, raStr, decStr);
  } catch (e) {
    // chart creation is optional — still keep data
    Logger.log("Chart creation failed: " + e);
  }

  SpreadsheetApp.flush();
}

/**
* Create an altitude-vs-time line chart on the source sheet.
* @param {Sheet} sheet - Sheet containing populated altitude data rows.
* @param {Sheet} src - Sheet on which to insert the chart.
* @param {number} nRows - Number of data rows.
* @param {string} raStr - Original RA string used for data label.
* @param {string} decStr - Original Dec string used for data label.
*/
function createAltitudeChart(sheet, src, nRows, raStr, decStr) {
  // Uses column A (Time local) vs column E (Altitude)
  const chart = src.newChart()
    .asLineChart()
    .addRange(sheet.getRange(1, 1, nRows, 1))           // Time (will be domain)
    .addRange(sheet.getRange(1, 5, nRows, 1))           // Altitude
    .setPosition(8, 4, 0, 0)
    .setOption("title", "Altitude vs Time (local) RA:"+raStr+" DEC:"+decStr)
    .setOption("hAxis", {title: "Time (local)"})
    .setOption("vAxis", {title: "Altitude (deg)"})
    .setOption("series", {0: {targetAxisIndex: 0}})
    .setOption("curveType", "none")
    .build();
  src.insertChart(chart);
}

/**
* Parse Right Ascension (string) into decimal degrees.
* Accepts formats like:
* - "hh:mm:ss"
* - "12h30m49.4s"
* - "12 30 49.4"
* - decimal degrees
* @param {string} s - RA string.
* @return {number} RA in degrees.
*/
function parseRAtoDeg(s) {
  // If contains letters or colon or space with 3 parts assume HMS
  if (s.match(/[hmsHMS:]/) || (s.split(/\s+/).length === 3 && s.indexOf(":") === -1)) {
    // allow formats like "12 34 56.7" or "12:34:56.7" or "12h34m56.7s"
    const cleaned = s.replace(/[hmsHMS]/g, " ").replace(/:+/g, " ").trim();
    const parts = cleaned.split(/\s+/);
    if (parts.length < 3) throw new Error("Cannot parse RA: " + s);
    const h = parseFloat(parts[0]), m = parseFloat(parts[1]), sec = parseFloat(parts[2]);
    if (isNaN(h) || isNaN(m) || isNaN(sec)) throw new Error("Cannot parse RA: " + s);
    return (h + m/60 + sec/3600) * 15.0; // hours -> degrees
  } else {
    // assume decimal degrees
    const v = parseFloat(s);
    if (isNaN(v)) throw new Error("Cannot parse RA: " + s);
    return v;
  }
}

/**
* Parse Declination into decimal degrees.
* Supports formats:
* - "+12:23:28", "12d23m28s", "12 23 28"
* - decimal degrees
* @param {string} s - Dec string.
* @return {number} Dec in degrees.
*/
function parseDECtoDeg(s) {
  if (s.match(/[dDmMsS°'":]/) || (s.split(/\s+/).length === 3 && s.indexOf(":") === -1)) {
    // replace common separators
    const cleaned = s.replace(/[°dDmMsS'"]/g, " ").replace(/:+/g, " ").trim();
    const parts = cleaned.split(/\s+/);
    if (parts.length < 3) throw new Error("Cannot parse Dec: " + s);
    let sign = 1;
    let degPart = parts[0];
    if (degPart.startsWith("+") || degPart.startsWith("-")) {
      if (degPart.startsWith("-")) sign = -1;
      degPart = degPart.substring(1);
    }
    const d = parseFloat(degPart), m = parseFloat(parts[1]), sec = parseFloat(parts[2]);
    if (isNaN(d) || isNaN(m) || isNaN(sec)) throw new Error("Cannot parse Dec: " + s);
    return sign * (d + m/60 + sec/3600);
  } else {
    const v = parseFloat(s);
    if (isNaN(v)) throw new Error("Cannot parse Dec: " + s);
    return v;
  }
}

/**
* Wrap any angle to the range [-180, 180).
* @param {number} a - Angle in degrees.
* @return {number} Wrapped angle.
*/
function wrap180(a) {
  let v = ((a + 180) % 360 + 360) % 360 - 180;
  return v;
}

/**
* Compute Local Sidereal Time at the given date and longitude.
* @param {Date} dateObj - JavaScript Date object.
* @param {number} lonDeg - Longitude in degrees (east positive).
* @return {number} LST in degrees.
*/
function computeLSTforDate(dateObj, lonDeg) {
  // convert dateObj to UTC milliseconds
  const jd = (dateObj.getTime() / 86400000.0) + 2440587.5; // julian date
  const d = jd - 2451545.0;
  // Greenwich Mean Sidereal Time in degrees
  let gmst = 280.46061837 + 360.98564736629 * d;
  gmst = ((gmst % 360) + 360) % 360;
  // local sidereal time
  let lst = gmst + lonDeg;
  lst = ((lst % 360) + 360) % 360;
  return lst;
}

/**
* Compute the altitude (deg) of an object given RA, Dec, observer lat, and LST.
* @param {number} raDeg
* @param {number} decDeg
* @param {number} latDeg
* @param {number} lstDeg
* @return {number} Altitude in degrees.
*/
function computeAltitudeDeg(raDeg, decDeg, latDeg, lstDeg) {
  // hour angle H = LST - RA (deg), convert to radians and compute
  let H = lstDeg - raDeg;
  H = H * Math.PI / 180.0;
  const decR = decDeg * Math.PI / 180.0;
  const latR = latDeg * Math.PI / 180.0;

  const sinA = Math.sin(latR) * Math.sin(decR) + Math.cos(latR) * Math.cos(decR) * Math.cos(H);
  const alt = Math.asin(Math.max(-1, Math.min(1, sinA)));
  return alt * 180.0 / Math.PI;
}

/**
* Compute the Sun's current RA/Dec (degrees) using low-precision solar theory.
* @param {Date} dateObj - JavaScript Date in UTC.
* @return {{raDeg:number, decDeg:number}} Sun coordinates.
*/
function getSunRADec(dateObj) {
  const d = ((dateObj.getTime() / 86400000.0) + 2440587.5) - 2451545.0;

  // Mean anomaly of Sun (deg)
  const M = (357.52911 + 0.98560028 * d) % 360;

  // Center equation
  const C = 1.914602 * Math.sin(M * Math.PI/180)
          + 0.019993 * Math.sin(2*M * Math.PI/180)
          + 0.000289 * Math.sin(3*M * Math.PI/180);

  // Ecliptic longitude
  const lambda = (M + 102.9372 + C + 180) % 360;

  // Obliquity of the ecliptic
  const epsilon = 23.439 - 0.00000036 * d;

  // Convert to RA/Dec
  const lambdaR = lambda * Math.PI/180;
  const epsR = epsilon * Math.PI/180;

  const sinDec = Math.sin(epsR) * Math.sin(lambdaR);
  const dec = Math.asin(sinDec) * 180/Math.PI;

  const y = Math.cos(epsR) * Math.sin(lambdaR);
  const x = Math.cos(lambdaR);

  let ra = Math.atan2(y, x) * 180/Math.PI;
  if (ra < 0) ra += 360;

  return { raDeg: ra, decDeg: dec };
}

/**
* Round a value to the given number of decimal places.
* @param {number} x - Input number.
* @param {number} dp - Decimal places.
* @return {number} Rounded value.
*/
function roundTo(x, dp) {
  const m = Math.pow(10, dp || 3);
  return Math.round(x * m) / m;
}

/**
* Remove empty rows in DB TARGET LIST, keeping only rows with data.
*/
function consolidateRows() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName('DB TARGET LIST');

  // 1. Read data (skip header row)
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;  // nothing to do

  const lastCol = sheet.getLastColumn();
  const range = sheet.getRange(2, 1, lastRow - 1, lastCol);
  const values = range.getValues();

  // 2. Filter out completely empty rows
  const filtered = values.filter(row =>
    row.some(cell => cell !== "" && cell !== null)
  );

  // 3. Clear FULL data area (from row 2 downward)
  sheet.getRange(2, 1, lastRow - 1, lastCol).clearContent();

  // 4. If we still have data, write it back compacted starting at row 2
  if (filtered.length > 0) {
    sheet.getRange(2, 1, filtered.length, lastCol).setValues(filtered);
  }
}

/**
 * Master onEdit handler for the spreadsheet.
 *
 * This function handles multiple edit events across different sheets:
 *
 * DIG001 sheet:
 *   - If columns D:E (5) rows 4-10 are edited:
 *     → Generates a JSON string representing the Digitiser configuration.
 *     → Writes JSON to TM_UI_API sheet cell B3.
 *
 * OBS DESIGN sheet:
 *   - If cell B25 or B30 is edited:
 *     → Generates altitude plot and updates chart title.
 *
 *   - If Delete Target checkbox (column F, rows ≥38) is ticked:
 *     → Deletes corresponding JSON from DB TARGET LIST sheet.
 *     → Consolidates rows.
 *     → Unticks the checkbox.
 *
 *   - If Observation checkbox (D3) is ticked:
 *     → Collects config values from predefined ranges.
 *     → Reads existing target JSON from DB TARGET LIST.
 *     → Constructs an observation JSON object with a "Targets" array.
 *     → Logs the resulting JSON string.
 *
 *   - If Add Target checkbox (B34) is ticked:
 *     → Collects target configuration ranges.
 *     → Generates target JSON and appends to DB TARGET LIST.
 *     → Clears input fields and resets the checkbox.
 *
 * @param {Object} e - Apps Script edit event.
 * @param {Range} e.range - Edited range object.
 * @param {string} e.value - New value entered in the edited cell.
 */
function onEdit(e) {
  const sheet = e.range.getSheet();
  const sheetName = sheet.getName();

  const userEmail = Session.getActiveUser().getEmail();  // Works only on INSTALLABLE trigger

  Logger.log(`onEdit triggered on sheet: ${sheetName}, cell: ${e.range.getA1Notation()} by user: ${userEmail}`);

  const col = e.range.getColumn();
  const row = e.range.getRow();

  const ss = SpreadsheetApp.getActive();
  const apiSheet = ss.getSheetByName("TM_UI_API");
  const obsSheet = ss.getSheetByName("DB OBS LIST")

  // ---------- DIG001 sheet: Digitiser config ----------
  if (sheetName === "DIG001" && col === 5 && row >= 4 && row <= 10) {
    const jsonStr = generateJSON(sheet, ["D4:E10"]);

    apiSheet.getRange("B3").setValue(jsonStr);
    Logger.log("Digitiser JSON updated in TM_UI_API B3");
    return;
  }

  // ---------- OBS DESIGN sheet ----------
  if (sheetName !== "OBS DESIGN") return;

  const targetSheet = ss.getSheetByName("DB TARGET LIST");
  if (!targetSheet) {
    Logger.log("DB TARGET LIST sheet not found. Exiting.");
    return;
  }

  // --- Update Altitude Plot if target changed ---
  if (col === 2 && (row === 25 || row === 30) || (col === 5 && row ===32)) {

    const startSB = sheet.getRange("E32").getValues()
    Logger.log("Scheduling Block Start:"+startSB)

    generateAltitudeData(startSB);
    updateChartTitle();
    return;
  }

  // --- Delete Target checkbox logic (column F, rows ≥38) ---
  if (col === 6 && row >= 38 && e.value === "TRUE") {
    const targetRow = row - 36; // Map 38->2, 39->3, etc.
    Logger.log(`Deleting JSON in DB TARGET LIST row: ${targetRow}`);
    targetSheet.getRange("A" + targetRow).clearContent();

    if (typeof consolidateRows === "function") {
      consolidateRows();
    }

    // Reset the checkbox
    sheet.getRange(row, col).setValue(false);
    Logger.log(`Delete checkbox reset for row ${row}`);
    return;
  }

  // --- Observation submission (D3) ---
  if (col === 4 && row === 3 && e.value === "TRUE") {
    
    const lastTargetRow = targetSheet.getLastRow();
    Logger.log("Last Target Row"+lastTargetRow)
    const targets = [];

    if (lastTargetRow >= 2) { // skip header
      const targetValues = targetSheet.getRange(2, 1, lastTargetRow - 1, 1).getValues();

      const keysToScale = ["center_freq", "bandwidth", "sample_rate"];
      
      targetValues.forEach((targetRow, index) => { // index is 0-based
        const targetJsonText = targetRow[0];
        Logger.log("Target JSON Text:" + targetJsonText);
        if (!targetJsonText) return;

        try {
          const targetObj = JSON.parse(targetJsonText);
          // Add an id based on the row order (1-based)
          targetObj.target_id = index + 1;

          // Scale specific keys
          keysToScale.forEach(key => {
            if (targetObj.hasOwnProperty(key)) {
              targetObj[key] = Number(targetObj[key]) * 1e6;
            }
          });

          targets.push(targetObj);
        } catch (err) {
          Logger.log(`Skipping invalid target JSON: ${err}`);
        }
      });
    }

    const obsRanges = ["A2:B3","A6:B11", "D29:E33"];
    const obsJsonObj = generateJSON(sheet, obsRanges, true);

    let dish_id;
    // Extract first token of dish_id (before first space)
    if (obsJsonObj.hasOwnProperty("dish_id")) {
      // Ensure it's treated as string
      const text = String(obsJsonObj["dish_id"]);
      dish_id = text.split(" ")[0];
      obsJsonObj["dish_id"] = dish_id;
    } else { dish_id = "<dish not selected>"}

    let start_datetime;

    // Extract observation start datetime
    if (obsJsonObj.hasOwnProperty("scheduling_block_start")) {
      const raw = String(obsJsonObj["scheduling_block_start"]["value"]);
      const date = new Date(raw);  // parse into Date object

      // Format as yy-mm-ddThh:mmZ
      start_datetime = Utilities.formatDate(date, "UTC", "yyyy-MM-dd'T'HH:mm'Z'");
    } else {
      throw new Error("Observation submitted with no scheduling_block_start attribute");
    }

    // Generate a unique observation id
    const obs_id = start_datetime + "-" + dish_id;
    obsJsonObj["obs_id"] = obs_id

    // Set ObsState to IDLE initially
    obsJsonObj["obs_state"] = { 
            "_type": "enum.IntEnum",
            "instance": "ObsState",
            "value": "IDLE"
    };
    // Add targets and meta data to the observation
    obsJsonObj["targets"] = targets;
    obsJsonObj["user_email"] = userEmail
    obsJsonObj["created"] = {
      "_type": "datetime",
      "value": new Date().toISOString()
    }

    const obsJsonStr = JSON.stringify(obsJsonObj, null, 2);
    Logger.log("Observation JSON string:\n" + obsJsonStr);
    
    // Write JSON to DB OBS LIST
    const lastObsRow = obsSheet.getLastRow();
    const writeRow = lastObsRow < 2 ? 2 : lastObsRow + 1;
    obsSheet.getRange("A" + writeRow).setValue(obsJsonStr);
    Logger.log(`Observation JSON written to DB OBS LIST row: ${writeRow}`);

    // Update consumed Scheduling Blocks
    updateConsumedBlocks(dish_id)

    // Reset the checkbox
    sheet.getRange(row, col).setValue(false);
    Logger.log(`Observation Submit checkbox reset for row ${row}`);

    // Reset input fields
    sheet.getRange("B2:B3").clearContent();
    sheet.getRange("E32").clearContent();
    targetSheet.getRange("A2:A").clearContent();
    return;
  }

  // --- Add Target checkbox (B34) ---
  const addTargetCell = sheet.getRange("B34");
  if (e.range.getA1Notation() !== addTargetCell.getA1Notation() || e.value !== "TRUE") {
    Logger.log("Add Target checkbox not ticked or wrong cell. Exiting.");
    return;
  }

  Logger.log("Add Target checkbox ticked. Processing JSON...");
  addTargetCell.setValue(false); // reset immediately

  const targetRanges = ["A14:B18", "A25:B25", "A29:B32", "D25:E26"];
  const targetJsonStr = generateJSON(sheet, targetRanges);

  // Write JSON to DB TARGET LIST
  const lastTargetRow = targetSheet.getLastRow();
  const writeRow = lastTargetRow < 2 ? 2 : lastTargetRow + 1;
  targetSheet.getRange("A" + writeRow).setValue(targetJsonStr);
  Logger.log(`Target JSON written to DB TARGET LIST row: ${writeRow}`);

  // Clear input fields
  sheet.getRange("B25").clearContent();
  sheet.getRange("B29:B32").clearContent();
  Logger.log("Input fields cleared");
}

/**
 * Computes which scheduling blocks are consumed by a telescope observation
 * and appends the corresponding block start times to the 'Lookup' sheet.
 *
 * Behaviour:
 * 1. Reads the observation start (E32) and end (E33) datetimes from 'OBS DESIGN'.
 * 2. Reads the scheduling block size in minutes (E34) from 'OBS DESIGN'.
 * 3. Calculates how many full scheduling blocks the observation spans.
 *    - Ensures the last block is not over-counted due to partial overlap.
 * 4. Finds the first empty row in column AL (column 38) of 'Lookup' sheet.
 * 5. Appends the start datetime of each consumed block to the sheet, leaving previous entries intact.
 *
 * Throws an error if the start/end datetimes or block size are invalid.
 */
function updateConsumedBlocks(dish_id) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('OBS DESIGN');

  Logger.log("Update Consumed Blocks for Dish ID:"+dish_id)

  const lookup = ss.getSheetByName("Lookup");
  if (!lookup) throw new Error("Sheet 'Lookup' not found.");

  const start = sheet.getRange("E32").getValue();  // observation start datetime
  const end   = sheet.getRange("E33").getValue();  // observation end datetime
  const blockSizeMin = lookup.getRange("AF2").getValue(); // scheduling block size in minutes

  if (!(start instanceof Date) || !(end instanceof Date)) {
    throw new Error("E32 and E33 must contain valid datetime values.");
  }
  if (isNaN(blockSizeMin) || blockSizeMin <= 0) {
    throw new Error("Invalid scheduling block size.");
  }

  const blockMS = blockSizeMin * 60 * 1000;
  const durationMS = end.getTime() - start.getTime();
  if (durationMS <= 0) {
    throw new Error("End datetime must be after start datetime.");
  }

  // Correct block count, avoiding the “1-second overlap” problem
  const blocks = Math.ceil(durationMS / blockMS - 1e-9);

  // Build output rows
  const rows = [];
  for (let i = 0; i < blocks; i++) {
    rows.push([dish_id, new Date(start.getTime() + i * blockMS)]);
  }

  // Find first empty row in column AL (col 38)
  const col = 37; // AK
  const lastRow = lookup.getLastRow();
  let writeRow = 2; // default start row if sheet is empty

  if (lastRow >= 2) {
    // Scan upward from lastRow in case there is trailing whitespace/content
    const colValues = lookup.getRange(2, col, lastRow - 1, 1).getValues();
    let lastUsed = 1;
    for (let i = colValues.length - 1; i >= 0; i--) {
      if (colValues[i][0] !== "") {
        lastUsed = i + 2; // offset because colValues[0] = row 2
        break;
      }
    }
    writeRow = lastUsed + 1;
  }

  // Write new rows
  lookup.getRange(writeRow, col, rows.length, 2).setValues(rows);

  Logger.log(`Appended ${rows.length} blocks starting at row ${writeRow}`);
}


/**
 * Custom function to generate JSON from ranges in a given sheet.
 *
 * @param {string} sheetName - Name of the sheet.
 * @param {string[]} cellRanges - Array of range strings, e.g., ["A2:B3", "D5:E10"].
 * @param {boolean} returnObject - If true, returns the JSON object; else returns JSON string.
 * @return {string} JSON string representing the ranges.
 * @customfunction
 */
function generateJSONFromRanges(sheetName, cellRanges, returnObject = false) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  if (!sheet) return "Error: Sheet not found";

  // Call your existing generateJSON function
  const result = generateJSON(sheet, cellRanges, returnObject, false);

  // Return as string for display in sheet
  return returnObject ? result : JSON.stringify(result);
}

/**
 * Generates an ObsList JSON object from all observations stored
 * in the "DB OBS LIST" sheet, column A, and writes the result
 * into a target cell.
 *
 * Structure produced:
 * {
 *   "_type": "ObsList",
 *   "obs_list": [...],
 *   "last_update": "<ISO8601 UTC timestamp>"
 * }
 */
function generateObsList() {
  const ss = SpreadsheetApp.getActive();
  const sourceSheet = ss.getSheetByName("DB OBS LIST");
  const targetSheet = ss.getSheetByName("TM_UI_API");  
  const targetCell = targetSheet.getRange("D3");

  // Get all values in column A (JSON objects)
  const values = sourceSheet.getRange("A2:A").getValues(); // skip header
  
  // Parse all non-empty JSON objects
  const obsList = [];
  for (const [cell] of values) {
    if (cell && cell.toString().trim() !== "") {
      try {
        obsList.push(JSON.parse(cell));
      } catch (err) {
        Logger.log("Skipping invalid JSON row: " + cell);
      }
    }
  }

  // Construct final ObsList object
  const result = {
    _type: "ObsList",
    obs_list: obsList,
    last_update: new Date().toISOString()  // valid ISO datetime
  };

  // Write JSON back to the sheet
  targetCell.setValue(JSON.stringify(result, null, 2));
}

/**
 * Cleans up the "DB OBS LIST" sheet by removing observations
 * with ObsState COMPLETED, ABORTED, or FAULT. 
 * Remaining observations are sorted by scheduling_block_start.value and
 * written back efficiently in a single batch operation.
 */
function cleanupObservations() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("DB OBS LIST");
  if (!sheet) throw new Error("Sheet 'DB OBS LIST' not found.");

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return; // nothing to do, no data

  const values = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
  const filteredObservations = [];

  values.forEach(row => {
    const obsText = row[0];
    if (!obsText) return;

    try {
      const obsObj = JSON.parse(obsText);
      const state = obsObj.obs_state?.value;
      if (!["COMPLETED", "ABORTED", "FAULT"].includes(state)) {
        filteredObservations.push(obsObj);
      }
    } catch (err) {
      Logger.log("Skipping invalid JSON row: " + err);
    }
  });

  // Sort remaining observations by scheduling_block_start.value ascending
  filteredObservations.sort((a, b) => {
    const dateA = new Date(a.scheduling_block_start?.value);
    const dateB = new Date(b.scheduling_block_start?.value);
    return dateA - dateB;
  });

  // Prepare data for batch write
  const outputValues = filteredObservations.map(obs => [JSON.stringify(obs, null, 2)]);

  // Clear old data and write all remaining observations in one go
  sheet.getRange(2, 1, sheet.getLastRow() - 1, 1).clearContent();
  if (outputValues.length > 0) {
    sheet.getRange(2, 1, outputValues.length, 1).setValues(outputValues);
  }

  Logger.log("Cleanup complete. " + filteredObservations.length + " observations remain.");
}

/**
 * Build a JSON object from a set of sheet ranges.
 * Handles duplicate keys by suffixing _1, _2, etc.
 * Converts any Date object in the sheet to ISO 8601 string.
 * @param {Sheet} sheet - Google Sheet object
 * @param {string[]} cellRanges - Array of A:B range strings
 * @param {boolean} [returnObject=false] - If true, returns JS object instead of JSON string
 * @param {boolean} [log=false] - If true, logs the generated JSON
 * @return {string|Object} JSON string (default) or JS object
 */
function generateJSON(sheet, cellRanges, returnObject = false, log = false) {
  const jsonObj = {};
  const keyCount = {};

  cellRanges.forEach(rangeStr => {
    const values = sheet.getRange(rangeStr).getValues();
    values.forEach(row => {
      const rawKey = row[0];
      const key = rawKey.toString().toLowerCase().replace(/ /g, "_");

      let value = row[1];
      if (!key || value === "") return;

      // Convert Feed key/value pairs to enum objects
      if (key === "feed") {
        value = {
          "_type": "enum.IntEnum",
          "instance": "Feed",
          "value": value
        }
      }
      // Convert Date objects to ISO strings
      if (value instanceof Date && !isNaN(value.getTime())) {
        value = {
          "_type": "datetime",
          "value": value.toISOString()
        }
      }
      // Convert numeric strings to numbers
      else if (typeof value === "string" && value.trim() !== "" && !isNaN(value)) {
        value = Number(value);
      }

      // Handle duplicate keys
      let finalKey = key;
      keyCount[finalKey] = keyCount[finalKey] || 0;
      if (keyCount[finalKey] > 0) finalKey = finalKey + "_" + keyCount[finalKey];
      keyCount[finalKey]++;
      jsonObj[finalKey] = value;
    });
  });
  if (log) Logger.log("Generated JSON:\n" + JSON.stringify(jsonObj, null, 2));
    return returnObject ? jsonObj : JSON.stringify(jsonObj, null, 2);
}

/**
 * Compute Altitude of an object
 * @param {number} raDeg - Right Ascension in degrees
 * @param {number} decDeg - Declination in degrees
 * @param {number} latDeg - Observer latitude in degrees
 * @param {number} lstDeg - Local Sidereal Time in degrees
 * @return {number} Altitude in degrees
 */
function computeAltitude(raDeg, decDeg, latDeg, lstDeg) {
  // Compute hour angle in degrees
  let H = lstDeg - raDeg;
  // Wrap H to [-180, +180] for cosine
  if (H < -180) H += 360;
  if (H > 180) H -= 360;
  
  // Convert to radians
  const Hrad = H * Math.PI / 180;
  const decRad = decDeg * Math.PI / 180;
  const latRad = latDeg * Math.PI / 180;
  
  // Altitude formula
  const sinA = Math.sin(latRad) * Math.sin(decRad) + 
               Math.cos(latRad) * Math.cos(decRad) * Math.cos(Hrad);
  
  const altRad = Math.asin(sinA);
  return altRad * 180 / Math.PI;  // convert back to degrees
}

/**
* Compute Local Sidereal Time.
* @param {Date|string} dateInput - UTC Date object or ISO date string.
* @param {number} lonDeg - Longitude (east positive)
* @return {number} LST in degrees
*/
function computeLST(dateInput, lonDeg) {
  // Ensure dateInput is a JavaScript Date object
  let date;
  if (dateInput instanceof Date) {
    date = dateInput;
  } else {
    date = new Date(dateInput);  // parse string
  }
  
  if (isNaN(date.getTime())) {
    throw new Error("Invalid date input: " + dateInput);
  }
  
  // Julian Date
  const JD = (date.getTime() / 86400000.0) + 2440587.5;  // convert ms -> JD
  const D = JD - 2451545.0;  // days since J2000
  
  // GMST in degrees
  let GMST = 280.46061837 + 360.98564736629 * D;
  GMST = ((GMST % 360) + 360) % 360;  // wrap to 0–360
  
  // LST in degrees
  const LST = (GMST + lonDeg) % 360;
  return (LST + 360) % 360;  // ensure 0–360
}

function testLST() {
  const lat = 53.187052;   // observer latitude
  const lon = -2.256079;    // longitude (East positive)
  const date = new Date(); // current time UTC

  const lst = computeLST(date, lon);
  Logger.log('LST = ' + lst.toFixed(2) + '°');

  const ra = 180.0;
  const dec = 20.0;

  const alt = computeAltitude(ra, dec, lat, lst);
  Logger.log('Altitude = ' + alt.toFixed(2) + '°');
}

/**
* Execute an ADQL query against SIMBAD TAP and write the results to the SIMBAD sheet.
* @param {string} adql_query - ADQL query string.
*/
function query_simbad(adql_query) {

  if (!adql_query) return [["Error", "Empty input"]];

  const sheetName = 'DB SIMBAD';  // tab to write into 
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
  }
  // Clear existing contents (optional)
  sheet.clearContents();

  try {
    
    const url = 'https://simbad.u-strasbg.fr/simbad/sim-tap/sync'
              + '?request=doQuery'
              + '&lang=adql'
              + '&format=tsv'
              + '&query=' + adql_query;
              
    const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (response.getResponseCode() !== 200) {
      throw new Error('Bad response code: ' + response.getResponseCode()
                      + ' – ' + response.getContentText());
    }
    const text = response.getContentText();
    
    // Parse TSV result
    const rows = Utilities.parseCsv(text, '\t');
    if (rows.length === 0) {
      sheet.getRange(1,1).setValue('No data returned');
      return;
    }
    
    // Write to sheet
    sheet.getRange(1,1,rows.length, rows[0].length).setValues(rows);
    
    // (Optional) Set header row bold
    sheet.getRange(1,1,1,rows[0].length).setFontWeight('bold');
    
  } catch (err) {
    Logger.log('Error fetching SIMBAD data: ' + err);
    sheet.getRange(1,1).setValue('Error: ' + err);
  }
}

/**
 * Executes an ADQL query against the SIMBAD TAP (Table Access Protocol) service
 * and returns the results as a 2-dimensional array suitable for Google Sheets.
 *
 * This function sends a synchronous TAP request to the SIMBAD endpoint using
 * `UrlFetchApp.fetch()`, retrieves the response in TSV (tab-separated) format,
 * and parses it into rows and columns. Error conditions (HTTP failure, empty
 * response, or parsing issues) are returned as user-visible ["Error", "..."]
 * or ["Warning", "..."] arrays so that Sheets formulas can display them cleanly.
 *
 * @param {string} adql_query
 *        The ADQL query string to execute. Must be a valid ADQL SELECT query.
 *
 * @returns {string[][]}
 *          A 2-dimensional array containing the parsed results. On failure,
 *          returns an array of the form:
 *               [["Error", "<message>"]]
 *          or, if the query succeeds but SIMBAD returns no rows:
 *               [["Warning", "No data returned"]]
 *
 * @throws {none}
 *         All exceptions are caught and converted to [["Error", "..."]] so that
 *         Google Sheets callers never encounter Apps Script runtime failures.
 *
 * @example
 *   =get_simbad_query_results("SELECT TOP 10 main_id, ra, dec FROM basic")
 *
 * @see https://simbad.u-strasbg.fr/simbad/sim-tap
 *      SIMBAD TAP documentation.
 */
function get_simbad_query_results(adql_query) {

  if (!adql_query) return [["Error", "Empty input"]];

  results = [];

  try {
    
    const url = 'https://simbad.u-strasbg.fr/simbad/sim-tap/sync'
              + '?request=doQuery'
              + '&lang=adql'
              + '&format=tsv'
              + '&query=' + adql_query;
              
    const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (response.getResponseCode() !== 200) {
      throw new Error('Bad response code: ' + response.getResponseCode()
                      + ' – ' + response.getContentText());
    }

    const text = response.getContentText();
    
    // Parse CSV result
    const rows = Utilities.parseCsv(text, '\t');
    if (rows.length === 0) {
      sheet.getRange(1,1).setValue('No data returned');
      return [["Warning", "No data returned"]];
    }
    
    rows.forEach((row, index) => {
      Logger.log('Row ' + row)
      results.push(row)
    })
    
  } catch (err) {
    Logger.log('Error fetching SIMBAD data: ' + err);
    return [["Error", err]];
  }
  return results
}

/**
* Execute an ADQL search, returning a list of catalogs supported by Simbad
* Used to enable a dropdown selection of catalogs
*/
function get_simbad_catalogs() {

  results = [];

  const query = encodeURIComponent(
    'SELECT TOP 10000 ' +
    'cat_name, ' +
    'description, ' +
    '"size" ' +
    'FROM cat ' + 
    'WHERE "size" > 100 ' +
    'ORDER BY "size" DESC'
    );

  Logger.log('SIMBAD catalogs query: ' + query);

  results = get_simbad_query_results(query)

  return results
}

/**
* Execute an arbitrary ADQL search, returning row arrays.
* @param {string} adql_query
*/
function get_search(adql_query) {

  const query = encodeURIComponent(adql_query)

  Logger.log('SIMBAD catalogs query: ' + query);

  results = get_simbad_query_results(query)

  return results

}

/**
 * Test1 of get_search
 */

function test1() {
  results = get_search("SELECT TOP 50000 b.main_id, i1.id, i2.id, b.ra, b.dec FROM basic AS b JOIN ident AS i1 ON b.oid = i1.oidref JOIN ident AS i2 ON b.oid = i2.oidref  WHERE i1.id LIKE '%'  AND i2.id LIKE 'M %'  AND (b.otype = 'Gal' ) ")
}

/**
* Execute an ADQL search, returning otypes supported by simbad
* Example otypes: Star, Galaxy, Pulsar etc
*/
function get_simbad_otypes() {
  results = [];

  results = get_search("SELECT description, otype FROM otypedef ORDER BY otype")
  return results
}

/**
 * Test2 of get_search
 */
function test2() {
  results = get_search("SELECT TOP 50000 b.main_id, b.ra, b.dec, MIN(i1.id) FROM basic AS b JOIN ident AS i1 ON b.oid = i1.oidref JOIN ident AS i2 ON b.oid = i2.oidref  WHERE i1.id LIKE '%'  AND i2.id LIKE 'M %'  GROUP BY b.main_id, b.ra, b.dec ORDER BY dec ASC")
}

/**
* Turns a text field into a JSON key by switching to lower case 
* and replaceing spaces with underscores
*/
function getJSONKey(keyText) {
  const key = keyText.toString().toLowerCase().replace(/ /g, "_");
  return key;
}

/**
* Extracts a target from a JSON string
* @param json string / text
* 
* @return targets in order of precedence:
* 1) Solar System 
* 2) AltAz
* 3) SkyCoord
* 4) Target
*/
function getTargetFromJSON(jsonText) {

  if (!jsonText) return "";   // empty cell guard

  try {
    var obj = JSON.parse(jsonText);

    // Check Solar System first
    if (obj["solar_system"] && obj["solar_system"].toString().trim() !== "") {
      return obj["solar_system"];
    }

    if (obj["altaz"] && obj["altaz"].toString().trim() !== "") {
      return obj["altaz"];
    }

    if (obj["skycoord"] && obj["skycoord"].toString().trim() !== "") {
      return obj["skycoord"];
    }

    // Fallback to Target
    if (obj["target"]) {
      return obj["target"];
    }

    return "";  // neither exists
  } catch (e) {
    // JSON parse failed
    return "Invalid JSON" + e;
  }
}

/**
 * flattenJSON(jsonText)
 * 
 * Flattens a JSON object into a 2D array with keys in dot notation and their values.
 * Returns a two-column array suitable for Google Sheets:
 *   Column 1: Keys in dot notation (e.g., "app.processors[0].name")
 *   Column 2: Corresponding values
 * 
 * Example:
 *   =flattenJSON(B1)
 * 
 * For nested objects and arrays, uses dot notation with [index] for arrays.
 * 
 * @param {string} jsonText - The cell containing JSON text
 * @return {Array<Array<string>>} A 2D array [["key", "value"], ...]
 * @customfunction
 */
function flattenJSON(jsonText) {
  try {
    if (!jsonText) return [["Error", "Empty input"]];

    // Parse JSON safely
    const obj = JSON.parse(jsonText);
    
    // Collect all key-value pairs
    const results = [];
    
    /**
     * Recursively flatten an object or array
     * @param {*} value - Current value to process
     * @param {string} prefix - Current key path in dot notation
     */
    function flatten(value, prefix = '') {
      if (value === null || value === undefined) {
        // Leaf node: null or undefined
        results.push([prefix, String(value)]);
      } else if (Array.isArray(value)) {
        // Array: recurse into each element with [index] notation
        if (value.length === 0) {
          results.push([prefix, '[]']);
        } else {
          value.forEach((item, index) => {
            const newKey = prefix ? `${prefix}[${index}]` : `[${index}]`;
            flatten(item, newKey);
          });
        }
      } else if (typeof value === 'object') {
        // Object: recurse into each property with dot notation
        const keys = Object.keys(value);
        if (keys.length === 0) {
          results.push([prefix, '{}']);
        } else {
          keys.forEach(key => {
            const newKey = prefix ? `${prefix}.${key}` : key;
            flatten(value[key], newKey);
          });
        }
      } else {
        // Leaf node: primitive value (string, number, boolean)
        results.push([prefix, String(value)]);
      }
    }
    
    flatten(obj);
    
    // Add header row
    return [["Key", "Value"]].concat(results);

  } catch (err) {
    return [["Error", "Invalid JSON: " + err.message]];
  }
}

/**
 * parseJSON(jsonText, keyPath)
 * 
 * Reads a JSON string and optionally returns the value at a nested path.
 * Supports dotted paths and array indices, e.g.:
 *   "dish.health.state" or "components[0].status"
 * 
 * Example:
 *   =parseJSON(B1, "dish.health.state")
 *   =parseJSON(B1, "Interfaces[1]")
 *   =parseJSON(B1)
 * 
 * @param {string} jsonText - The cell containing JSON text
 * @param {string} [keyPath] - Optional path to nested field
 * @return {string} The extracted value or a friendly message
 */
function parseJSON(jsonText, keyPath) {
  try {
    if (!jsonText) return "";

    // Parse JSON safely
    const obj = JSON.parse(jsonText);
    if (!keyPath) {
      // Return list of top-level keys
      if (typeof obj !== 'object' || obj === null) return obj;
      return Object.keys(obj).join(', ');
    }

    // Split keyPath into parts: supports dot and bracket notation
    const parts = [];
    keyPath.split('.').forEach(part => {
      const matches = part.match(/([^[\]]+)|(\[\d+\])/g);
      if (matches) {
        matches.forEach(m => {
          if (m.startsWith('[') && m.endsWith(']'))
            parts.push(parseInt(m.slice(1, -1), 10));
          else
            parts.push(m);
        });
      }
    });

    // Traverse JSON structure
    let val = obj;
    for (const key of parts) {
      if (val === undefined || val === null) return "Not found";
      val = val[key];
    }

    // Return prettified JSON if value is an object/array
    if (typeof val === 'object') {
      return JSON.stringify(val, null, 2);
    } else {
      return val;
    }

  } catch (err) {
    return "Invalid JSON";
  }
}
