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
    if (!jsonText) return "Empty input";

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

function checkStaleData() {
  tm = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('TM');
  updateStaleApp(tm)

  sdp = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('SDP');
  updateStaleApp(sdp)

  dig = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('DIG');
  updateStaleApp(dig)
}

function updateStaleApp(sheet) {

  const value = sheet.getRange("B2").getValue();
  const now = new Date();

  // Normalize the value into a real JS Date
  let timestamp;

  if (Object.prototype.toString.call(value) === "[object Date]" && !isNaN(value.getTime())) {
    // Real or wrapped Date object
    timestamp = new Date(value); // rewrap to ensure native Date
  } else if (typeof value === "number") {
    // Excel/Sheets serial date number
    timestamp = new Date(Math.round((value - 25569) * 86400 * 1000));
  } else if (typeof value === "string" && value.trim() !== "") {
    // Fallback parse for string date (e.g. "25/10/2025 20:54:21")
    const parts = value.trim().split(/[\/ :]/);
    if (parts.length >= 6) {
      const [day, month, year, hour, minute, second] = parts.map(Number);
      timestamp = new Date(year, month - 1, day, hour, minute, second);
    }
  }

  // Bail if not valid
  if (!(timestamp instanceof Date) || isNaN(timestamp.getTime())) {
    Logger.log("B2 is not a valid Date. Raw value: " + value);
    return;
  }

  const diffMinutes = (now - timestamp) / 1000 / 60;

  if (diffMinutes > 2) {
    //sheet.getRange("B3:B9").setValue("UNKNOWN");
    Logger.log(`Data stale: ${diffMinutes.toFixed(1)} minutes old.`);
  } else {
    Logger.log(`Data fresh: ${diffMinutes.toFixed(1)} minutes old.`);
  }
}
