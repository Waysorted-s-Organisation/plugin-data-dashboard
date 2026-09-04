/**
 * Waysorted conversion tracker — daily sync.
 *
 * Paste this into the spreadsheet's own Apps Script project (Extensions → Apps
 * Script), set the three script properties named below, then run
 * `installDailyTrigger` once. It fetches the dashboard's export endpoint and
 * writes the machine-owned columns of the Users tab.
 *
 * Apps Script rather than a service account on purpose: the script already runs
 * as the sheet's owner, so there is no Google Cloud project to create, no key
 * file to download, and no long-lived credential sitting in a repository or a
 * deployment environment. The only secret involved is the dashboard's own Basic
 * Auth password, held in this script's properties.
 *
 * What it will never touch:
 *   - Column E (Source). That is the acquisition channel, which the dashboard
 *     does not know. The authentication source it does know is a different
 *     fact that happens to sound alike, and writing one into the other would
 *     quietly destroy real information.
 *   - The Activity Log tab. Outreach notes are written by people.
 *   - The Dashboard tab. It is formulas, and it recalculates itself once Users
 *     is populated.
 *
 * Rows are matched on email. A row already in the sheet is updated in place, so
 * anything a person typed beside it survives. A row that is not in the sheet is
 * appended. A row that is in the sheet but no longer in the dashboard is left
 * alone rather than deleted — a disappearing account is more likely a filter or
 * an outage than a person who ceased to exist, and deleting their outreach
 * history to find out is not a reasonable trade.
 */

var PROPS = PropertiesService.getScriptProperties();
var SHEET_NAME = 'Users';
var HEADER_ROWS = 1;
var EMAIL_COLUMN = 3; // C
var SOURCE_COLUMN = 5; // E — human owned, never written

/** Column number -> field name from the export payload. E is absent by design. */
var FIELD_BY_COLUMN = {
  1: 'userId',
  2: 'name',
  3: 'email',
  4: 'signupDate',
  6: 'likedFeature',
  7: 'activeStatus',
  8: 'lastActiveDate',
  9: 'plan'
};

/**
 * Columns carrying a reject-on-invalid dropdown, and what to fall back to when
 * the dashboard reports a value the list has never heard of.
 *
 * This is the difference between a sync that finishes and one that stops on the
 * first row. A rejecting dropdown makes setValue THROW, so a single unrecognised
 * tool name — the dashboard says "Icon Library", a sheet says "Icon Search" —
 * aborted the entire run with four cells written. Names drift; the sync has to
 * survive it.
 */
var DROPDOWN_FALLBACKS = {
  6: ['Unknown', 'Other'],
  7: ['Unknown'],
  // No fallback on Plan. Every other column can afford a catch-all, but writing
  // "Free" against someone who is paying — because their tier's name is spelled
  // differently here than in the billing records — is a lie that reads as a
  // fact. Blank, and reported, is the only honest answer.
  9: []
};

/**
 * Reads a required script property, and on failure says what it DID find.
 *
 * "Not set" is true but useless: the property is almost always there under a
 * name that is off by a character — a missing letter, a trailing dot, a space
 * pasted along with the text — or it was typed and never saved. Listing the
 * names actually stored turns a guessing game into a one-look fix.
 */
function requiredProperty(name) {
  var value = PROPS.getProperty(name);
  if (value && value.trim()) return value.trim();

  var found = PROPS.getKeys();
  var detail = found.length
    ? 'Properties currently saved: ' + found.map(function (key) { return '"' + key + '"'; }).join(', ') + '.'
    : 'No script properties are saved at all — check you pressed "Save script properties".';
  var nearMiss = found.filter(function (key) {
    return key !== name && key.replace(/[^A-Za-z]/g, '').toUpperCase().indexOf(name.replace(/_/g, '')) >= 0;
  });
  if (nearMiss.length) {
    detail += ' "' + nearMiss[0] + '" looks like a misspelling of "' + name + '" — rename it exactly, with no trailing dot or spaces.';
  } else if (value !== null) {
    detail += ' "' + name + '" exists but its value is empty.';
  }
  throw new Error('Script property "' + name + '" is not set. ' + detail);
}

/**
 * Prints the property names and whether each has a value, without printing the
 * values themselves — one of them is a password.
 */
function showScriptProperties() {
  var keys = PROPS.getKeys();
  if (!keys.length) {
    Logger.log('No script properties are saved. Project Settings → Script Properties → Save script properties.');
    return 'none';
  }
  var report = keys.map(function (key) {
    var value = PROPS.getProperty(key);
    return '"' + key + '" → ' + (value && value.trim() ? value.trim().length + ' characters' : 'EMPTY');
  }).join('\n');
  Logger.log(report);
  return report;
}

function fetchExport() {
  var base = requiredProperty('DASHBOARD_URL').replace(/\/+$/, '');
  var user = requiredProperty('DASHBOARD_USER');
  var pass = requiredProperty('DASHBOARD_PASS');
  var response = UrlFetchApp.fetch(base + '/api/exports/users-sheet?days=30', {
    muteHttpExceptions: true,
    headers: {
      Authorization: 'Basic ' + Utilities.base64Encode(user + ':' + pass),
      Accept: 'application/json'
    }
  });
  var code = response.getResponseCode();
  var body = response.getContentText();
  if (code === 401 || code === 403) {
    throw new Error('The dashboard rejected these credentials (' + code + '). Check DASHBOARD_USER and DASHBOARD_PASS.');
  }
  if (code === 503) {
    throw new Error('The dashboard is not serving data (503): ' + body);
  }
  if (code !== 200) {
    throw new Error('Export request failed with ' + code + ': ' + body.slice(0, 300));
  }
  var payload = JSON.parse(body);
  if (!payload || !payload.rows) throw new Error('Export payload had no rows.');
  return payload;
}

/**
 * Existing rows, by lower-cased email.
 *
 * Read in one call rather than per row: Apps Script charges a round trip for
 * every range access, and a per-row lookup over a few hundred users is the
 * difference between two seconds and a timeout.
 */
/**
 * The values a column's dropdown will accept, or null if it accepts anything.
 *
 * Read from the sheet rather than hard-coded, so the list stays whatever its
 * owner decided it should be. Checked across the first few data rows because
 * validation lives on cells, and the first one is not always formatted.
 */
function allowedValues(sheet, column) {
  var lastRow = Math.max(sheet.getLastRow(), HEADER_ROWS + 1);
  for (var row = HEADER_ROWS + 1; row <= Math.min(lastRow, HEADER_ROWS + 5); row++) {
    var rule = sheet.getRange(row, column).getDataValidation();
    if (!rule) continue;
    if (rule.getCriteriaType() !== SpreadsheetApp.DataValidationCriteria.VALUE_IN_LIST) continue;
    var values = rule.getCriteriaValues()[0];
    if (values && values.length) {
      return values.map(function (value) { return String(value); });
    }
  }
  return null;
}

/**
 * A value the dropdown will accept, or blank.
 *
 * Case-insensitive match first, then the column's fallback, then blank. Blank
 * rather than a guess: writing "Palettable" because the real answer was not in
 * the list would be inventing an answer, and this lands in a sheet someone acts
 * on. Whatever gets dropped is reported so the list can be extended.
 */
function coerceToAllowed(value, allowed, fallbacks, dropped) {
  if (allowed === null) return value;
  var text = String(value === undefined || value === null ? '' : value).trim();
  if (!text) return '';
  for (var i = 0; i < allowed.length; i++) {
    if (allowed[i].toLowerCase() === text.toLowerCase()) return allowed[i];
  }
  for (var f = 0; f < (fallbacks || []).length; f++) {
    for (var j = 0; j < allowed.length; j++) {
      if (allowed[j].toLowerCase() === fallbacks[f].toLowerCase()) {
        dropped[text] = (dropped[text] || 0) + 1;
        return allowed[j];
      }
    }
  }
  dropped[text] = (dropped[text] || 0) + 1;
  return '';
}

function indexExistingRows(sheet) {
  var lastRow = sheet.getLastRow();
  var index = {};
  if (lastRow <= HEADER_ROWS) return index;
  var emails = sheet.getRange(HEADER_ROWS + 1, EMAIL_COLUMN, lastRow - HEADER_ROWS, 1).getValues();
  for (var i = 0; i < emails.length; i++) {
    var email = String(emails[i][0] || '').trim().toLowerCase();
    if (email) index[email] = HEADER_ROWS + 1 + i;
  }
  return index;
}

function syncUsersSheet() {
  var payload = fetchExport();
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = spreadsheet.getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error('No sheet named "' + SHEET_NAME + '" in this spreadsheet.');

  var existing = indexExistingRows(sheet);
  var appendAt = Math.max(sheet.getLastRow(), HEADER_ROWS) + 1;
  var updated = 0;
  var added = 0;
  var dropped = {};

  // Read once for the whole run rather than per cell: every range access is a
  // round trip, and this turns a few hundred users from a timeout into seconds.
  var allowed = {};
  for (var key in DROPDOWN_FALLBACKS) allowed[key] = allowedValues(sheet, Number(key));

  for (var r = 0; r < payload.rows.length; r++) {
    var row = payload.rows[r];
    var email = String(row.email || '').trim().toLowerCase();
    if (!email) continue;
    var target = existing[email];
    if (!target) {
      target = appendAt;
      appendAt += 1;
      existing[email] = target;
      added += 1;
    } else {
      updated += 1;
    }

    var cells = {};
    for (var col in FIELD_BY_COLUMN) {
      var column = Number(col);
      if (column === SOURCE_COLUMN) continue;
      var value = row[FIELD_BY_COLUMN[col]];
      if (value === undefined || value === null) value = '';
      if (DROPDOWN_FALLBACKS[column]) {
        value = coerceToAllowed(value, allowed[column], DROPDOWN_FALLBACKS[column], dropped);
      }
      cells[column] = value;
    }

    // Two block writes rather than eight single ones, which also steps over
    // column E without having to blank it.
    sheet.getRange(target, 1, 1, 4).setValues([[cells[1], cells[2], cells[3], cells[4]]]);
    sheet.getRange(target, 6, 1, 4).setValues([[cells[6], cells[7], cells[8], cells[9]]]);
  }

  var stamp = 'Synced ' + new Date().toISOString() + ' — ' + updated + ' updated, ' + added + ' added, ' +
    payload.coverage.users + ' accounts. ' + payload.coverage.message;
  var droppedNames = Object.keys(dropped);
  if (droppedNames.length) {
    stamp += ' Values the sheet dropdowns do not accept: ' + droppedNames.map(function (name) {
      return '"' + name + '" (' + dropped[name] + ')';
    }).join(', ') + '. Add them to the dropdown to keep them.';
  }
  PROPS.setProperty('LAST_SYNC', stamp);
  Logger.log(stamp);
  return stamp;
}

/**
 * Replaces the placeholder rows the sheet shipped with.
 *
 * Run once, by hand, before the first sync. It removes only rows whose email
 * ends in @example.com so a real row typed in early is never caught by it.
 */
function removePlaceholderRows() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error('No sheet named "' + SHEET_NAME + '".');
  var lastRow = sheet.getLastRow();
  if (lastRow <= HEADER_ROWS) return 'Nothing to remove.';
  var emails = sheet.getRange(HEADER_ROWS + 1, EMAIL_COLUMN, lastRow - HEADER_ROWS, 1).getValues();
  var removed = 0;
  // Bottom up, so deleting a row does not shift the ones not yet examined.
  for (var i = emails.length - 1; i >= 0; i--) {
    if (/@example\.com\s*$/i.test(String(emails[i][0] || ''))) {
      sheet.deleteRow(HEADER_ROWS + 1 + i);
      removed += 1;
    }
  }
  return 'Removed ' + removed + ' placeholder rows.';
}

/** Daily at 06:00 in the spreadsheet's timezone. Safe to run more than once. */
function installDailyTrigger() {
  var existing = ScriptApp.getProjectTriggers();
  for (var i = 0; i < existing.length; i++) {
    if (existing[i].getHandlerFunction() === 'syncUsersSheet') ScriptApp.deleteTrigger(existing[i]);
  }
  ScriptApp.newTrigger('syncUsersSheet').timeBased().atHour(6).everyDays(1).create();
  return 'Daily sync installed for 06:00 ' + Session.getScriptTimeZone() + '.';
}

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Waysorted')
    .addItem('Sync users now', 'syncUsersSheet')
    .addItem('Remove placeholder rows', 'removePlaceholderRows')
    .addItem('Install daily sync', 'installDailyTrigger')
    .addItem('Check settings', 'showScriptProperties')
    .addToUi();
}
