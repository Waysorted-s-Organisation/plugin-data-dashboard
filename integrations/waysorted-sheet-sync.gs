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

function requiredProperty(name) {
  var value = PROPS.getProperty(name);
  if (!value) {
    throw new Error(
      'Script property "' + name + '" is not set. Open Project Settings → Script Properties and add it.'
    );
  }
  return value;
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
    // Written column by column so column E is skipped rather than overwritten
    // with a blank. setValue is one call each, which is the cost of not
    // destroying a column somebody filled in by hand.
    for (var col in FIELD_BY_COLUMN) {
      var column = Number(col);
      if (column === SOURCE_COLUMN) continue;
      var value = row[FIELD_BY_COLUMN[col]];
      sheet.getRange(target, column).setValue(value === undefined || value === null ? '' : value);
    }
  }

  var stamp = 'Synced ' + new Date().toISOString() + ' — ' + updated + ' updated, ' + added + ' added, ' +
    payload.coverage.users + ' accounts. ' + payload.coverage.message;
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
    .addToUi();
}
